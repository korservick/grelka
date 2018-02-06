package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

type stringList []string

type metricJSON struct {
	Metric    string  `json:"Metric"`
	Value     float64 `json:"Value"`
	Timestamp uint32  `json:"Timestamp"`
	Version   uint32  `json:"Version"`
}

func (s *stringList) Set(value string) error {
	*s = strings.Split(value, ",")
	return nil
}

var (
	ip            = flag.String("ip", "0.0.0.0", "ip listening on")
	port          = flag.String("port", "2003", "port listening on")
	brokersString = flag.String("brokers", "localhost:9092", "Brokers server1:9092, server2:9092... separeted by comma")
	frequency     = flag.Int64("frequency", 500, "flush frequency to kafka in milliseconds")
	prefix        = flag.String("prefix", "", "Metric prefix")
	topicData     = flag.String("topic-data", "graphite-json-1", "Kafka write topic data")
	topicTree     = flag.String("topic-tree", "graphite-json-tree-1", "Kafka write topic tree")

	brokers            stringList
	producer           sarama.AsyncProducer
	ms                 metricJSON
	metrics            chan string
	trees              chan string
	muTree             sync.RWMutex
	pathMap            map[string]int
	lenghtTree         uint32
	previousLenghtTree uint32
	lastSendTreeDate   int
	// version  int64
)

func relayMetrics(conn net.Conn) {
	var sp string
	var metricArray []string
	var pathSend string
	var i int
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, 4096)
	for {
		buf, _, err := reader.ReadLine()
		if err != nil {
			if io.EOF != err {
				log.Fatal("Error read line:", err.Error())
			}
			break
		}
		version := time.Now().Unix()
		s := strings.Split(string(buf), " ")
		if len(s) == 3 {
			metric := s[0]
			value, err := strconv.ParseFloat(s[1], 64)
			if err == nil {
				timestamp, err := strconv.ParseUint(s[2], 10, 64)
				if err == nil {
					sp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Value\":%f,\"Time\":%d,\"Timestamp\":%d}\n", *prefix, metric, value, timestamp, version)
					err = json.Unmarshal([]byte(sp), &ms)
					if err != nil {
						log.Print(err, " in: ", sp)
					} else {
						metrics <- sp

						// fmt.Printf("{\"Metric\":\"%s%s\",\"Value\":%f,\"Timestamp\":%d}\n", *prefix, metric, value, timestamp)
						metricArray = strings.Split(metric, ".")
						pathSend = ""
						muTree.Lock()
						for i = 0; i < len(metricArray)-1; i++ {
							pathSend = pathSend + metricArray[i] + "."
							pathMap[pathSend] = i + 1
							// atomic.AddUint32(&lenghtTree, 1)
							// stp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Version\":%d}\n", *prefix, pathSend, i+1, version)
							// sendStringTree = sendStringTree + stp
						}
						pathMap[metric] = i + 1
						atomic.StoreUint32(&lenghtTree, uint32(len(pathMap)))
						muTree.Unlock()
						// atomic.AddUint32(&lenghtTree, 1)

						// for path, lenght := range pathMap {
						// 	stp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Version\":%d}\n", *prefix, path, lenght, version)
						// 	trees <- stp
						// }
					}
				}
			}
		}
	}
}

func main() {
	flag.Parse()
	brokers.Set(*brokersString)
	if flag.Parsed() {
		log.Printf("brokers=%s topic-data=%s topic-tree=%s\n", brokers, *topicData, *topicTree)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}
	lAddr, err := net.ResolveTCPAddr("tcp4", *ip+":"+*port)
	if err != nil {
		log.Fatal("Error resolve addr:", err.Error())
	}
	listener, err := net.ListenTCP("tcp4", lAddr)
	if err != nil {
		log.Fatal("Error listening:", err.Error())
	}
	defer listener.Close()
	log.Printf("Listening on: %s:%s\n", *ip, *port)
	log.Println("Started...")

	configProducer := sarama.NewConfig()
	configProducer.Version = sarama.V1_0_0_0
	configProducer.Producer.RequiredAcks = sarama.WaitForLocal
	configProducer.Producer.Compression = sarama.CompressionLZ4
	configProducer.Producer.Flush.Frequency = time.Duration(*frequency) * time.Millisecond

	producer, err = sarama.NewAsyncProducer(brokers, configProducer)
	if err != nil {
		log.Fatalln("Error:", err)
	} else {
		log.Printf("New kafka producer created\n")
	}
	defer producer.AsyncClose()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	metrics = make(chan string, 10000)
	trees = make(chan string, 10000)

	newConns := make(chan net.Conn)
	go func(l net.Listener) {
		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				log.Fatal("Error accepting:", err.Error())
				newConns <- nil
				return
			}
			newConns <- conn
		}
	}(listener)

	tickerData := time.NewTicker(time.Second)
	tickerTree := time.NewTicker(time.Second * 60)
	bufferData := make([]byte, 0)
	bufferTree := make([]byte, 0)
	var lenghtData uint32
	var counter uint32
	var muData sync.RWMutex
	var stp string
	pathMap = make(map[string]int)

	hostname, _ := os.Hostname()
	internalMetric := "carbon.ktok." + hostname + "." + *topicData + ".metrics.send"
	metricArray := strings.Split(internalMetric, ".")
	pathSend := ""
	var i int
	for i = 0; i < len(metricArray)-1; i++ {
		pathSend = pathSend + metricArray[i] + "."
		pathMap[pathSend] = i + 1
	}
	pathMap[internalMetric] = i + 1
	lenghtTree = uint32(len(pathMap))

	for {
		select {
		case conn := <-newConns:
			go relayMetrics(conn)
		case metric := <-metrics:
			{
				// fmt.Println("metric: ", metric)
				muData.Lock()
				bufferData = append(bufferData, []byte(metric)...)
				muData.Unlock()
				atomic.AddUint32(&lenghtData, 1)
				atomic.AddUint32(&counter, 1)
			}
		// case tree := <-trees:
		// 	{
		// 		// fmt.Println("tree: ", tree)
		// 		muTree.Lock()
		// 		bufferTree = append(bufferTree, []byte(tree)...)
		// 		muTree.Unlock()
		// 	}
		case <-tickerData.C:
			{
				version := time.Now().Unix()
				metrics <- fmt.Sprintf("{\"Path\":\"%s%s\",\"Value\":%f,\"Time\":%d,\"Timestamp\":%d}\n", *prefix, internalMetric, float64(counter), version, version)

				if lenghtData > 0 {
					// log.Println("Send metrics: ", counter)
					muData.Lock()
					// fmt.Printf("buffer count:%d\nbuffer:%s", lenghtData, string(bufferData))
					producer.Input() <- &sarama.ProducerMessage{
						Topic: *topicData,
						Value: sarama.ByteEncoder(bufferData),
					}

					bufferData = nil
					muData.Unlock()

					atomic.StoreUint32(&lenghtData, 0)
					atomic.StoreUint32(&counter, 0)
				}
			}

		case <-tickerTree.C:
			{
				if lenghtTree != previousLenghtTree || time.Now().Day() != lastSendTreeDate {
					muTree.RLock()
					tree := pathMap
					// pathMap = make(map[string]int)
					muTree.RUnlock()
					bufferTree = nil

					version := time.Now().Unix()
					for path, lenght := range tree {
						stp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Version\":%d}\n", *prefix, path, lenght, version)
						// trees <- stp
						bufferTree = append(bufferTree, []byte(stp)...)
					}

					atomic.StoreUint32(&previousLenghtTree, lenghtTree)
					lastSendTreeDate = time.Now().Day()

					// fmt.Printf("buffer tree lenght:%d, date:%d, tree:\n%s", lenghtTree, lastSendTreeDate, string(bufferTree))
					producer.Input() <- &sarama.ProducerMessage{
						Topic: *topicTree,
						Value: sarama.ByteEncoder(bufferTree),
					}
					atomic.StoreUint32(&lenghtTree, 0)
				}
			}
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
		case <-signals:
			return
		}
	}
}
