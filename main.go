package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	jump "github.com/dgryski/go-jump"
	"github.com/pierrec/xxHash/xxHash64"
)

type stringList []string

type metricJSON struct {
	Metric    string  `json:"Metric"`
	Value     float64 `json:"Value"`
	Timestamp uint32  `json:"Timestamp"`
	Version   uint32  `json:"Version"`
}

type buffer struct {
	Shard int
	Data  []byte
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
	topicData     = flag.String("topic-data", "graphite-json", "Kafka write topic data")
	topicTree     = flag.String("topic-tree", "graphite-json-tree", "Kafka write topic tree")
	topicNums     = flag.Int("topic-nums", 3, "Numbers of data and tree topics: topic-data-00...topic-data-02 and topic-json-tree-00...topic-json-tree-02")
	// dataFlashMinBuffer = flag.Uint64("data-flash-min-buffer", 0, "")
	dataFlashBuffer = flag.Uint64("data-flash-buffer", 30000, "")
	treeFlashBuffer = flag.Uint64("tree-flash-buffer", 30000, "")

	brokers            stringList
	producer           sarama.AsyncProducer
	ms                 metricJSON
	metrics            chan buffer
	trees              chan buffer
	muTree             sync.RWMutex
	pathMap            map[string]int
	lenghtTree         uint32
	previousLenghtTree uint32
	lastSendTreeDate   int
	treeMap            map[string]int
	seriesMap          map[string]int
	mu                 sync.RWMutex
	// tickerTreeDuration = time.Second * 60
	i              int
	metricCount    [3]uint64
	treeCount      [3]uint64
	bufferData     [][]byte
	bufferTree     [][]byte
	metricInternal [2]string
	shardInternal  [2]int

// dataFlashMinBuffer = uint32(1000)
// dataFlashBuffer    = uint32(30000)
// treeFlashBuffer    = uint32(30000)

// version  int64
)

func relayMetrics(conn net.Conn) {
	var (
		version     int64
		s           [][]byte
		sp          string
		stp         string
		metricName  string
		metricArray []string
		pathSend    string
		timestamp   int64
		value       float64
		i           int
		shard       int
		// shardInternal   int
		foundTree   bool
		foundSeries bool
		nowDay      int
		hash        uint64
		// remoteNameArray []string
		// remoteName      string
		// err             error
		tree   buffer
		metric buffer
	)
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
		version = time.Now().Unix()
		nowDay = time.Now().Day()
		if nowDay != lastSendTreeDate {
			mu.Lock()
			lastSendTreeDate = nowDay
			seriesMap = make(map[string]int)
			mu.Unlock()
			for k := 0; i < len(metricInternal); i++ {
				stp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Time\":%d,\"Version\":%d},\n", *prefix, metricInternal[k], strings.Count(metricInternal[k], ".")+1, version, version)
				tree.Shard = shardInternal[k]
				tree.Data = []byte(stp)
				trees <- tree
			}
		}

		s = bytes.Split(buf, []byte(" "))
		if len(s) == 3 && len(s[0]) > 0 {
			metricName = string(s[0])
			value, err = strconv.ParseFloat(string(s[1]), 64)
			if err == nil && (value == value) && (value > -math.MaxFloat64) && (value < math.MaxFloat64) && len(s[1]) > 0 && len(s[2]) > 0 {
				timestamp, err = strconv.ParseInt(string(s[2]), 10, 64)
				if err == nil {
					// fmt.Printf("%v\n", metricName)

					mu.RLock()
					shard, foundTree = treeMap[metricName]
					mu.RUnlock()

					if !foundTree {
						hash = xxHash64.Checksum(s[0], 0xC0FE)
						shard = int(jump.Hash(hash, *topicNums))
						mu.Lock()
						treeMap[metricName] = shard
						mu.Unlock()

						metricArray = strings.Split(metricName, ".")
						pathSend = ""
						for i = 0; i < len(metricArray)-1; i++ {
							pathSend = pathSend + metricArray[i] + "."
							stp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Time\":%d,\"Version\":%d},\n", *prefix, pathSend, i+1, timestamp, version)
							tree.Shard = shard
							tree.Data = []byte(stp)
							trees <- tree
						}
						// metricArray = nil
						// stp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Time\":%d,\"Version\":%d},\n", *prefix, metricName, i+1, timestamp, version)
						// tree.Shard = shard
						// tree.Data = []byte(stp)
						// trees <- tree
					}

					sp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Value\":%f,\"Time\":%d,\"Timestamp\":%d},\n", *prefix, metricName, value, timestamp, version)
					metric.Shard = shard
					metric.Data = []byte(sp)
					metrics <- metric

					mu.RLock()
					shard, foundSeries = seriesMap[metricName]
					mu.RUnlock()

					if !foundSeries || version-timestamp > 120 {
						hash = xxHash64.Checksum(s[0], 0xC0FE)
						shard = int(jump.Hash(hash, *topicNums))
						mu.Lock()
						seriesMap[metricName] = shard
						mu.Unlock()

						stp = fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Time\":%d,\"Version\":%d},\n", *prefix, metricName, strings.Count(metricName, ".")+1, timestamp, version)
						tree.Shard = shard
						tree.Data = []byte(stp)
						trees <- tree
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
		var topicDataList, topicTreeList string
		for i = 0; i < *topicNums; i++ {
			topicDataList = topicDataList + *topicData + fmt.Sprintf("-%02d", i) + " "
			topicTreeList = topicTreeList + *topicTree + fmt.Sprintf("-%02d", i) + " "
		}
		log.Printf("brokers=%s topic-data=%s topic-tree=%s\n", brokers, topicDataList, topicTreeList)
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

	metrics = make(chan buffer, 10000)
	trees = make(chan buffer, 10000)

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

	treeMap = make(map[string]int)
	seriesMap = make(map[string]int)
	tickerData := time.NewTicker(time.Second)
	// tickerTree := time.NewTicker(tickerTreeDuration)
	bufferData = make([][]byte, 3)
	for i := range bufferData {
		bufferData[i] = make([]byte, 0, 10000000)
	}
	bufferTree = make([][]byte, 3)
	for i := range bufferTree {
		bufferTree[i] = make([]byte, 0, 10000000)
	}

	hostname, _ := os.Hostname()
	version := time.Now().Unix()
	subNameArray := [2]string{".metric", ".tree"}
	for k, subName := range subNameArray {
		metricInternal[k] = "carbon.grelka." + hostname + subName + ".send"
		hash := xxHash64.Checksum([]byte(metricInternal[k]), 0xC0FE)
		shardInternal[k] = int(jump.Hash(hash, *topicNums))
		metricArray := strings.Split(metricInternal[k], ".")
		pathSend := ""
		for i = 0; i < len(metricArray)-1; i++ {
			pathSend = pathSend + metricArray[i] + "."
			stp := fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Time\":%d,\"Version\":%d},\n", *prefix, pathSend, i+1, version, version)
			bufferTree[shardInternal[k]] = append(bufferTree[shardInternal[k]], []byte(stp)...)
		}
		// metricArray = nil
		stp := fmt.Sprintf("{\"Path\":\"%s%s\",\"Level\":%d,\"Time\":%d,\"Version\":%d},\n", *prefix, metricInternal[k], i+1, version, version)
		bufferTree[shardInternal[k]] = append(bufferTree[shardInternal[k]], []byte(stp)...)
	}

	for {
		select {
		case conn := <-newConns:
			go relayMetrics(conn)
		case metric := <-metrics:
			{
				// fmt.Println("metric: ", metric)
				bufferData[metric.Shard] = append(bufferData[metric.Shard], metric.Data...)
				metricCount[metric.Shard]++
				if metricCount[metric.Shard] > *dataFlashBuffer {
					// fmt.Printf("shard: %d, (flash > %d metrics) buffer data size:%d, metrics count:%d\n", shard, *dataFlashBuffer, len(bufferData[shard]), metricCount[shard])
					producer.Input() <- &sarama.ProducerMessage{
						Topic: *topicData + fmt.Sprintf("-%02d", metric.Shard),
						Value: sarama.ByteEncoder(bufferData[metric.Shard]),
					}

					metricCount[metric.Shard] = 0
					bufferData[metric.Shard] = nil
					// bufferData = bufferData[:0]

				}
			}
		case tree := <-trees:
			{
				// fmt.Println("tree: ", tree)
				bufferTree[tree.Shard] = append(bufferTree[tree.Shard], tree.Data...)
				treeCount[tree.Shard]++
				if treeCount[tree.Shard] > *treeFlashBuffer {
					// fmt.Printf("shard: %d, (flash > %d trees) buffer tree size:%d, tree count:%d\n", shard, *treeFlashBuffer, len(bufferTree[shard]), treeCount[shard])
					producer.Input() <- &sarama.ProducerMessage{
						Topic: *topicTree + fmt.Sprintf("-%02d", tree.Shard),
						Value: sarama.ByteEncoder(bufferTree[tree.Shard]),
					}

					treeCount[tree.Shard] = 0
					// bufferTree = bufferTree[:0]
					bufferTree[tree.Shard] = nil
				}
			}
		case <-tickerData.C:
			{
				for i = range metricCount {
					// fmt.Printf("i: %d, count: %d\n", i, metricCount[i])
					if metricCount[i] > 0 {
						version := time.Now().Unix()
						sp := fmt.Sprintf("{\"Path\":\"%s%s\",\"Value\":%f,\"Time\":%d,\"Timestamp\":%d},\n", *prefix, metricInternal[0], float64(metricCount[i]), version, version) +
							fmt.Sprintf("{\"Path\":\"%s%s\",\"Value\":%f,\"Time\":%d,\"Timestamp\":%d},\n", *prefix, metricInternal[1], float64(treeCount[i]), version, version)
						bufferData[i] = append(bufferData[i], []byte(sp)...)
						// fmt.Printf("shard: %d buffer data size:%d metrics count:%d\n", i, len(bufferData[i]), metricCount[i])
						producer.Input() <- &sarama.ProducerMessage{
							Topic: *topicData + fmt.Sprintf("-%02d", i),
							Value: sarama.ByteEncoder(bufferData[i]),
						}

						bufferData[i] = nil
						metricCount[i] = 0
					}
				}
				for i = range treeCount {
					if treeCount[i] > 0 {
						// fmt.Printf("shard: %d, buffer tree size: %d, tree count: %d\n", i, len(bufferTree[i]), treeCount[i])
						// fmt.Printf("topic: %s, buffer tree:\n%s\n", *topicTree+fmt.Sprintf("-%02d", i), string(bufferTree[i]))
						producer.Input() <- &sarama.ProducerMessage{
							Topic: *topicTree + fmt.Sprintf("-%02d", i),
							Value: sarama.ByteEncoder(bufferTree[i]),
						}

						bufferTree[i] = nil
						treeCount[i] = 0
					}
				}
			}
		// case <-tickerTree.C:
		// 	{
		// 	}
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
		}
	}
}
