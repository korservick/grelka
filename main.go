package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type stringList []string

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
	topic         = flag.String("topic", "graphite-json", "Kafka write topic")

	brokers  stringList
	producer sarama.AsyncProducer
)

func relayMetrics(conn net.Conn) {
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
		s := strings.Split(string(buf), " ")
		if len(s) == 3 {
			metric := s[0]
			value, err := strconv.ParseFloat(s[1], 64)
			if err == nil {
				timestamp, err := strconv.ParseUint(s[2], 10, 64)
				if err == nil {
					// fmt.Printf("{\"metric\":\"%s%s\",\"value\":%f,\"timestamp\":%d}\n", *prefix, metric, value, timestamp)
					producer.Input() <- &sarama.ProducerMessage{
						Topic: *topic,
						Value: sarama.StringEncoder(
							fmt.Sprintf("{\"metric\":\"%s%s\",\"value\":%f,\"timestamp\":%d}\n", *prefix, metric, value, timestamp)),
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
		log.Printf("brokers=%s\n write topic=%s\n", brokers, *topic)
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
	configProducer.Version = sarama.V0_10_2_0
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

	for {
		select {
		case conn := <-newConns:
			go relayMetrics(conn)
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
		case <-signals:
			return
		}
	}
}
