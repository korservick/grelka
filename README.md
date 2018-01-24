# grelka
graphite relay to kafka

Simple listen on ip:port and waiting incoming messages (line graphite protocol: "metric value timestamp"). Then encapsulate to JSON format (line by line) all incoming strings and send (asynchronous) this JSONs to kafka brokers topic.

If you need more functionality, please use [g2mt](https://github.com/go-graphite/g2mt).
