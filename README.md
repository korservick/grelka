# grelka
graphite relay to kafka

Simple listen on ip:port and waiting incoming messages (line graphite protocol: "metric value timestamp"). Then encapsulate to JSON format (line by line) all incoming strings and send (asynchronous) this JSONs to kafka brokers topic.