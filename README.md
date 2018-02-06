# grelka
Graphite relay to kafka. Simple replace of [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)

This relay was made to change the model of passing metrics from: 
    carbon-clickhouse -(push)> ClickHouse -> graphite-clickhouse -> Grafana 
to
    grelka -(push)> Kafka -(pull)> ClickHouse -> graphite-clickhouse -> Grafana

Listen on ip:port and waiting incoming messages (line graphite protocol: "metric value timestamp"). Then encapsulate to [JSONEachRow](https://clickhouse.yandex/docs/en/formats/jsoneachrow.html) format (line by line) all incoming strings and send (asynchronous) this JSONs to kafka brokers topics (data and tree seperated). 

## Kafka configuration
Create topics:
1. graphite-json-1
2. graphite-json-tree-1

## ClickHouse configuration
1. Configurate Clickhouse according for this [instruction](https://github.com/lomik/carbon-clickhouse#clickhouse-configuration).
2. Create tables in Clickhouse:
```sql
CREATE TABLE kafka_json (
    Path String,
    Value Float64,
    Time UInt32,
    Timestamp UInt32
) ENGINE = Kafka('localhost:9092', 'graphite-json-1', 'clickhouse', 'JSONEachRow');

CREATE MATERIALIZED VIEW kafka_json_to_graphite 
TO graphite AS 
    SELECT 
        Path, 
        Value,
        Time,
        toDate(Time,'Europe/Moscow') AS Date,
        Timestamp
FROM kafka_json;

CREATE TABLE kafka_json_tree (
    Path String,
    Level UInt32,
    Version UInt32
) ENGINE = Kafka('localhost:9092', 'graphite-json-tree-1', 'clickhouse', 'JSONEachRow');

CREATE MATERIALIZED VIEW kafka_json_tree_to_graphite_tree 
TO graphite_tree AS 
    SELECT 
        toDate('2011-11-11') AS Date,
        Level,
        Path,
        0 AS Deleted,
        Version 
FROM kafka_json_tree;
```
## Use
```sh
./grelka -ip 0.0.0.0 -port 2003 -brokers localhost:9092 -topic-data graphite-json-1 -topic-tree graphite-json-tree-1
```
If you need more relay functionality, please use [gorelka](https://github.com/go-graphite/gorelka).
