Kafka Camel Elastic Spring Integration 
==============

This contains a Spring Boot Kafka Producer and Camel Kafka Consumer.

The Consumer uses the `org.elasticsearch.client.RestHighLevelClient` to put JSON message bodies into ElasticSearch.

### Start Kafka, Zookeeper, and ElasticSearch
Create directory `/mnt/kafka-data` if it does not exist

`docker-compose up`

### Produce
`./gradlew :kafka:run`

The test produces 10 random activity stream documents

### Consume
`./gradlew :kafka:runConsumer`

### View Test Data in ElasticSearch
Go to Kibana at http://localhost:5601 

## Kafka Connect
This is an alternate demo implementation of the above.  
It uses the [Avro Random Generator](https://github.com/confluentinc/avro-random-generator) to stream data through Kafka to Elasticsearch. 

```bash
$ cd kafka/src/test/resources
```

`docker-compose -f kafka-connect-test.yml up`

### Add Elasticsearch Connector
```bash
 curl -X POST http://localhost:8083/connectors \
 -H 'Content-Type:application/json' \
 -H 'Accept:application/json' \
 -d @elasticsearch-connector-config.json
 ```
 
### Add Datagen Connector
```bash
 curl -X POST http://localhost:8083/connectors \
 -H 'Content-Type:application/json' \
 -H 'Accept:application/json' \
 -d @connect.source.datagen.json
 ```

### View Topics UI
http://localhost:8000

### View Connect UI
http://localhost:8001
