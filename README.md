Kafka Camel Elastic Spring Integration 
==============

This contains a Spring Boot Kafka Producer and a Camel Kafka Consumer.

The Consumer uses the `org.elasticsearch.client.RestHighLevelClient` to put JSON message bodies into ElasticSearch.

### Start Kafka, Zookeeper, and ElasticSearch
Create directory `/mnt/kafka-data` if it does not exist
`docker-compose up`

### Produce Test
`./gradlew :kafka:run`

The test produces 10 activity stream documents

### Consume
`./gradlew :kafka:runConsumer`

### View Test Data in ElasticSearch
Go to Kibana at http://localhost:5601 
