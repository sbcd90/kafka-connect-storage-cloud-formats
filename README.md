kafka-connect-storage-cloud-formats
===================================

- This project provides an [Apache ORC](https://orc.apache.org/) writer to S3
for [kafka-connect-s3](https://docs.confluent.io/current/connect/kafka-connect-s3/index.html) connector.

## Installation

- Generate jar using maven
```
mvn clean install
```

- Download [confluent cloud](https://www.confluent.io/download/).

- Put the jar in plugins directory.

- Start Zookeeper, Kafka, schema-registry.

## Configuration

- In `quickstart-s3.properties` change,

```
format.class=io.confluent.connect.s3.format.orc.OrcFormat
```

## Test cases

- Full test cases are available [here](src/test/java/io/confluent/connect/s3/DataWriterOrcTest.java)
