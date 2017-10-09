
# Kafka Java example

Simple project to demonstrate the use of Kafka's Java APIs, it is intended
to demonstrate the possibility of sending big messages to Kafka (10MB+).

Used the following articles to implement :

- [Confluent blog - Kafka 0.9 consumer client tutorial](https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/)
- [HDP documentation - Kafka development guide](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.2/bk_kafka-component-guide/content/ch_kafka-development.html)

## Description

The job has two modes :

- READ: Listens to given topic and prints every incoming message to
console (key as INFO and message as DEBUG)
- WRITE: Takes a directory, reads every file in it line by line and sends
the content to a Kafka producer (key is file name)

The messages are serialized as Byte array and compressed with Snappy.

## Setup

Project uses Maven + Java 8, only needs a functional Kafka cluster to
connect to.

Run `mvn clean package` to package Jar.

## Run

### Read mode

```
java \
  -Dlog4j.configuration=file:path/to/log4j.properties \
  -jar kafka_java_example-1.0.jar \
  --mode read \
  --topic <topic_name> \
  -ks <kafka_serv1:port,kafka_serv2:port,...>
```

### Write mode

```
java \
  -Dlog4j.configuration=file:path/to/log4j.properties \
  -jar kafka_java_example-1.0.jar \
  --mode write \
  --input-dir /path/to/input-dir \
  --topic <topic_name> \
  -ks <kafka_serv1:port,kafka_serv2:port,...>
```

## Contributors

* [César Berezowski](https://github.com/cesarBere)