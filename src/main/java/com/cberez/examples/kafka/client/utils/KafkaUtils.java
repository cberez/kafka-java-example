package com.cberez.examples.kafka.client.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Handle kafka producer and consumer creation
 *
 * @author César Berezowski
 */
public class KafkaUtils {

    /**
     * Create a {@link Properties} with properties common to producer and consumer
     *
     * @param kafkaServers kafka connection string (server:port[,..])
     * @return {@link Properties} instance
     */
    private static Properties createProps(String kafkaServers) {
        Properties props = new Properties();

        props.put("group.id", "kafka_java_example");
        props.put("bootstrap.servers", kafkaServers);

        props.put("batch.size", "16384");
        props.put("buffer.memory", "33554432");
        props.put("compression.codec", "snappy");

        return props;
    }

    /**
     * Create a {@link org.apache.kafka.clients.producer.Producer}
     *
     * @param kafkaServers kafka connection string (server:port[,..])
     * @return {@link KafkaProducer<String, byte[]>} instance
     */
    public static KafkaProducer<String, byte[]> createProducer(String kafkaServers) {
        Properties props = createProps(kafkaServers);

        // http://www.bigendiandata.com/2016-08-14-How-to-use-Java-Serializers-with-Kafka/
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Max message size
        props.put("max.request.size", 1024 * 1024 * 15); // 15 MB

        return new KafkaProducer<>(props);
    }

    /**
     *
     * Create a {@link org.apache.kafka.clients.consumer.Consumer}
     *
     * @param kafkaServers kafka connection string (server:port[,..])
     * @return {@link KafkaConsumer<String, byte[]>} instance
     */
    public static KafkaConsumer<String, byte[]> createConsumer(String kafkaServers) {
        Properties props = createProps(kafkaServers);

        // http://www.bigendiandata.com/2016-08-14-How-to-use-Java-Serializers-with-Kafka/
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Max message size
        props.put("max.partition.fetch.bytes", 1024 * 1024 * 15); // 15 MB

        return new KafkaConsumer<>(props);
    }
}
