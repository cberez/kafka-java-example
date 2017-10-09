package com.cberez.kafka_java_example;

import com.cberez.kafka_java_example.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;

/**
 * Hold write and read job logic
 *
 * @author César Berezowski
 */
public class Jobs {
    private static final Logger logger = Logger.getLogger(Jobs.class);
    private static KafkaConsumer<String, byte[]> consumer = null;

    /**
     * Read files from input dir and send content line by line to given kafka topic
     *
     * @param kafkaServers kafka connection string (server:port[,..])
     * @param topic        kafka topic name
     * @param inputDir     path to directory containing files to send
     *
     * @throws IOException thrown by file opening and reading
     */
    public static void write(String kafkaServers, String topic, String inputDir) throws IOException {
        KafkaProducer<String, byte[]> producer = KafkaUtils.createProducer(kafkaServers);
        ProducerCallback callback = new ProducerCallback();

        logger.info(String.format("Sending content of files in %s to topic %s", inputDir, topic));

        File folder = new File(inputDir);
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    logger.info(String.format("Processing %s", file.getPath()));
                    FileReader fileReader = new FileReader(file);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    String line;
                    while((line = bufferedReader.readLine()) != null) {
                        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, file.getName(), line.getBytes());
                        producer.send(data, callback);
                    }
                }
            }
        }

        producer.close();
    }

    /**
     * Listen on given kafka topic and print received content to console
     *
     * @param kafkaServers kafka connection string (server:port[,..])
     * @param topic        kafka topic name
     */
    public static void read(String kafkaServers, String topic) {
        consumer = KafkaUtils.createConsumer(kafkaServers);
        Runtime.getRuntime().addShutdownHook(new Thread(Jobs::shutdown));

        try {
            consumer.subscribe(Collections.singletonList(topic));
            logger.info(String.format("Listening for incoming messages on topic %s", topic));

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, byte[]> record : records) {
                    logger.info(String.format("Got a message with key %s", record.key()));
                    logger.debug(new String(record.value()));
                }
            }
        } catch (WakeupException e) {
            // Ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    /**
     * Properly shutdown read job consumer, called by shutdown hook
     */
    private static void shutdown() {
        logger.warn("Shutting down consumer");
        if (consumer != null) consumer.wakeup();
    }

    /**
     * Callback class used when sending a message in the read job
     */
    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                logger.error(String.format("Error while sending message to topic %s", recordMetadata), e);
            } else {
                logger.info(String.format("sent message to topic:%s partition:%s offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()));
            }
        }
    }
}
