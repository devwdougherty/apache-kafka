package com.github.devwdougherty.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        /*
            Kafka will convert whenever we send to bytes, so we need to tell Kafka the type of the data.
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(Integer i = 0; i < 10; i++) {
            // Create a producer record
            String topic = "first_topic";
            String message = "hello world " + Integer.toString(i);
            String key = "ID_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

            logger.info("KEY: " + key);

            // Send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    /* This executes every time a record is successfully sent or an exception is thrown.*/

                    if (e == null) {
                        /* The record was successfully sent. */
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing: " + e);
                    }
                }
            }).get();
        }

        // Flush data
        producer.flush();
        // Flush and close Producer
        producer.close();
    }
}
