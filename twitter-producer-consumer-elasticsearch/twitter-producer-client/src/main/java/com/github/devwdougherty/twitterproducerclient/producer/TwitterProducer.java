package com.github.devwdougherty.twitterproducerclient.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class TwitterProducer {

    @Value("${kafka.bootstrap-server}")
    private String kafkaBootStrapServer;

    Properties kafkaProperties = new Properties();

    public void producerTwitter() {

        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServer);
        KafkaProducer<String, String> twitterProducer = new KafkaProducer<String, String>(kafkaProperties);
    }
}
