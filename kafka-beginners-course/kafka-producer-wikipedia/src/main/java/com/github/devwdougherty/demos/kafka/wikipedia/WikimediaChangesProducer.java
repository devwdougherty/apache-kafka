package com.github.devwdougherty.demos.kafka.wikipedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // For safe producer configs (Kafka <= 2.8) we need to set: ENABLE_IDEMPOTENCE_CONFIG(true), ACKS_CONFIG(all), RETRIES_CONFIG(Integer.MAX_VALUE)

        // Set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchanges";

        EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url)).build();

        // Start the producer in another thread
        eventSource.start();

        // We produce for X minutes and block the program until then
        TimeUnit.MINUTES.sleep(1);
    }
}
