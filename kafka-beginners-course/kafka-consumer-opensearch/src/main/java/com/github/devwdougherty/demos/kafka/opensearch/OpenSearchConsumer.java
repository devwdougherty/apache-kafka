package com.github.devwdougherty.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        //String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "opensearch-consumer-demo";

        // Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //none/earliest/latest

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {

        // Gson Library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        // First create an OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        // Create our Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // We need to create the index on OpenSearch if it doesn't exist already
        try {
            boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!indexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia index has been created!");
            } else {
                log.info("The Wikimedia index already exist!");
            }

            // Here we subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchanges"));

            while(true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordsCount = records.count();
                log.info("Received {} record(s)", recordsCount);

                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record: records) {

                    try {
                        // Strategy 1 -  Define an ID using Kafka Record coordinates
                        //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                        // Strategy 2 - We extract the ID from JSON Value using GSON Library
                        String id = extractId(record.value());

                        // Send the record into OpenSearch
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        //log.info(response.getId());

                        bulkRequest.add(indexRequest);
                    } catch(Exception e) {

                    }
                }

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} record(s)", bulkResponse.getItems().length);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception: {}", e.getMessage());
        }

        // Main code logic

        // Close things
        openSearchClient.close();
    }
}
