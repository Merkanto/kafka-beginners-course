package com.merkanto.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to Localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Conduktor Playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"ws1nzQgreZEXs5TzBABnt\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ3czFuelFncmVaRVhzNVR6QkFCbnQiLCJvcmdhbml6YXRpb25JZCI6NzMyMzgsInVzZXJJZCI6ODUxNDUsImZvckV4cGlyYXRpb25DaGVjayI6ImM3OTYzNTMzLTg2Y2ItNGI0YS05ZGMxLTIyODdhNTJhYjZjMyJ9fQ.l1OiYLy0XqjrlTd81polrBVRZ7FyZb_vjG5dHLYLJzM\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world " + i);

            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // executes every time a record successfully sent or an exception is thrown
                    if (e == null) {
                        // the record successfully sent
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the Producer
        producer.close();
    }
}
