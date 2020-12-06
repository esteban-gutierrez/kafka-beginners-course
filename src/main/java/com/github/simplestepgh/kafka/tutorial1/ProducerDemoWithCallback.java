package com.github.simplestepgh.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        // 1. Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2.a) Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 2.b) Create a Producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        // 3. Send data (asynchronously)
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executed every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata: \n" +
                                "Topic: \t" + recordMetadata.topic() + "\n" +
                                "Partition: \t" + recordMetadata.partition() + "\n" +
                                "Offset: \t" + recordMetadata.offset() + "\n" +
                                "Timestamp: \t" + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing message", e);
                }
            }
        });

        // Flush data
        producer.flush();;

        // Flush and close producer
        producer.close();
    }
}
