package com.github.simplestepgh.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallbackAndKeys {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackAndKeys.class);

    private static final String TOPIC_NAME = "first_topic";
    private static final String RECORD_VALUE = "hello world %s";
    private static final String KEY = "id_%s";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2.a) Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // 2.b) Create a Producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(TOPIC_NAME, String.format(KEY, i), String.format(RECORD_VALUE, i));

            logger.info("Key: \t" + String.format(KEY, i));
            // The keys always go to the same partition for a fixed number of partitions; in this case:
            // id_0 -> partition 1
            // id_1 -> partition 0
            // id_2 -> partition 2
            // id_3 -> partition 0
            // id_4 -> partition 2
            // id_5 -> partition 2
            // id_6 -> partition 0
            // id_7 -> partition 2
            // id_8 -> partition 1
            // id_9 -> partition 2

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
            }).get(); // block the .send() to make it synchronous - Don't do this in production!!
        }

        // Flush data
        producer.flush();;

        // Flush and close producer
        producer.close();
    }
}
