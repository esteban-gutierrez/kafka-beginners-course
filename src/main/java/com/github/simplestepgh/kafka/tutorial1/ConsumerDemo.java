package com.github.simplestepgh.kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    private static final String TOPIC_NAME = "first_topic";
    private static final String GROUP_ID = "my-kafka-demo-application";

    public static void main(String[] args) {
        // 1. Create Consumer configuration
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 3. Subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 4. Poll for new data
        while (true) {
            //consumer.poll(100); // new in Kafka 2.x
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.x
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: \t" + record.key() + "\n" +
                            "Value: \t " + record.value() + "\n" +
                            "Offset: \t" + record.offset());
            }
        }

    }
}
