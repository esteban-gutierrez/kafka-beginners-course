package com.github.simplestepgh.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);
    private static final String TOPIC_NAME = "first_topic";
    private static final String GROUP_ID = "my-consumer-demo-with-assign-and-seek-application";

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

        // Assign and seek are mostly used to replay data or fetch a specific message:
        // 3.a) Assign
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC_NAME, 0);
        long offsetToReadFrom = 10L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // 3.b) Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesSoFar = 0;

        // 4. Poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.x
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesSoFar++;
                logger.info("Key: \t" + record.key() + "\n" +
                            "Value: \t " + record.value() + "\n" +
                            "Partition: \t" + record.partition() + "\n" +
                            "Offset: \t" + record.offset());
                if (numberOfMessagesSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application");
    }
}
