package com.github.simplestepgh.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "first_topic";
    private static final String GROUP_ID = "my-consumer-demo-with-threads-application";

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private void run() {
        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");
        // Create the consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVER, GROUP_ID, TOPIC_NAME, latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException ie) {
            logger.error("Application got interrupted", ie);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            // 1. Create Consumer configuration
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // 2. Create Consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // 3. Subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    //consumer.poll(100); // new in Kafka 2.x
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.x
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: \t" + record.key() + "\n" +
                                "Value: \t " + record.value() + "\n" +
                                "Offset: \t" + record.offset());
                    }
                }
            } catch (WakeupException we) {
                logger.warn("Received shutdown signal!");
            } finally {
                consumer.close(); // really important!
                // tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // 'wakeup' method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();;
        }
    }
}
