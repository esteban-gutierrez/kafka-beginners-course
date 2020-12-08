package com.github.simplestepgh.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Remember to create first the topic with the name specified in the config.properties file
 * in the property with key 'kafka.topic'.
 */
public class TwitterProducer {
    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static final String CONFIG_PROPERTIES = "src/main/resources/config.properties";
    private static final String CONSUMER_KEY = "twitter.consumer.key";
    private static final String CONSUMER_SECRET = "twitter.consumer.secret";
    private static final String TOKEN = "twitter.token";
    private static final String SECRET = "twitter.secret";
    private static final String BOOTSTRAP_SERVER = "bootstrap.server";
    private static final String KAFKA_TOPIC = "kafka.topic";

    private Properties globalProperties;

    public TwitterProducer() {
        try {
            // Reads sensitive data from properties file (not committed)
            globalProperties = readPropertiesFile(CONFIG_PROPERTIES);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }

    public void run() {
        try {
            /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

            // 1. Creates a Twitter client
            Client client = createTwitterClient(msgQueue);

            // Attempts to establish a connection.
            client.connect();

            // 2. Creates a Kafka producer
            KafkaProducer<String, String> producer = createKafkaProducer();

            // Nice improvement: shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                logger.info("Stopping application...");
                logger.info("Shutting down client from Twitter...");
                client.stop();
                logger.info("Closing producer...");
                producer.close();
                logger.info("All closed!");
            }));

            // 3. Loops and sends tweets to Kafka
            // on a different thread, or multiple different threads....
            while (!client.isDone()) {
                String msg = null;
                try {
                    msg = msgQueue.poll(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    client.stop();
                }
                if (msg != null) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>((String) globalProperties.get(KAFKA_TOPIC), null, msg);
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.error("Something bad happened", e);
                            }
                        }
                    });
                    logger.info(msg);
                }
            }
            logger.info("End of application");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka", "api", "biden", "covid");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1((String) globalProperties.get(CONSUMER_KEY),
                                                 (String) globalProperties.get(CONSUMER_SECRET),
                                                 (String) globalProperties.get(TOKEN),
                                                 (String) globalProperties.get(SECRET));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue); // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    private Properties readPropertiesFile(String propertiesFile) throws IOException {
        FileInputStream fis = null;
        Properties prop = null;
        try {
            fis = new FileInputStream(propertiesFile);
            prop = new Properties();
            prop.load(fis);
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } finally {
            fis.close();
        }
        return prop;
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        // 1. Creates Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) globalProperties.get(BOOTSTRAP_SERVER));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput producer (at expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32*1024)); // 32 KB

        // 2) Creates and returns the Producer
        return new KafkaProducer<String, String>(properties);
    }
}
