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

public class TwitterProducer {
    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static final String CONFIG_PROPERTIES = "src/main/resources/config.properties";
    private static final String CONSUMER_KEY = "twitter.consumer.key";
    private static final String CONSUMER_SECRET = "twitter.consumer.secret";
    private static final String TOKEN = "twitter.token";
    private static final String SECRET = "twitter.secret";

    public TwitterProducer() {
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

    public static void main(String[] args) throws IOException {
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
        List<String> terms = Lists.newArrayList("kafka", "api");
        hosebirdEndpoint.trackTerms(terms);

        // Reads sensitive data from properties file (not committed)
        Properties twitterProperties = readPropertiesFile(CONFIG_PROPERTIES);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1((String) twitterProperties.get(CONSUMER_KEY),
                                                 (String) twitterProperties.get(CONSUMER_SECRET),
                                                 (String) twitterProperties.get(TOKEN),
                                                 (String) twitterProperties.get(SECRET));

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
}
