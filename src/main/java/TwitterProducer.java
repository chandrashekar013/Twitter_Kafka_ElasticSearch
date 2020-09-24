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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // create twitter client
        // Attempts to establish a connection.
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create kafka producer
        KafkaProducer<String, String> kp = createKafkaProducer();

        //add showdonw hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            client.stop();
            kp.close();
            log.info("producer and client shutdown");
        }));

        // loop tweets
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                kp.send(new ProducerRecord<>("tweeter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            log.error("Something bad ahppened");
                        }
                    }
                });
            }

        }
        log.info("end of app");
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        final Logger log = LoggerFactory.getLogger(KafkaProd.class);
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer object
        KafkaProducer<String, String> kp = new KafkaProducer<String, String>(p);
        return kp;
    }

    String consumerKey = "dOHiV1vYFQjA4Zixo4SHP4ZxU";
    String consumerSecret = "nQe5eibr4JGRVYgyeguMz1ZEqX40cThW7C8UmIkG9XalvjhLOR";
    String token = "707230359-2tGBjJhM6tfJyEPmA7412ZukEXKaV7IVRD8BKyxo";
    String secret = "UzgEILCXsnj1EPahuBOMwec3p9Yx9YHdoj4zKI6b6wGgN";

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("modi");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
}
