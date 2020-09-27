
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.omg.Messaging.SyncScopeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsum {

    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(KafkaConsum.class);

        //create properties for consumer
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_fourth_app");
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer object
        KafkaConsumer<String, String> kc = new KafkaConsumer<String, String>(p);

        //subscribe to topic
        kc.subscribe(Arrays.asList("first_topic"));

        //poll data
        while (true) {
            ConsumerRecords<String, String> cr = kc.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> r : cr) {
                System.out.println(r.key() + r.value() +
                        r.topic() + r.offset());

            }
        }

    }
}
