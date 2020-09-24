import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaProd {


    public static void main(String[] args) {
        //create properties for producer
        final Logger log = LoggerFactory.getLogger(KafkaProd.class);
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer object
        KafkaProducer<String, String> kp = new KafkaProducer<String, String>(p);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> pr = new ProducerRecord<String, String>("first_topic", "hey how are you");

            //send data
            kp.send(pr, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Data inserted successfully in" + recordMetadata.topic() +
                                recordMetadata.partition());
                    } else {
                        log.error("error" + e);
                    }

                }
            });
        }
        kp.flush();
        kp.close();

    }
}
