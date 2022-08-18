package Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws IOException, InterruptedException {
        run();
    }

    public static void run() throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "demo");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        String dataPath = "/media/lucifer/STORAGE/IMPORTANTS/Documents/INTERN/BTL_Masterdev/Crawl/videos.csv";
        String topicName = "quickstart-events";

        File file = new File(dataPath);
        long lastTime = file.lastModified();

        while (true) {
            if (lastTime != file.lastModified()) {
                System.out.println("changed!!");
                new Records(producer, topicName, dataPath, true).run();
                lastTime = file.lastModified();
            }
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }
}