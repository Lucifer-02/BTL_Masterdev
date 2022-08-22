package Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws IOException, InterruptedException {
        run();
    }

    public static void run() throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "demo");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.193.52:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String dataPath = "../Crawl/videos.csv";
        String topicName = "video-tracking";

        File file = new File(dataPath);
        long lastTime = file.lastModified();

        while (true) {
            if (lastTime != file.lastModified()) {
                System.out.println("File changed!!!");
                lastTime = file.lastModified();
                sendMessage(file, producer, topicName, true);
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    private static void sendMessage(File file, KafkaProducer<String, String> producer, String
            topicName, boolean ignoreHeader) {
        try (Scanner scanner = new Scanner(file)) {
            if (ignoreHeader) {
                scanner.nextLine();
            }
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName, null, line));
                producer.flush();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}