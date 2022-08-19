package SparkStream;


import com.opencsv.CSVReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.StringReader;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class Streaming {

    public static void main(String[] args) throws InterruptedException {

        // Create a local StreamingContext and batch interval of x second
        SparkConf conf = new SparkConf()
//                .setMaster("local")
                .setAppName("Kafka Spark Integration");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));


        //Define Kafka parameter
        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
//        kafkaParams.put("bootstrap.servers", "172.17.80.21:9092");
        kafkaParams.put("bootstrap.servers", "172.16.30.205:9092");

        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "0");

        // Automatically reset the offset to the earliest offset
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        //Define a list of Kafka topic to subscribe
        Collection<String> topics = Collections.singletonList("videos-tracking");


        // Consume String data from Kafka
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> records = stream.map((Function<ConsumerRecord<String, String>, String>) ConsumerRecord::value);


//      Convert RDDs of the records DStream to DataFrame and save
        records.foreachRDD((rdd, time) -> {
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

            // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
            JavaRDD<Video> rowRDD = rdd.map((Function<String, Video>) chunk -> {
                CSVReader csvReader = new CSVReader(new StringReader(chunk));
                String[] line = csvReader.readNext();
                Video video = new Video();
                try {
                    video.setIndex(Integer.parseInt(line[0]));
                    video.setTitle(line[1]);
                    video.setPublished_date(Date.valueOf(line[2]));
                    video.setViews(Integer.parseInt(line[3]));
                    video.setLikes(Integer.parseInt(line[4]));
                    video.setComments(Integer.parseInt(line[5]));
                    video.setTimestamp(new Timestamp(Long.parseLong(line[6])));


//                    // parse timestamp
//                    long timestamp = Long.parseLong(line[6]);
//                    LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), java.time.ZoneId.systemDefault());
//
//                    video.setYear(localDateTime.getYear());
//                    video.setMonth(localDateTime.getMonthValue());
//                    video.setDay(localDateTime.getDayOfMonth());
//                    video.setHour(localDateTime.getHour());
//                    video.setMinute(localDateTime.getMinute());

                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                }
                return video;
            });

            Dataset<Row> rowDataset = spark.createDataFrame(rowRDD, Video.class);
            rowDataset.show();

            if (rowDataset.isEmpty()) {
                System.out.println("Empty DataFrame");
            } else {
                rowDataset.write().mode("append")
//                        .option("compression", "snappy")
                        .option("checkpointLocation", "hdfs://172.17.80.21:9000/user/hoangnlv/btl/output/data_tracking/checkpoint")
//                        .option("checkpointLocation", "./output/data_tracking/checkpoint")
                        .format("parquet")
//                        .partitionBy("year", "month", "day", "hour", "minute")
//                        .save("./output/data_tracking/checkpoint");
                        .save("hdfs://172.17.80.21:9000/user/hoangnlv/btl/output/data_tracking");
            }

        });


        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}

/**
 * Lazily instantiated singleton instance of SparkSession
 */
class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;

    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}