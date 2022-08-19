package Spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DataProcessing {
    public static void main(String[] args) throws IOException, InterruptedException {
        run();
//        checkLastModifiedTime();
    }

    public static void run() throws IOException, InterruptedException {
        SparkSession spark = SparkSession
                .builder()
                .appName("hoangnlv DataProcessing")
//                .config("spark.master", "local")
                .config("spark.dynamicAllocation.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");


        Dataset<Row> parquetFileDF = spark.read().parquet("hdfs://172.17.80.21:9000/user/hoangnlv/btl/output/data_tracking");
//        Dataset<Row> parquetFileDF = spark.read().parquet("./output/data_tracking/videos");

        parquetFileDF.show();

        FileSystem fs = FileSystem.get(new Configuration());
//        String hdfsFilePath = "./output/data_tracking/";
        String hdfsFilePath = "hdfs://172.17.80.21:9000/user/hoangnlv/btl/output/data_tracking";
        long lastAccessTimeLong = 0;
        while (true) {
            FileStatus[] status = fs.listStatus(new Path(hdfsFilePath));  // you need to pass in your hdfs path
            if (lastAccessTimeLong != status[0].getModificationTime()) {
                System.out.println("File has been modified!!!");
                lastAccessTimeLong = status[0].getModificationTime();
                removeTimestampFields(parquetFileDF, spark);
            }
            TimeUnit.SECONDS.sleep(2);
        }

    }

    public static void checkLastModifiedTime() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
//        String hdfsFilePath = "./output/data_tracking/";
        String hdfsFilePath = "hdfs://172.17.80.21:9000/user/hoangnlv/btl/output/data_tracking/";
        FileStatus[] status = fs.listStatus(new Path(hdfsFilePath));  // you need to pass in your hdfs path
        long lastAccessTimeLong = status[0].getModificationTime();

        // convert to date
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date lastAccessTime = new Date(lastAccessTimeLong);
        System.out.println("Last modified time: " + formatter.format(lastAccessTime));
    }

    public static void removeTimestampFields(Dataset<Row> dataset, SparkSession spark) {
        dataset.createOrReplaceTempView("videos");
        Dataset<Row> simple = spark.sql(
                "SELECT index, title, published_date, views, likes, comments FROM videos");
        simple.show();

        // save to parquet file
        simple.write().mode("overwrite")
                .format("parquet")
//                .save("./output/data_tracking/videos/result");
                .save("hdfs://172.17.80.21:9000/user/hoangnlv/btl/output/data_tracking/result");
    }
}
