package Spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataProcessing {
    public static void main(String[] args) {
        run();
    }

    public static void run() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();


        Dataset<Row> parquetFileDF = spark.read().parquet("./output/data_tracking/");
        parquetFileDF.createOrReplaceTempView("videos");
        Dataset<Row> titles = spark.sql(
                "SELECT title FROM videos");
        titles.show();
        // show number of rows
        System.out.println("Number of rows: " + parquetFileDF.count());

        spark.stop();

    }
}
