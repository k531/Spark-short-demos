package com.spark.example.streaming;

import com.spark.example.streaming.model.JavaRow;
import org.apache.parquet.format.StringType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingDataframeOperations {
    public static void main(String[] args) throws InterruptedException {

        // 1. Initialize Spark Streaming Context
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        words.foreachRDD((rdd, time) -> {
            // Get the singleton instance of SparkSession
            SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

            // Convert RDD[String] to RDD[case class] to DataFrame
            JavaRDD<JavaRow> rowRDD = rdd.map(word -> {
                JavaRow record = new JavaRow();
                record.setWord(word);
                return record;
            });
            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);

            // Creates a temporary view using the DataFrame
            wordsDataFrame.createOrReplaceTempView("words");

            // Do word count on table using SQL and print it
            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count(*) as total from words group by word");
            wordCountsDataFrame.show();
        });


        jssc.start();
        jssc.awaitTermination();
    }
}
