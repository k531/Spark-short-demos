package com.spark.example.streaming.core;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingFromFile {

    public static void main(String[] args) throws InterruptedException {

        // 1. Initialize Spark Streaming Context
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // 2. Init filestream
        JavaDStream<String> lines = jssc.textFileStream("src/main/resources/stream/input");

        // 3. Operations
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
