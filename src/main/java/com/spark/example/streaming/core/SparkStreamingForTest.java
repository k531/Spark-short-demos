package com.spark.example.streaming.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SparkStreamingForTest {
    public static void main(String[] args) throws InterruptedException {

        // 1. Initialize Spark Streaming Context
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = jssc.sparkContext().parallelize(data);

        final Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();

        rddQueue.add(distData);

        JavaDStream<Integer> lines = jssc.queueStream(rddQueue);

        JavaDStream<Integer> sum = lines.reduce(Integer::sum);

        sum.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
