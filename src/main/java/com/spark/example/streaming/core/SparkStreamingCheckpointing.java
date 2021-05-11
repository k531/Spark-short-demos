package com.spark.example.streaming.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingCheckpointing {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .set("spark.stream.receiver.WriteAheadLog.enable", "true")
                .setAppName("NetworkWordCount");

        // Creating Contextfactory
        Function0<JavaStreamingContext> contextFactory = () -> {
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

            setUpFlow(jssc);

            jssc.checkpoint("checkpoint");

            return jssc;
        };

        // 1.Initialize Spark Streaming Context
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate("checkpoint", contextFactory);

        jssc.start();
        jssc.awaitTermination();
    }

    private static void setUpFlow(JavaStreamingContext jssc) {

        // Init input stream
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        // Configuring Checkpoint
        lines.checkpoint(Durations.seconds(1));

        // Operations
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        wordCounts.print();
    }
}
