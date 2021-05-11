package com.spark.example.core.source.file;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.function.Function.identity;

public class SparkCoreWithFileSource {
    public static void main(String[] args) throws IOException {

        // 1. Creating Spark Context
        SparkConf sparkConf = new SparkConf().setAppName("DemoSpark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 2  Referencing a dataset in an external storage system
        JavaRDD<String> fileRdd = sc.textFile("src/main/resources/data.csv", 4);

        List<String>[] partitions = fileRdd.collectPartitions(new int[] {0, 1, 2, 3});
        Map<Integer, List<String>> partitionsByKey = IntStream.range(0, partitions.length).boxed().collect(Collectors.toMap(identity(), index -> partitions[index]));
        partitionsByKey.forEach((part_index, partition) ->  System.out.printf("Partition %d, content: %s\n", part_index, partition));

        // 3 Spark Operations
        JavaPairRDD<String, Integer> pairs = fileRdd.mapToPair(s -> new Tuple2(s, 1)).cache(); // Transform
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum); // Transform


        counts.collect().forEach( count -> System.out.printf("Result: %s: %d\n", count._1, count._2)); // Action

        pairs.saveAsTextFile("spark_core_output");

        System.in.read();
        sc.stop();
    }
}
