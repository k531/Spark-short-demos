package com.spark.example.core.source.collection;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.function.Function.identity;

public class SparkCoreWithCollectionSource {
    public static void main(String[] args) throws IOException {

        // 1. Creating Spark Context
        SparkConf sparkConf = new SparkConf().setAppName("DemoSpark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 2  Parallelize an existing collection in the driver program
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> parallelizedRdd = sc.parallelize(data);


        // 3 Spark Operations
        long count = parallelizedRdd.count();
        System.out.printf("Result: %d\n", count); // Action

        System.in.read();
        sc.stop();
    }
}
