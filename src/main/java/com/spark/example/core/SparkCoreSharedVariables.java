package com.spark.example.core;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SparkCoreSharedVariables {
    public static void main(String[] args) throws IOException {

        // 1. Creating Spark Context
        SparkConf sparkConf = new SparkConf().setAppName("DemoSpark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 2. Creating BrodCast var
        Broadcast<Integer> broadcastVar = sc.broadcast(99);

        // 3. Creating Accumulator
        LongAccumulator accum = sc.sc().longAccumulator("MyAccumulator");

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data, 2);


        // 4. Using broadcast variable for addition to the Accumulator
        rdd.foreach(x -> accum.add(x + broadcastVar.value()));

        System.out.printf("BrodCast var value: %d, Accumulator value: %d\n", broadcastVar.value(), accum.value());

        System.in.read();
        sc.stop();
    }
}
