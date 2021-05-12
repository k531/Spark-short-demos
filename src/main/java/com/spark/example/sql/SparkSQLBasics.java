package com.spark.example.sql;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkSQLBasics {

    public static void main(String[] args) throws IOException {

        // 1. Creating SparkSession
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("DemoSpark")
                .master("local")
                .getOrCreate();

        // 2.1 Reading data to DataFrame
        Dataset<Row> logData = sparkSession.read().option("inferSchema", "true")
                .option("header", "true").csv("src/main/resources/data_2.csv");

        // 2.2 Reading data from RDD
        RDD<String> fileRdd = sparkSession.sparkContext().textFile("src/main/resources/data.csv", 4);
        Dataset<String> lines =  sparkSession.createDataset(fileRdd, Encoders.STRING());

        // 3. Writing out
        logData.write().partitionBy("b").format("csv").option("sep", "^").mode("overwrite").save("sql_output");

        System.in.read();
        sparkSession.stop();
    }
}

