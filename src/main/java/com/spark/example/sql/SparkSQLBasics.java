package com.spark.example.sql;

import org.apache.spark.sql.Dataset;
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

        // 2. Reading data to DataFrame
        Dataset<Row> logData = sparkSession.read().option("inferSchema", "true")
                .option("header", "true").csv("src/main/resources/data_2.csv");

        // 3. Writing out
        logData.write().partitionBy("b").format("csv").option("sep", "^").mode("overwrite").save("ala.csv");

        System.in.read();
        sparkSession.stop();
    }
}

