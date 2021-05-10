package com.spark.example.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;

public class SparkSQLDataFrameOperations {

    public static void main(String[] args) throws IOException {

        // 1. Creating SparkSession
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("DemoSpark")
                .master("local")
                .getOrCreate();

        // 2. Reading data to DataFrame
        Dataset<Row> df = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/data_2.csv");

        // 3. Applying some operation
        df.select(col("b"))
                .filter(col("b").startsWith("e").or(col("b").startsWith("b")))
                .groupBy(col("b"))
                .count()
                .show();

        // 4. Writing out the dataset
        df.write()
                .partitionBy("b")
                .format("csv")
                .option("sep", "^")
                .mode("overwrite")
                .save("ala.csv");

        System.in.read();
        sparkSession.stop();

    }
}

