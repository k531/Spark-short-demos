package com.spark.example.sql;

import com.spark.example.sql.model.Dog;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkSQLDataSetOperations {

    public static void main(String[] args) throws IOException {

        // 1. Creating SparkSession
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("DemoSpark")
                .master("local")
                .getOrCreate();

        // 2. Creating Encoder
        Encoder<Dog> dogEncoder = Encoders.bean(Dog.class);

        // 3. Reading data to DataFrame
        Dataset<Dog> ds = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/dogs.csv")
                .as(dogEncoder);

        Dataset<Dog> filteredDs = ds.filter((FilterFunction<Dog>) dog -> dog.getAge() > 5)
                .distinct();

        filteredDs.show();

        System.in.read();
        sparkSession.stop();

    }
}

