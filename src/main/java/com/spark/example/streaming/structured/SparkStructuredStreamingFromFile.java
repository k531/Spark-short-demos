package com.spark.example.streaming.structured;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;

public class SparkStructuredStreamingFromFile {
    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

        // 1. Initialize Spark Streaming Context
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();

        // 2. Set File stream
        StructType userSchema = new StructType().add("name", "string").add("age", "integer");
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .schema(userSchema)
                .option("header", true)
                .csv("src/main/resources/stream/structured/input");

        // 3. Operations
        // 3.1 Split the lines into words
        Dataset<String> words = csvDF.select(col("name"))
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        // 3.2 Generate running word count
        Dataset<Row> wordCounts2 = words.withColumn("time", current_timestamp()).withWatermark("time", "10 seconds").groupBy("time", "value").count();

        // 4. Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts2.writeStream()
                .format("csv")
                .trigger(Trigger.ProcessingTime("1 seconds"))
                .option("path", "src/main/resources/stream/structured/output")
                .option("checkpointLocation", "src/main/resources/stream/structured/checkpoint")
                .start();

        query.awaitTermination();
    }
}
