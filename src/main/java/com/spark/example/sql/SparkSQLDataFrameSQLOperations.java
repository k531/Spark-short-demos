package com.spark.example.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkSQLDataFrameSQLOperations {

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

        // createGlobalTempView to access table within multiple Sessions
        // 3. Creating View for querying
        df.createOrReplaceTempView("test");

        // 4. Running Spark SQL
        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM test WHERE length(b) > 1");

        sqlDF.show();

        System.in.read();
        sparkSession.stop();

    }
}

