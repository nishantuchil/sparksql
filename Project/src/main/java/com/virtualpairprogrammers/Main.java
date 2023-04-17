package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    Configuration logConfig = loggerContext.getConfiguration();
    LoggerConfig loggerConfig = logConfig.getLoggerConfig("org.apache");
    loggerConfig.setLevel(Level.WARN);
    loggerContext.updateLoggers();

    // SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    // JavaSparkContext sc = new JavaSparkContext(conf);

    SparkSession spark =
        SparkSession.builder()
            .appName("testingsql")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
            .getOrCreate();

    Dataset<Row> dataset =
        spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
    // Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");

    Dataset<Row> modernArtResults =
        dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));

    modernArtResults.show();

    spark.close();
  }
}
