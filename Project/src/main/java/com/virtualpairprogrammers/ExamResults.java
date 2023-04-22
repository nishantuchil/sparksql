package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExamResults {

  public static void main(String args[]) {

    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration logConfiguration = context.getConfiguration();
    LoggerConfig loggerConfig = logConfiguration.getLoggerConfig("org.apache");
    loggerConfig.setLevel(Level.WARN);
    context.updateLoggers();

    SparkSession spark =
        SparkSession.builder()
            .appName("testingSql")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///c:/tmp")
            .getOrCreate();

    Dataset<Row> dataset =
        spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

    /*
    dataset =
        dataset
            .groupBy(col("subject"))
            .agg(max(col("score")).alias("max score"), min(col("score")).alias("min score"));
    */

    dataset =
        dataset
            .groupBy(col("subject"))
            .pivot(col("year"))
            .agg(
                round(avg(col("score")), 2).alias("average"),
                round(stddev(col("score")), 2).alias("stddev"));
    dataset.show();
  }
}
