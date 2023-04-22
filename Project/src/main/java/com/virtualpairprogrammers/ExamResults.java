package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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

    spark
        .udf()
        .register(
            "hasPassed",
            (String grade, String subject) -> {
              if (subject.equals("Biology")) {
                if (grade.startsWith("A")) {
                  return true;
                }
                return false;
              }
              return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
            },
            DataTypes.BooleanType);
    Dataset<Row> dataset =
        spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

    dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));

    dataset.show();
  }
}
