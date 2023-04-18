package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

    // Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
    // modernArtResults.show();

    List<Row> inMemory = new ArrayList<Row>();
    inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
    inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
    inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
    inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
    inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

    StructField[] fields =
        new StructField[] {
          new StructField("level", DataTypes.StringType, false, Metadata.empty()),
          new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
    StructType schema = new StructType(fields);
    Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
    dataset.show();

    /*
    Dataset<Row> dataset =
        spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

    dataset.createOrReplaceTempView("my_students_view");

    Dataset<Row> results = spark.sql("select distinct(year) from my_students_view order by year");
    results.show();
    */
    spark.close();
  }
}
