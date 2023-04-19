package com.virtualpairprogrammers;

import java.util.ArrayList;

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

    ArrayList<Row> dataArray = new ArrayList<Row>();
    dataArray.add(RowFactory.create(1L, "Hello World"));

    SparkSession spark =
        SparkSession.builder()
            .appName("testingsql")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
            .getOrCreate();

    StructField[] fields =
        new StructField[] {
          new StructField("id", DataTypes.LongType, false, Metadata.empty()),
          new StructField("name", DataTypes.StringType, false, Metadata.empty())
        };
    StructType schema = new StructType(fields);

    Dataset<Row> dataSet = spark.createDataFrame(dataArray, schema);
    dataSet.show();

    Dataset<Row> dataset =
        spark.read().option("header", true).csv("src/main/resources/biglog_1.txt");
    //    dataset.show();

    dataset.createOrReplaceTempView("logging_view");

    Dataset<Row> results =
        spark.sql(
            "select level, date_format(datetime, 'MMMM') as month, date_format(datetime, 'M') as monthnum from logging_view order by monthnum");
    results.show();

    results.createOrReplaceTempView("logging_view");
    results =
        spark.sql(
            "select level, month, count(1) as total from logging_view group by level, month order by cast(first(monthnum) as int), level");
    // results = results.drop("monthnumber");
    results.show(100);

    results.createOrReplaceTempView("results_table");
    results = spark.sql("select sum(total) from results_table");

    results.show();

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
