package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.ArrayList;
import java.util.Arrays;
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

    /*
        dataset.createOrReplaceTempView("logging_view");

        Dataset<Row> results =
            spark.sql(
                "select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_view group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");
        results.show();

        // results = results.drop("monthnumber");
        results.show(100);

        results.createOrReplaceTempView("results_table");
        results = spark.sql("select sum(total) from results_table");

        results.show();
    */

    dataset =
        dataset.select(
            col("level"),
            date_format(col("datetime"), "MMMM").alias("month"),
            date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

    Object months[] =
        new Object[] {
          "January",
          "February",
          "March",
          "April",
          "May",
          "June",
          "July",
          "August",
          "September",
          "October",
          "November",
          "December",
          "TestingMonth"
        };
    List<Object> monthsList = Arrays.asList(months);

    dataset = dataset.groupBy(col("level")).pivot("month", monthsList).count().na().fill(0);
    // dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
    // dataset = dataset.orderBy(col("monthnum"), col("level")).drop(col("monthnum"));

    dataset.show(100);
    spark.close();
  }
}
