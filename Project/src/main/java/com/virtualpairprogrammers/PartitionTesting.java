package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class PartitionTesting {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    Configuration logConfiguration = logContext.getConfiguration();
    LoggerConfig loggerConfig = logConfiguration.getLoggerConfig("org.apache");
    loggerConfig.setLevel(Level.WARN);
    logContext.updateLoggers();

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");

    System.out.println("Number of cores = " + Runtime.getRuntime().availableProcessors());

    System.out.println("Initial RDD Partition Size: " + initialRdd.getNumPartitions());

    JavaPairRDD<String, String> warningsAgainstDate =
        initialRdd.mapToPair(
            inputLine -> {
              String[] cols = inputLine.split(":");
              String level = cols[0];
              String date = cols[1];
              return new Tuple2<>(level, date);
            });

    System.out.println(
        "After a narrow transformation we have "
            + warningsAgainstDate.getNumPartitions()
            + " parts");

    // Now we're going to do a "wide" transformation
    JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();

    results = results.persist(StorageLevel.MEMORY_AND_DISK());

    System.out.println(results.getNumPartitions() + " partitions after the wide transformation");

    results.foreach(
        it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));

    System.out.println(results.count());

    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();
    sc.close();
  }
}
