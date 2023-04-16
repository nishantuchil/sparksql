package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;
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

import scala.Tuple2;

public class Main {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    Configuration logConfig = loggerContext.getConfiguration();
    LoggerConfig loggerConfig = logConfig.getLoggerConfig("org.apache");
    loggerConfig.setLevel(Level.WARN);
    loggerContext.updateLoggers();

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

    JavaRDD<String> lettersOnlyRdd =
        initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

    JavaRDD<String> removedBlankLines =
        lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

    JavaRDD<String> justWords =
        removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

    JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

    JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));

    JavaPairRDD<String, Long> pairRdd =
        justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));

    JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

    JavaPairRDD<Long, String> switched =
        totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));

    JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

    List<Tuple2<Long, String>> results = sorted.take(10);

    results.forEach(result -> System.out.println(result));

    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();

    sc.close();
  }
}
