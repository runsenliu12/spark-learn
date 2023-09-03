package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: WordCount <inputFilePath>");
            System.exit(1);
        }

        String inputFilePath = args[0];

        // 配置 Spark
        SparkConf conf = new SparkConf()
                .setAppName("WordCountApp")
                .setMaster("local[*]"); // 使用本地模式，[*] 表示使用所有可用核心



        // 创建 Spark 上下文
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // 读取文本
            JavaRDD<String> lines = sc.textFile(inputFilePath);

            // 执行 Word Count
            JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
            JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((count1, count2) -> count1 + count2);

            // 打印结果
            wordCounts.foreach(pair -> System.out.println(pair._1() + ": " + pair._2()));
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            System.exit(1);
        } finally {
            // 关闭 Spark 上下文
            sc.stop();
        }
    }
}
