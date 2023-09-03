package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordMaxLineExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordMaxLineExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Tuple2<Integer, String>> lines = sc.parallelize(
                Arrays.asList(
                        new Tuple2<>(1, "Hello world"),
                        new Tuple2<>(2, "This is a Spark example"),
                        new Tuple2<>(3, "Spark is fun and powerful"),
                        new Tuple2<>(4, "Hello again")
                )
        );

        // 使用flatMapToPair操作将每个单词映射为键值对 (单词, 行号)，其中行号是Tuple2的第一个元素
        JavaPairRDD<String, Integer> wordMaxLine = lines
                .flatMapToPair(line -> {
                    // 拆分每行的文本内容为单词
                    String[] words = line._2.split(" ");
                    List<Tuple2<String, Integer>> wordWithLine = new ArrayList<>();
                    for (String word : words) {
                        // 创建键值对 (单词, 行号)
                        wordWithLine.add(new Tuple2<>(word, line._1));
                    }
                    return wordWithLine.iterator(); // 返回包含单词和行号的迭代器
                })
                // 使用reduceByKey操作，根据单词对行号进行聚合，找出每个单词出现的最大行号
                .reduceByKey((line1, line2) -> Math.max(line1, line2));

        wordMaxLine.foreach(tuple -> {
            System.out.println("Word: " + tuple._1() + ", Max Line: " + tuple._2());
        });

        sc.stop();
    }
}
