package com.sparkconf.operation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class WordCountTest {
    public static void main(String[] args) {

        // 配置 Spark
        SparkConf conf = new SparkConf()
                .setAppName("WordCountTest")
                .setMaster("local[*]"); // 使用本地模式，[*] 表示使用所有可用核心


        // 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);


        // 获取资源文件路径
        String resourcePath = WordCountTest.class.getClassLoader().getResource("sparkconfdata/input.txt").getPath();
        // 读取文本
        JavaRDD<String> lines = sc.textFile(resourcePath);

        // 执行 Word Count
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((count1, count2) -> count1 + count2);

        wordCounts.foreach(pair -> System.out.println(pair._1() + ": " + pair._2()));

        // 关闭 SparkContext
        sc.stop();
    }


}

