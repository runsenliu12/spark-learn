package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FlatMapDistinctExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FlatMapDistinctExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourcePath = WordCount.class.getClassLoader().getResource("sparkconfdata/input.txt").getPath();

        // 读取文本文件并创建包含文本行的 RDD
        JavaRDD<String> linesRDD = sc.textFile(resourcePath);


        // 使用 flatMap 将每行拆分成单词
        JavaRDD<String> wordsRDD = linesRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // 使用 distinct 去除重复的单词
        JavaRDD<String> distinctWordsRDD = wordsRDD.distinct();

        // 打印不重复的单词
        distinctWordsRDD.foreach(word -> System.out.println(word));


        List<String> uniqueWordsList  = sc.textFile(resourcePath)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .distinct()
                .collect();


        // 将不重复的单词列表合并成一个字符串，以空格分隔
        String uniqueWordsString = String.join(" ", uniqueWordsList);

        // 输出不重复的单词字符串
        System.out.println(uniqueWordsString);

        sc.stop();
    }
}
