package com.other;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.Arrays;

public class SparkOperationsExample {
    public static void main(String[] args) {
        // 创建Spark配置
        SparkConf conf = new SparkConf().setAppName("SparkOperationsExample").setMaster("local[*]");

        // 创建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建一个整数RDD
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // 使用map操作将每个整数平方
        JavaRDD<Integer> squaredRDD = inputRDD.map(x -> x * x);

        // 输出平方后的结果
        System.out.println("Squared RDD: " + squaredRDD.collect());

        // 创建一个包含文本行的RDD
        JavaRDD<String> textRDD = sc.parallelize(Arrays.asList("Hello World", "Spark is great"));

        // 使用flatMap操作拆分并展平文本行
        JavaRDD<String> wordsRDD = textRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // 输出拆分后的单词
        System.out.println("Words RDD: " + wordsRDD.collect());

        // 创建一个包含单词的RDD
        JavaRDD<String> wordRDD = sc.parallelize(Arrays.asList("Hello", "World", "Spark", "is", "great"));

        // 使用mapToPair操作为每个单词创建键值对 (word, 1)
        JavaPairRDD<String, Integer> pairRDD = wordRDD.mapToPair(word -> new Tuple2<>(word, 1));

        // 输出键值对RDD
        System.out.println("Pair RDD: " + pairRDD.collect());

        // 创建一个包含键值对的PairRDD
        JavaPairRDD<String, Integer> keyValueRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("apple", 3),
                new Tuple2<>("banana", 2),
                new Tuple2<>("cherry", 5)
        ));

        // 使用mapValues操作将值翻倍
        JavaPairRDD<String, Integer> doubledValuesRDD = keyValueRDD.mapValues(value -> value * 2);

        // 输出翻倍后的键值对RDD
        System.out.println("Doubled Values Pair RDD: " + doubledValuesRDD.collect());

        // 关闭Spark上下文
        sc.close();
    }
}
