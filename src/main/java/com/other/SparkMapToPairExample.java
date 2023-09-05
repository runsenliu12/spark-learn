package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkMapToPairExample {
    public static void main(String[] args) {
        // 创建 Spark 配置
        SparkConf conf = new SparkConf().setAppName("SparkMapToPairExample").setMaster("local[*]");

        // 创建 JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 假设你有一个包含学生姓名和他们的成绩的RDD
        JavaRDD<String> inputRDD = sc.parallelize(
                Arrays.asList("Alice 85", "Bob 76", "Charlie 92", "David 78", "Eve 88", "Frank 95", "Grace 89")
        );

        // 使用 mapToPair 将数据映射为键值对，键是姓名，值是成绩
        JavaPairRDD<String, Integer> studentScoresRDD = inputRDD.mapToPair(line -> {
            String[] parts = line.split(" ");
            String name = parts[0];
            int score = Integer.parseInt(parts[1]);
            return new Tuple2<>(name, score);
        });

        // 使用 reduceByKey 计算每个学生的平均分数，并将结果以键值对的形式返回
        JavaPairRDD<String, Double> averageScoresRDD = studentScoresRDD
                .groupByKey()
                .mapToPair(student -> {
                    String name = student._1;
                    Iterable<Integer> scores = student._2;
                    int sum = 0;
                    int count = 0;
                    for (int score : scores) {
                        sum += score;
                        count++;
                    }
                    double average = (double) sum / count;
                    return new Tuple2<>(name, average);
                });

        // 打印每个学生的平均分数
        averageScoresRDD.foreach(student -> System.out.println(student._1 + ": " + student._2));

        // 关闭 SparkContext
        sc.close();
    }
}
