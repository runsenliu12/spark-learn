package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.DecimalFormat; // 导入DecimalFormat
import java.util.Arrays;

public class GroupByKeyExample {
    public static void main(String[] args) {
        // 创建Spark配置
        SparkConf conf = new SparkConf().setAppName("GroupByKeyExample").setMaster("local");

        // 创建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建包含学生姓名和分数的键值对RDD
        JavaPairRDD<String, Double> studentScoresRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("Alice", 90.0),
                new Tuple2<>("Bob", 85.0),
                new Tuple2<>("Alice", 92.0),
                new Tuple2<>("Bob", 88.0),
                new Tuple2<>("Alice", 87.0)
        ));

        // 使用groupByKey操作按学生姓名分组
        JavaPairRDD<String, Iterable<Double>> groupedRDD = studentScoresRDD.groupByKey();

        // 计算每个学生的平均分数
        JavaPairRDD<String, Double> averageScoresRDD = groupedRDD.mapToPair(tuple -> {
            // 从元组中获取学生姓名
            String studentName = tuple._1();

            // 从元组中获取学生分数的可迭代集合
            Iterable<Double> scores = tuple._2();

            // 初始化分数总和和分数计数器
            double sum = 0.0;
            int count = 0;

            // 遍历学生分数集合，计算总和和分数计数
            for (Double score : scores) {
                sum += score;
                count++;
            }

            // 计算平均分数
            double average = sum / count;

            // 使用DecimalFormat来保留两位小数
            DecimalFormat df = new DecimalFormat("#.00");

            // 格式化平均分数并转换为双精度浮点数
            double roundedAverage = Double.parseDouble(df.format(average));

            // 返回包含学生姓名和平均分数的元组
            return new Tuple2<>(studentName, roundedAverage);
        });

        // 打印结果
        averageScoresRDD.foreach(tuple -> {
            System.out.println("Student: " + tuple._1() + ", Average Score: " + tuple._2());
        });

        // 停止Spark上下文
        sc.stop();
    }
}
