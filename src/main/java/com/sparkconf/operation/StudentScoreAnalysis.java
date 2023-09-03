package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DecimalFormat;
import java.util.Arrays;

public class StudentScoreAnalysis {
    public static void main(String[] args) {
        // 创建Spark配置
        SparkConf conf = new SparkConf().setAppName("StudentScoreAnalysis").setMaster("local");

        // 创建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建包含学生姓名、性别和科目的键值对RDD
        JavaPairRDD<Tuple3<String, String, String>, Double> studentScoresRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Tuple3<>("Alice", "Female", "Math"), 90.0),
                new Tuple2<>(new Tuple3<>("Bob", "Male", "Math"), 85.0),
                new Tuple2<>(new Tuple3<>("Alice", "Female", "Math"), 92.0),
                new Tuple2<>(new Tuple3<>("Bob", "Male", "Math"), 88.0),
                new Tuple2<>(new Tuple3<>("Alice", "Female", "Science"), 87.0)
        ));

        // 使用groupByKey操作按学生性别和科目分组
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Double>> groupedRDD = studentScoresRDD.groupByKey();

        // 计算每个学生的平均分数
        JavaPairRDD<Tuple3<String, String, String>, Double> averageScoresRDD = groupedRDD.mapToPair(tuple -> {
            Tuple3<String, String, String> studentInfo = tuple._1(); // 获取学生性别和科目
            Iterable<Double> scores = tuple._2(); // 获取学生分数的可迭代集合
            double sum = 0.0;
            int count = 0;
            for (Double score : scores) {
                sum += score;
                count++;
            }
            double average = sum / count;

            // 使用DecimalFormat来保留两位小数
            DecimalFormat df = new DecimalFormat("#.00");
            double roundedAverage = Double.parseDouble(df.format(average));

            // 返回包含学生性别、科目和平均分数的元组
            return new Tuple2<>(studentInfo, roundedAverage);
        });

        // 打印结果
        averageScoresRDD.foreach(tuple -> {
            Tuple3<String, String, String> studentInfo = tuple._1();
            double averageScore = tuple._2();
            System.out.println("Gender: " + studentInfo._1() + ", Subject: " + studentInfo._2() + ", Average Score: " + averageScore);
        });

        // 停止Spark上下文
        sc.stop();
    }
}
