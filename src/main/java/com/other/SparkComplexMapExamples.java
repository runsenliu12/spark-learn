package com.other;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;

public class SparkComplexMapExamples {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkComplexMapExamples").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建包含电影评分数据的RDD
        JavaRDD<String> lines = sc.parallelize(Arrays.asList(
                "1,101,4.0",
                "2,102,3.5",
                "3,103,5.0",
                "1,104,3.0",
                "2,105,4.5",
                "3,106,2.5"
        ));

        // 使用map操作将每行的数据解析为三元组（电影ID，用户ID，评分）
        JavaRDD<Tuple3<Integer, Integer, Double>> ratings = lines.map(line -> {
            String[] parts = line.split(",");
            int movieId = Integer.parseInt(parts[0]);
            int userId = Integer.parseInt(parts[1]);
            double rating = Double.parseDouble(parts[2]);
            return new Tuple3<>(movieId, userId, rating);
        });

        // 使用mapValues操作计算每个电影的平均评分
        JavaPairRDD<Integer, Double> movieAvgRatings = ratings
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._3()))
                .groupByKey()
                .mapValues(ratingsList -> {
                    double sum = 0.0;
                    int count = 0;
                    for (Double rating : ratingsList) {
                        sum += rating;
                        count++;
                    }
                    return sum / count;
                });


        // 打印结果
        System.out.println("Movie Ratings: " + ratings.collect());
        System.out.println("Movie Average Ratings: " + movieAvgRatings.collect());

        sc.stop();
    }
}
