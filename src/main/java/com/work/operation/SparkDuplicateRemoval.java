package com.work.operation;

import com.work.entity.UserInteraction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple2;


import java.util.List;

public class SparkDuplicateRemoval {
    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkDeduplication")  // 设置应用程序名称
                .master("local[*]")  // 在本地运行Spark
                .getOrCreate();

        // 创建 JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 生成随机的 UserInteraction 数据
        List<UserInteraction> userInteractions = UserInteraction.generateRandomInteractions(100);

        // 转换为 RDD，每个元素是一个元组 (userID, brand, category, timestamp)
        JavaRDD<Tuple4<String, String, String, Long>> rdd = sc.parallelize(userInteractions)
                .map(userInteraction -> new Tuple4<>(
                        userInteraction.getUserID(),
                        userInteraction.getBrand(),
                        userInteraction.getCategory(),
                        userInteraction.getTimestamp()
                ));

        // 去重数据，当时间戳相差不足一个小时时，保留最早的数据
        JavaRDD<Tuple4<String, String, String, Long>> deduplicatedRDD = rdd
                .mapToPair(tuple -> new Tuple2<>(new Tuple3<>(tuple._1(), tuple._2(), tuple._3()), tuple))
                .reduceByKey((tuple1, tuple2) -> {
                    long timestamp1 = tuple1._4();
                    long timestamp2 = tuple2._4();
                    if (Math.abs(timestamp2 - timestamp1) >= 3600000) {
                        // 如果时间戳相差超过一个小时，保留最早的数据
                        return timestamp1 < timestamp2 ? tuple1 : tuple2;
                    } else {
                        // 如果时间戳相差不足一个小时，保留最早的数据
                        return timestamp1 < timestamp2 ? tuple1 : tuple2;
                    }
                })
                .map(Tuple2::_2);

        // 将结果转换回 UserInteraction 对象并打印
        List<UserInteraction> deduplicatedInteractions = deduplicatedRDD.map(tuple -> {
            UserInteraction userInteraction = new UserInteraction();
            userInteraction.setUserID(tuple._1());
            userInteraction.setBrand(tuple._2());
            userInteraction.setCategory(tuple._3());
            userInteraction.setTimestamp(tuple._4());
            return userInteraction;
        }).collect();

        deduplicatedInteractions.forEach(System.out::println);

        // 停止 Spark
        spark.stop();
    }
}
