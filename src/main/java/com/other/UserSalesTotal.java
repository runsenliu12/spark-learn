package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class UserSalesTotal {
    public static void main(String[] args) {
        // 创建 Spark 配置
        SparkConf conf = new SparkConf().setAppName("UserSalesTotal").setMaster("local[*]");

        // 创建 JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 假设你有一个包含销售记录的RDD，每个记录包括用户ID和销售金额
        JavaRDD<String> salesDataRDD = sc.parallelize(
                Arrays.asList("user1,100", "user2,200", "user1,150", "user3,50", "user2,300")
        );

        // 使用 map 将数据转换为键值对，键是用户ID，值是销售金额
        JavaRDD<Tuple2<String, Double>> userSalesRDD = salesDataRDD.map(line -> {
            String[] parts = line.split(",");
            String userId = parts[0];
            double amount = Double.parseDouble(parts[1]);
            return new Tuple2<>(userId, amount);
        });

        // 使用 map 和 HashMap 计算每个用户的总销售额
        Map<String, Double> userTotalSales = userSalesRDD.mapToPair(pair -> pair)
                .reduceByKey(Double::sum)
                .collectAsMap();

        // 打印每个用户的总销售额
        for (Map.Entry<String, Double> entry : userTotalSales.entrySet()) {
            System.out.println("User: " + entry.getKey() + ", Total Sales: " + entry.getValue());
        }

        // 关闭 SparkContext
        sc.close();
    }
}
