package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JoinExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JoinExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建两个包含键值对的 RDD
        List<Tuple2<Integer, String>> data = Arrays.asList(
                new Tuple2<>(1, "apple"),
                new Tuple2<>(2, "banana"),
                new Tuple2<>(3, "orange")
        );



        List<Tuple2<Integer, Integer>> counts = Arrays.asList(
                new Tuple2<>(1, 5),
                new Tuple2<>(2, 3),
                new Tuple2<>(3, 8)
        );
        JavaPairRDD<Integer, String> dataRDD = sc.parallelizePairs(data);
        JavaPairRDD<Integer, Integer> countsRDD = sc.parallelizePairs(counts);

        // 使用 join 操作连接两个 RDD
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinedRDD = dataRDD.join(countsRDD);

        // 打印连接后的结果
        joinedRDD.foreach(tuple -> {
            int key = tuple._1();
            String value1 = tuple._2()._1();
            int value2 = tuple._2()._2();
            System.out.println("(" + key + ", (" + value1 + ", " + value2 + "))");
        });

        sc.stop();
    }
}
