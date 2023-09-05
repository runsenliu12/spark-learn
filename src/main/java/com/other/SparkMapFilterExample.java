package com.other;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.Arrays;

public class SparkMapFilterExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkMapFilterExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建包含整数的RDD
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));

        // 使用map操作将每个数加倍，然后使用filter操作筛选出偶数
        JavaRDD<Integer> doubledAndEven = numbers.map(x -> x * 2).filter(x -> x % 2 == 0);

        // 打印结果
        System.out.println("Numbers: " + numbers.collect());
        System.out.println("Doubled and Even Numbers: " + doubledAndEven.collect());

        sc.stop();
    }
}
