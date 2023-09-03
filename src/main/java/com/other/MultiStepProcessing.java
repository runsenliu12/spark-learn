package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MultiStepProcessing {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MultiStepProcessing")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. 创建整数列表
        List<Integer> numbersList = Arrays.asList(5, 12, 8, 18, 3, 15, 20);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbersList);

        // 2. 过滤出大于 10 的数字
        JavaRDD<Integer> filteredNumbers = numbersRDD.filter(num -> num > 10);

        // 3. 计算平均值
        double average = filteredNumbers.mapToDouble(Integer::doubleValue).mean();
        System.out.println("Average of numbers > 10: " + average);

        // 关闭 SparkContext
        sc.stop();
    }
}
