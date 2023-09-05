package com.other;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.Comparator;

public class SparkDataNormalizationExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDataNormalizationExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建包含不同范围的数值的RDD
        JavaRDD<Double> data = sc.parallelize(Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0));

        // 使用map操作将数据标准化到0到1的范围
        double min = data.min(Comparator.naturalOrder());
        double max = data.max(Comparator.naturalOrder());

        JavaRDD<Double> normalizedData = data.map(x -> (x - min) / (max - min));

        // 打印结果
        System.out.println("Original Data: " + data.collect());
        System.out.println("Normalized Data: " + normalizedData.collect());

        sc.stop();
    }
}
