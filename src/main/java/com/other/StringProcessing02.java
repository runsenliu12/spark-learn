package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class StringProcessing02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("StringProcessing")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 获取资源文件路径
        String resourcePath = StringProcessing02.class.getClassLoader().getResource("data.txt").getPath();

        // 读取数据源
        JavaRDD<String> lines = sc.textFile(resourcePath);

        // 转换和操作
        JavaDoubleRDD allValues = lines.flatMapToDouble(line -> {
            String[] values = line.split(",");
            double[] doubleValues = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                doubleValues[i] = Double.parseDouble(values[i].trim());
            }
            return Arrays.stream(doubleValues).iterator();
        });

        // 计算平均值
        double average = allValues.mean();
        System.out.println("Average: " + average);

        // 关闭 SparkContext
        sc.stop();
    }
}
