package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class StringProcessing03 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("StringProcessing")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 获取资源文件路径
        String resourcePath = StringProcessing03.class.getClassLoader().getResource("data.txt").getPath();

        // 读取数据源
        JavaRDD<String> lines = sc.textFile(resourcePath);

        // 转换和操作
        JavaRDD<Integer> maxPerLine = lines.map(line -> {
            String[] values = line.split(",");
            int max = Integer.MIN_VALUE;
            for (String value : values) {
                int intValue = Integer.parseInt(value.trim());
                if (intValue > max) {
                    max = intValue;
                }
            }
            return max;
        });

        // 打印结果
        maxPerLine.foreach(max -> System.out.println("Max: " + max));

        // 关闭 SparkContext
        sc.stop();
    }
}
