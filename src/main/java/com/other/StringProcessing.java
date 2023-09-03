package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class StringProcessing {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("StringProcessing")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 获取资源文件路径
        String resourcePath = StringProcessing.class.getClassLoader().getResource("data.txt").getPath();

        // 读取数据源
        JavaRDD<String> lines = sc.textFile(resourcePath);

        // 转换和操作
        JavaRDD<Integer> sumPerLine = lines.map(line -> {
            String[] values = line.split(",");
            int sum = 0;
            for (String value : values) {
                sum += Integer.parseInt(value.trim());
            }
            return sum;
        });

        // 打印结果
        sumPerLine.foreach(sum -> System.out.println("Sum: " + sum));

        // 关闭 SparkContext
        sc.stop();
    }
}
