package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.text.DecimalFormat;

public class CalculateMeanForEachLineWithSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("CalculateMeanForEachLineWithSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 获取资源文件路径
        String resourcePath = CalculateMeanForEachLineWithSpark.class.getClassLoader().getResource("sparkconfdata/data.txt").getPath();

        // 读取数据源
        JavaRDD<String> lines = sc.textFile(resourcePath);

        // 创建DecimalFormat来保留两位小数
        DecimalFormat df = new DecimalFormat("#0.00");

        // 转换和操作
        JavaRDD<String> meanPerLine = lines.map(line -> {
            String[] values = line.split(",");
            double sum = 0.0;
            int count = 0;
            for (String value : values) {
                double doubleValue = Double.parseDouble(value.trim());
                sum += doubleValue;
                count++;
            }
            if (count > 0) {
                double mean = sum / count; // 计算平均值
                return df.format(mean); // 格式化为保留两位小数的字符串
            } else {
                return "0.00"; // 避免除以零并返回0.00
            }
        });

        // 打印结果
        meanPerLine.foreach(mean -> System.out.println("平均值: " + mean));

        // 关闭 SparkContext
        sc.stop();
    }
}
