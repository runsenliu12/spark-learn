package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

public class ColumnCoverage {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ColumnCoverage").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建一个包含CSV数据的List
        List<String> csvData = Arrays.asList(
                "1,2,3,4",
                "5,,7,8",
                "9,10,,12",
                "13,14,15,16"
        );

        // 使用parallelize方法创建RDD
        JavaRDD<String> data = sc.parallelize(csvData);

        // 分割每一行，以逗号作为分隔符
        JavaRDD<String[]> columns = data.map(line -> line.split(","));

        // 计算每列的缺失值数量
        JavaRDD<Long> missingValuesCount = columns.map(column -> {
            long count = 0;
            for (String value : column) {
                if (value.isEmpty()) {
                    count++;
                }
            }
            return count;
        });

        // 计算每列的覆盖率并保留两位小数
        long totalRows = data.count();
        JavaRDD<Double> coverage = missingValuesCount.map(count -> {
            double coveragePercentage = 100.0 - (count * 100.0 / totalRows);
            return Double.parseDouble(new DecimalFormat("0.00").format(coveragePercentage));
        });

        // 打印每列的覆盖率
        coverage.collect().forEach(System.out::println);

        sc.stop();
    }
}
