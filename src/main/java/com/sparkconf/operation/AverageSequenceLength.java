package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class AverageSequenceLength {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AverageSequenceLength").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建一个包含二维列表的RDD
        List<List<String>> sequencesList = Arrays.asList(
                Arrays.asList("AGCTAGCTAGCTAGCT", "CGCGCGCGCGCG"),
                Arrays.asList("ATATAT", "GCGCGCGCGCGCGCGCGC"),
                Arrays.asList("ATATAT", "GC")
        );
        JavaRDD<List<String>> sequencesRDD = sc.parallelize(sequencesList);

        // 使用map计算每个子列表的平均长度
        JavaRDD<Double> averageLengthsRDD = sequencesRDD.map(sequenceList -> {
            int totalLength = sequenceList.stream().mapToInt(String::length).sum();
            return (double) totalLength / sequenceList.size();
        });

        // 收集结果并打印
        List<Double> averageLengths = averageLengthsRDD.collect();
        for (int i = 0; i < averageLengths.size(); i++) {
            System.out.println("子列表 " + i + " 的平均长度: " + averageLengths.get(i));
        }

        sc.stop();
    }
}
