package com.other;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkMapPartitionsExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkMapPartitionsExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建包含整数的RDD
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3); // 3个分区

        // 使用mapPartitions操作计算每个分区的平均值
        JavaRDD<Double> avgPerPartition = numbers.mapPartitions(iterator -> {
            double sum = 0.0;
            int count = 0;
            while (iterator.hasNext()) {
                sum += iterator.next();
                count++;
            }
            List<Double> result = new ArrayList<>();
            if (count > 0) {
                result.add(sum / count);
            }
            return result.iterator();
        });

        // 打印结果
        System.out.println("Numbers: " + numbers.collect());
        System.out.println("Avg per Partition: " + avgPerPartition.collect());

        sc.stop();
    }
}
