package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MoreTransformationsExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MoreTransformationsExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbersList);

        // 使用 distinct 转换操作去除重复元素
        JavaRDD<Integer> distinctRDD = numbersRDD.distinct();
        System.out.println("Distinct RDD: " + distinctRDD.collect());

        // 使用 sortBy 转换操作对元素进行排序，默认升序
        JavaRDD<Integer> sortedRDD = numbersRDD.sortBy(num -> num, true, 0);
        System.out.println("Sorted RDD: " + sortedRDD.collect());

        // 使用 groupBy 转换操作根据指定的键对元素进行分组
        JavaPairRDD<String, Iterable<Integer>> groupedRDD = numbersRDD.groupBy(num -> num % 2 == 0 ? "even" : "odd");
        groupedRDD.foreach(tuple -> {
            System.out.print(tuple._1() + ": ");
            for (Integer num : tuple._2()) {
                System.out.print(num + " ");
            }
            System.out.println();
        });

        // 使用 mapPartitions 转换操作对每个分区中的元素进行处理
        JavaRDD<Integer> multipliedRDD = numbersRDD.mapPartitions(iterator -> {
            // 在这里可以对每个分区中的元素进行处理
            // 这里只是示例，实际使用可能需要更复杂的操作
            return iterator;
        });
        System.out.println("Multiplied RDD: " + multipliedRDD.collect());

        // 使用 union 转换操作将两个 RDD 合并
        JavaRDD<Integer> mergedRDD = numbersRDD.union(distinctRDD);
        System.out.println("Merged RDD: " + mergedRDD.collect());

        // 使用 sample 转换操作对 RDD 进行抽样
        JavaRDD<Integer> sampledRDD = numbersRDD.sample(false, 0.5);
        System.out.println("Sampled RDD: " + sampledRDD.collect());

        // 使用 mapValues 转换操作只对键值对中的值进行处理
        JavaPairRDD<Integer, Integer> keyValueRDD = numbersRDD.mapToPair(num -> new Tuple2<>(num, num));
        JavaPairRDD<Integer, Integer> squaredKeyValueRDD = keyValueRDD.mapValues(value -> value * value);
        squaredKeyValueRDD.foreach(tuple -> {
            System.out.println("(" + tuple._1() + ", " + tuple._2() + ")");
        });

        sc.stop();
    }
}
