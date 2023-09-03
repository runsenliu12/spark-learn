package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class CSVToKeyValueRDD {
    public static void main(String[] args) {
        // 创建Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("CSVToKeyValueRDD")
                .setMaster("local[*]"); // 在本地模式下运行，使用所有可用的CPU核心

        // 创建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CSVToKeyValueRDD")
                .getOrCreate();

        String resourcePath = WordCount.class.getClassLoader().getResource("sparkconfdata/sale.csv").getPath();


        // 从CSV文件加载数据
        JavaRDD<String> csvData = sc.textFile(resourcePath);

        // 使用map操作将每行数据转换为键值对形式的RDD
        JavaRDD<Tuple2<String, Integer>> keyValueRDD = csvData.map(line -> {
            String[] fields = line.split(","); // 使用逗号分隔字段
            String key = fields[0]; // 假设CSV文件的第一列是键
            Integer value = Integer.parseInt(fields[1]); // 假设CSV文件的第二列是值
            return new Tuple2<>(key, value);
        });
        System.out.println("--------------原始数据--------------");


        // 打印结果
        keyValueRDD.foreach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));


        // 使用reduceByKey聚合，并计算每个键对应的总和和计数
        JavaPairRDD<String, Tuple2<Integer, Integer>> aggregatedRDD = keyValueRDD
                .mapToPair(pair -> new Tuple2<>(pair._1(), new Tuple2<>(pair._2(), 1)))
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()));

        System.out.println("--------------计算每个键对应的总和和计数--------------");

        // 打印结果
        aggregatedRDD.foreach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));

        // 计算平均值
        JavaPairRDD<String, Double> averageValueRDD = aggregatedRDD.mapValues(
                tuple -> (double) tuple._1() / tuple._2()
        ).sortByKey(false);;

        System.out.println("--------------计算平均值--------------");

        // 打印结果
        averageValueRDD.foreach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));

        // 关闭Spark上下文
        sc.close();

    }
}
