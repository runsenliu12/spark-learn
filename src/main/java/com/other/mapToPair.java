package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.logging.Logger;

public class mapToPair {

    private static final Logger LOGGER = Logger.getLogger(mapToPair.class.getName());

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("mapToPair").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputRDD = sc.textFile("data/sales_data.txt");

        JavaPairRDD<String, Double> stringDoubleJavaPairRDD = inputRDD.mapToPair(
                line -> {
                    String[] words = line.split(",", -1);
                    String produce = words[0];
                    Double sale = Double.parseDouble(words[1]);
                    return new Tuple2<>(produce, sale);
                }
        );

        JavaPairRDD<String, Double> stringDoubleJavaPairRDD1 = stringDoubleJavaPairRDD.reduceByKey(Double::sum);


        // 找出销售额最高的产品
        Tuple2<String, Double> topSellingProduct = stringDoubleJavaPairRDD1
                .reduce((tuple1, tuple2) -> tuple1._2() > tuple2._2() ? tuple1 : tuple2);

        System.out.println("销售额最高的产品: " + topSellingProduct._1() + " 销售额: " + topSellingProduct._2());



        JavaPairRDD<String, Double> stringDoubleJavaPairRDD2 = stringDoubleJavaPairRDD1.
            groupByKey().mapValues(sales -> {
                double sum = 0.0;
                int count = 0;
                for (Double sale : sales) {
                    sum += sale;
                    count++;
                }
                return sum / count;
        });

        // 打印日志
        stringDoubleJavaPairRDD2.foreach(tuple -> {
            LOGGER.info("产品: " + tuple._1() + " 平均销售额: " + tuple._2());
        });


        stringDoubleJavaPairRDD2.foreach(tuple -> System.out.println("产品: " + tuple._1() + " 平均销售额: " + tuple._2()));

        sc.close();
    }
}
