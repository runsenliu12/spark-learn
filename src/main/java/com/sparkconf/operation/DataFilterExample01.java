package com.sparkconf.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import  org.apache.spark.api.java.JavaRDD;
import java.util.Arrays;

public class DataFilterExample01 {
    public static void main(String[] args) {

        // 创建Spark配置
        SparkConf conf = new SparkConf().setAppName("DataFilterExample01").setMaster("local[*]");

        // 创建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);


        // 创建一个包含整数的RDD示例数据集
        JavaRDD<Integer> data = sc.parallelize(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,12 ,24)
        );

        // 使用filter操作筛选出偶数
        JavaRDD<Integer> evenNumbers = data.filter(num -> num % 2 == 0);

        System.out.println("筛选出的偶数: " + evenNumbers.collect());
        evenNumbers.foreach(num -> System.out.println(num));

        // 使用reduce操作求和
        int sum = evenNumbers.reduce((x, y) -> x + y);

        // 打印求和结果
        System.out.println("偶数的和为: " + sum);

        // 使用 filter 操作筛选出偶数，然后使用 reduce 操作求和
        int sum1 = data.filter(num -> num % 2 == 0).reduce((x, y) -> x + y);


        System.out.println("偶数的和为: " + sum1);

        // 使用一行代码完成筛选、加1和相乘的操作
//        int result = data.filter(num -> num % 2 == 0 && num % 3 == 0).map(num -> num + 1).reduce((x, y) -> x * y);


        int result = data.filter(num -> num % 2 == 0 && num % 3 == 0).map(num ->num + 1).reduce((x,y) -> x * y);



        System.out.println("是3和2的倍数加1然后相乘的结果为: " + result);

        // 关闭Spark上下文
        sc.close();


//        SparkConf conf = new SparkConf().setAppName("DataFilterExample01").setMaster("local[*]");
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//        JavaRDD<Integer> data = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 24));
//
//
//        Integer result = data.filter(x -> x % 2 == 0 && x % 3 == 0).map(x -> x + 1).reduce((x, y) -> x * y);
//
////        System.out.println("是3和2的倍数加1然后相乘的结果为: " + result);


    }
}
