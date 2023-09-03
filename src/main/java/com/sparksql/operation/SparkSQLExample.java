package com.sparksql.operation;

import com.sparksql.entity.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

public class SparkSQLExample {

    public static void main(String[] args) {


        // 创建Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("SparkSQLExample")
                .setMaster("local[*]"); // 在本地模式下运行


        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLExample")
                .getOrCreate();


        // 获取资源路径
        String resourcepath = SparkSQLExample.class.getClassLoader().getResource("data/person.txt").getPath();

        System.out.println(resourcepath);


        // 从文本文件中读取数据并转换为Person对象
        JavaRDD<Person> personRDD = sc.textFile(resourcepath)
                .map(line -> {
                    String[] parts = line.split(",");
                    String name = parts[0];
                    int age = Integer.parseInt(parts[1]);
                    return new Person(name, age);
                });

        // 将RDD转换为DataFrame
        Dataset<Row> personDF = spark.createDataFrame(personRDD, Person.class);

        // 注册DataFrame为一张临时表
        personDF.createOrReplaceTempView("people");

        // 执行Spark SQL查询
        Dataset<Row> result = spark.sql("SELECT name, age FROM people WHERE age >= 18");

        // 打印查询结果
        result.show();

        // 停止SparkSession和JavaSparkContext
        spark.stop();
        sc.stop();
    }
}
