package com.sparksql.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SparkSQLTest {

    public static void main(String[] args) {
        // 创建Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("SparkSQLTest")
                .setMaster("local[*]"); // 在本地模式下运行

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLTest")
                .getOrCreate();


        // 创建一个示例数据集合
        List<Row> data = Arrays.asList(
                RowFactory.create("John", 25),
                RowFactory.create("Jane", 30),
                RowFactory.create("Bob", 22),
                RowFactory.create("Alice", 28)
        );

        // 定义DataFrame的结构
        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);

        // 创建DataFrame
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // 注册DataFrame为一张临时表
        df.createOrReplaceTempView("people");

        // 执行Spark SQL查询
        Dataset<Row> result = spark.sql("SELECT name, age FROM people WHERE age >= 18");

        // 打印查询结果
        result.show();

        // 停止SparkSession和JavaSparkContext
        spark.stop();
        sc.stop();
    }
}
