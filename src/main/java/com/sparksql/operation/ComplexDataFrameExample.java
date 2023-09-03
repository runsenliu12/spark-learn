package com.sparksql.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.Window;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class ComplexDataFrameExample {
    public static void main(String[] args) {
        // 创建Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("ComplexDataFrameExample")
                .setMaster("local[*]"); // 在本地模式下运行

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("ComplexDataFrameExample")
                .getOrCreate();

        // 创建一个示例数据集合
        List<Row> data = Arrays.asList(
                RowFactory.create("John", "Sales", 25, 5000.0),
                RowFactory.create("Jane", "Marketing", 30, 6000.0),
                RowFactory.create("Bob", "Sales", 22, 4800.0),
                RowFactory.create("Alice", "Marketing", 28, 5500.0)
        );


        // 定义DataFrame的结构
        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("department", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("salary", DataTypes.DoubleType, false, Metadata.empty()),

        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // 显示DataFrame的内容
        df.show();


        // 使用 foreach 打印列名
        Arrays.stream(df.columns()).forEach(System.out::println);

        // 将所有列名拼接成一个字符串并打印
        System.out.println( String.join(", ", df.columns()));


        // 数据过滤需求
        Dataset<Row> filteredDF = df.filter(col("age").gt(25));
        filteredDF.show();

        // 数据聚合需求
        Dataset<Row> aggregatedDF = df.groupBy("department")
                .agg(avg("salary").as("avg_salary"), sum("salary").as("total_salary"),
                        max("salary").as("max_salary"), min("salary").as("min_salary"));
        aggregatedDF.show();

        // 连接数据需求
        // 连接数据需求（根据员工姓名连接）
        String path = ComplexDataFrameExample.class.getClassLoader().getResource("sparksqldata/department.csv").getPath();
         // 读取数据并将第一行作为列名
        Dataset<Row> departmentData = spark.read()
                .option("header", "true")  // 设置header选项为true，将第一行作为列名
                .csv(path);

        System.out.println(Arrays.toString(departmentData.columns()));


// 连接操作，根据员工姓名连接
        Dataset<Row> joinedDF = df.join(departmentData, "name");
        joinedDF.show();


        // 排序需求
        Dataset<Row> sortedDF = df.orderBy(col("salary").desc());
        sortedDF.show();

        // 计算排名需求
        WindowSpec windowSpec = Window.partitionBy("department").orderBy(col("salary").desc());
        Dataset<Row> rankedDF = df.withColumn("rank", rank().over(windowSpec));
        rankedDF.show();

        // 停止SparkSession和JavaSparkContext
        spark.stop();
        sc.stop();
    }
}
