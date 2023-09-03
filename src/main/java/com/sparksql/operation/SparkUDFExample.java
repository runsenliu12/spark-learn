package com.sparksql.operation;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class SparkUDFExample {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkUDFExample")
                .master("local[*]")
                .getOrCreate();

        // 创建示例数据
        Dataset<Row> dataset = spark.createDataset(
                Arrays.asList("Hello", "Spark", "User", "Defined", "Function"),
                Encoders.STRING()
        ).toDF("value");

        // 注册UDF
        // 创建一个用户自定义函数（UDF），用于计算字符串的长度，并将其命名为stringLengthUDF。
        UserDefinedFunction stringLengthUDF = udf((String s) -> s.length(), IntegerType);

        // 注册这个UDF，使其可在Spark SQL中使用。给它起了一个名字"stringLengthUDF"。
        spark.udf().register("stringLengthUDF", stringLengthUDF);


        // 使用UDF
        dataset = dataset.withColumn("string_length", expr("stringLengthUDF(value)"));

        // 显示结果
        dataset.show();

        // 使用内置函数 length() 计算字符串长度，并创建新列 不使用UDF
        dataset = dataset.withColumn("string_length2", functions.length(dataset.col("value")));
        // 显示结果
        dataset.show();



        // 停止SparkSession
        spark.stop();
    }
}