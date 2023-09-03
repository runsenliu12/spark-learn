package com.sparksql.operation;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import java.util.Arrays;

public class LengthUDFExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("LengthUDFExample")
                .master("local[*]")
                .getOrCreate();

        // 定义 UDF
        UDF1<String, Integer> lengthUDF = new UDF1<String, Integer>() {
            @Override
            public Integer call(String input) {
                return input.length();
            }
        };

        // 注册 UDF
        UDFRegistration udfRegistration = spark.udf();
        UserDefinedFunction lengthFunction = udfRegistration.register("lengthUDF", (UserDefinedFunction) lengthUDF);

        // 创建一个包含字符串的 DataFrame
        Dataset<Row> df = spark.createDataFrame(Arrays.asList("hello", "world", "Spark"), String.class);

        // 使用 UDF 计算字符串长度
        df = df.withColumn("length", functions.callUDF("lengthUDF", df.col("value")));

        // 显示结果
        df.show();
    }
}
