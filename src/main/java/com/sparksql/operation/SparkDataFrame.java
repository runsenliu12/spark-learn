package com.sparksql.operation;

import org.apache.spark.sql.*;

import org.apache.spark.sql.expressions.*;

import static org.apache.spark.sql.functions.*;


public class SparkDataFrame {

    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkDataFrameTutorial")
                .master("local[*]")
                .getOrCreate();

        // 读取包含日期的 CSV 文件
        String csvPath = SparkDataFrame.class.getClassLoader().getResource("sparksqldata/sales.csv").getPath();

        Dataset<Row> df = spark.read()
                .option("header", "true") // 指定文件中包含列名
                .option("inferSchema", "true") // 自动推断列的数据类型
                .csv(csvPath);

        // 显示 DataFrame 的前几行数据
        df.show();

        // 数据透视：根据某些列的值创建数据透视表格
        Dataset<Row> pivotDF = df.groupBy("category")
                .pivot("product")
                .sum("sales");

        pivotDF.show();

        // 拆分列：将包含多个值的列拆分成多个列
        df = df.withColumn("name_parts", split(col("full_name"), " "));
        df.show();

        // 合并列：将多个列的值合并成一个新的列
        df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")));
        df.show();

        // 聚合窗口：在窗口函数中执行更高级的聚合操作，如累积总和、移动平均值等
        // 在这个例子中，我们将数据按 'category' 列分区（分组），然后按 'date' 列排序。
        WindowSpec windowSpec = Window.partitionBy("category").orderBy("date");

        // 使用窗口函数（over）计算每个分组内 'sales' 列的累积总和，并将结果存储在新列 'total_sales' 中。
        Dataset<Row> aggregatedDF = df.withColumn("total_sales", sum("sales").over(windowSpec));

        // 显示包含累积总和的 DataFrame。
        aggregatedDF.show();

        // 停止 SparkSession
        spark.stop();
    }
}
