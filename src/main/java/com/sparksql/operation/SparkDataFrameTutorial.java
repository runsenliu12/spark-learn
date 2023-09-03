package com.sparksql.operation;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;


import static java.util.stream.IntStream.concat;
import static org.apache.spark.sql.functions.*;

public class SparkDataFrameTutorial {

    public static void main(String[] args) {

        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkDataFrameTutorial")
                .master("local[*]")
                .getOrCreate();

        // 读取包含日期的 CSV 文件

        String csvPath = SparkDataFrameTutorial.class.getClassLoader().getResource("sparksqldata/sales.csv").getPath();

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


        // 聚合窗口：在窗口函数中执行更高级的聚合操作，如累积总和、移动平均值等
        WindowSpec windowSpec = Window.partitionBy("category").orderBy("date");
        Dataset<Row> aggregatedDF = df.withColumn("total_sales", sum("sales").over(windowSpec));
        aggregatedDF.show();

        // 停止 SparkSession
        spark.stop();
    }


}
