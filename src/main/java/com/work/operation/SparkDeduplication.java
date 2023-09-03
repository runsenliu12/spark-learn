package com.work.operation;

import com.work.entity.UserInteraction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

public class SparkDeduplication{
    public static void main(String[] args) {
        // 创建一个 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkDeduplication")
                .master("local[*]")
                .getOrCreate();

        // 创建一个 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // 生成随机 UserInteraction 数据
        List<UserInteraction> userInteractions = UserInteraction.generateRandomInteractions(100);

        // 转换为 JavaRDD<Row>
        JavaRDD<Row> rdd = jsc.parallelize(
                userInteractions.stream()
                        .map(ui -> RowFactory.create(ui.getUserID(), ui.getBrand(), ui.getCategory(), Timestamp.valueOf(ui.getTime()), ui.getTimestamp()))
                        .collect(Collectors.toList())
        );

        // 转换为 Spark DataFrame
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("userID", DataTypes.StringType, false),
                DataTypes.createStructField("brand", DataTypes.StringType, false),
                DataTypes.createStructField("category", DataTypes.StringType, false),
                DataTypes.createStructField("time", DataTypes.TimestampType, false),
                DataTypes.createStructField("timestamp", DataTypes.LongType, false)
        });

        Dataset<Row> df = spark.createDataFrame(rdd, schema);



        // 创建一个窗口规范，将数据分区并按照userID、brand和category排序
        WindowSpec windowSpec = Window.partitionBy("userID", "brand", "category")
                .orderBy("time");


        // 使用窗口函数计算每个分区内的前一个时间
        Dataset<Row> deduplicatedDF = df.withColumn("prev_time", functions.lag("time", 1).over(windowSpec))
        // 计算时间差，将时间列转换为长整数并计算差值
                .withColumn("time_diff", functions.col("time").cast("long")
                        .minus(functions.col("prev_time").cast("long")))
        // 筛选时间差大于等于3600秒（一个小时）或前一个时间为空的行
                .filter(functions.col("time_diff").$greater$eq(3600L).or(functions.col("prev_time").isNull()))
        // 选择所需的列进行输出
                .select("userID", "brand", "category", "time", "timestamp");


        deduplicatedDF.show();

        // 获取资源路径
        String resourcePath = SparkDeduplication.class.getClassLoader().getResource("workdata").getPath();
        System.out.println(resourcePath);
        if (resourcePath != null) {

            // 保存原始数据为CSV
            String originalDataPath = resourcePath + "/original_data"; // 原始数据保存路径
            System.out.println(originalDataPath);
            df.write().csv(originalDataPath);

            // 保存去重数据为CSV
            String deduplicatedDataPath = resourcePath + "/deduplicated_data"; // 去重数据保存路径4
            System.out.println(deduplicatedDataPath);

            deduplicatedDF.write().csv(deduplicatedDataPath);
        } else {
            System.err.println("Resource path not found.");
        }

        spark.stop();
    }
}
