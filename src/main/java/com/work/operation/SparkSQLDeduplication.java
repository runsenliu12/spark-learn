package com.work.operation;

import com.work.entity.UserInteraction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

public class SparkSQLDeduplication {
    public static void main(String[] args) {
        // 创建一个 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLDeduplication")
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

        // 创建一个 Spark SQL 临时视图
        df.createOrReplaceTempView("user_interactions");

        // 使用 Spark SQL 进行去重和筛选
        String sql = "SELECT * FROM (" +
                "SELECT *, " +
                "       LAG(time) OVER(PARTITION BY userID, brand, category ORDER BY time) AS prev_time, " +
                "       LAG(timestamp) OVER(PARTITION BY userID, brand, category ORDER BY time) AS prev_timestamp " +
                "FROM user_interactions" +
                ") temp " +
                "WHERE prev_time IS NULL OR UNIX_TIMESTAMP(time) - UNIX_TIMESTAMP(prev_time) >= 3600";

        Dataset<Row> deduplicatedDF = spark.sql(sql).drop("prev_time", "prev_timestamp");

        deduplicatedDF.show();


        spark.stop();
    }
}
