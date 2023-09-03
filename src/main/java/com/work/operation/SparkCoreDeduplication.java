package com.work.operation;

import com.work.entity.UserInteraction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SparkCoreDeduplication {
    public static void main(String[] args) {
        // 创建一个 SparkConf 和 JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("SparkCoreDeduplication").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 创建一个 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkCoreDeduplication")
                .config(conf)
                .getOrCreate();

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

        // 创建DataFrame
        Dataset<Row> df = spark.createDataFrame(rdd, schema);

        // 使用 Spark Core 进行去重和筛选
        JavaRDD<Row> deduplicatedRDD = df.javaRDD().groupBy(row -> row.getString(0) + row.getString(1) + row.getString(2))
                // 第一步：使用groupByKey将数据分组，键为用户ID、品牌和类别的组合
                .flatMapValues(rows -> {
                    // 第二步：对每个分组的数据进行处理

                    // 创建一个列表来存储排序后的行数据
                    List<Row> sortedRows = new ArrayList<>();

                    // 获取迭代器以遍历每个分组的行
                    Iterator<Row> iterator = rows.iterator();
                    while (iterator.hasNext()) {
                        // 将行数据添加到排序列表中
                        sortedRows.add(iterator.next());
                    }

                    // 第三步：对行数据进行排序，按时间戳升序排列
                    sortedRows.sort((row1, row2) -> Long.compare(row1.getTimestamp(3).getTime(), row2.getTimestamp(3).getTime()));

                    // 返回排序后的行数据的迭代器
                    return sortedRows.iterator();
                })
                // 第四步：仅保留排序后的数据，即时间戳最早的数据
                .values();


        // 转换为 DataFrame
        Dataset<Row> deduplicatedDF = spark.createDataFrame(deduplicatedRDD, schema);

        deduplicatedDF.show();


        df.write().csv("src/main/resources/workdata/deduplicatedDF");
        deduplicatedDF.write().csv("src/main/resources/workdata/deduplicatedDF");

        // 关闭 SparkSession 和 JavaSparkContext
        spark.stop();
        jsc.stop();
    }
}
