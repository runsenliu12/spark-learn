package com.work.operation;

import com.work.entity.UserInteraction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public class SparkDuplicateRemovalWithEarliestTimestamp {
    public static void main(String[] args) {
        // 创建Spark配置和SparkContext
        SparkConf conf = new SparkConf().setAppName("SparkDuplicateRemovalWithEarliestTimestamp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 生成随机数据
        List<UserInteraction> userInteractions = UserInteraction.generateRandomInteractions(100);

        // 将数据转换为RDD
        JavaRDD<UserInteraction> rdd = sc.parallelize(userInteractions);

        // 将数据转换为键值对，其中键是(userID, brand, category)，值是UserInteraction对象
        JavaPairRDD<Tuple2<String, String>, UserInteraction> keyedData = rdd
                .mapToPair(interaction -> new Tuple2<>(new Tuple2<>(interaction.getUserID(), interaction.getBrand()), interaction));

        // 使用groupByKey进行分组，然后使用mapValues筛选出最早的数据
        JavaPairRDD<Tuple2<String, String>, UserInteraction> filteredData = keyedData
                .groupByKey()
                .mapValues(new org.apache.spark.api.java.function.Function<Iterable<UserInteraction>, UserInteraction>() {
                    @Override
                    public UserInteraction call(Iterable<UserInteraction> interactions) throws Exception {
                        List<UserInteraction> interactionList = new ArrayList<>();
                        for (UserInteraction interaction : interactions) {
                            interactionList.add(interaction);
                        }
                        // 排序并返回最早的数据
                        interactionList.sort((o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp()));
                        return interactionList.get(0);
                    }
                });

        // 打印结果
        filteredData.values().foreach(System.out::println);

        // 停止SparkContext
        sc.stop();
    }


    private static void saveToHDFS(JavaRDD<String> data, String outputPath) {
        try {
            // 创建HDFS配置
            Configuration hadoopConf = new Configuration();
            // 替换成你的HDFS路径
            Path hdfsPath = new Path(outputPath);
            FileSystem fs = FileSystem.get(hadoopConf);

            // 删除已存在的输出目录
            if (fs.exists(hdfsPath)) {
                fs.delete(hdfsPath, true);
            }

            // 将数据写入HDFS
            data.saveAsTextFile(outputPath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
