package com.sparkconf.operation;

import com.sparkconf.entity.MobileTrafficLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.net.URL;
import java.util.List;

public class GenerateRandomTrafficData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GenerateRandomTrafficData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 随机生成手机用户流量日志数据集
        List<MobileTrafficLog> trafficLogs = MobileTrafficLog.generateRandomTrafficData(100);

        // 将数据集转换为RDD
        JavaRDD<MobileTrafficLog> trafficDataRDD = sc.parallelize(trafficLogs);

        // 将数据集保存到文件
        trafficDataRDD.map(log -> log.toString()).saveAsTextFile("output");
// 读取生成的随机数据文件
        JavaRDD<String> inputLogData = sc.textFile("output");

        // 使用mapToPair将每行日志数据解析为键值对 (手机号, (上行流量, 下行流量))
        JavaPairRDD<String, Tuple2<Long, Long>> userTrafficData = inputLogData.mapToPair(line -> {
            String[] parts = line.split("\\s+");
            String mobileNumber = parts[0];
            long upTraffic = Long.parseLong(parts[1]);
            long downTraffic = Long.parseLong(parts[2]);
            return new Tuple2<>(mobileNumber, new Tuple2<>(upTraffic, downTraffic));
        });


        // 使用reduceByKey对每个手机号的流量数据进行累加
        JavaPairRDD<String, Tuple2<Long, Long>> aggregatedTrafficData = userTrafficData.reduceByKey((t1, t2) ->
                new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)
        );

        // 输出结果
        aggregatedTrafficData.foreach(data -> {
            String mobileNumber = data._1;
            long upTrafficTotal = data._2._1;
            long downTrafficTotal = data._2._2;
            System.out.println(mobileNumber + "，上行流量：" + upTrafficTotal + "，下行流量：" + downTrafficTotal);
        });

        sc.stop();
        sc.close();
    }
}
