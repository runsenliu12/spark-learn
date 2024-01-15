package com.sparkconf.operation;

import com.sparkconf.entity.CustomData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CustomDataProcessing {
    public static void main(String[] args) {
        // Initialize Spark
        SparkConf conf = new SparkConf().setAppName("CustomDataProcessing").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create sample CustomData instances
        CustomData data1 = CustomData.builder().udid("udid1").brands(Arrays.asList("BrandA", "BrandB")).categorys(Arrays.asList("CategoryX")).build();
        CustomData data2 = CustomData.builder().udid("udid2").brands(Arrays.asList("BrandA", "BrandC")).categorys(Arrays.asList("CategoryY")).build();
        CustomData data3 = CustomData.builder().udid("udid1").brands(Arrays.asList("BrandB", "BrandD")).categorys(Arrays.asList("CategoryX", "CategoryZ")).build();

        List<CustomData> customDataList = Arrays.asList(data1, data2, data3);

        // Create RDD from the list of CustomData
        JavaRDD<CustomData> customDataRDD = sc.parallelize(customDataList);

        // Perform aggregation based on udid
        JavaPairRDD<String, CustomData> resultRDD = customDataRDD
                .mapToPair(customData -> new Tuple2<>(customData.getUdid(), customData))
                .reduceByKey((d1, d2) -> {
                    List<String> mergedBrands = Stream.concat(d1.getBrands().stream(), d2.getBrands().stream())
                            .distinct()
                            .collect(Collectors.toList());

                    List<String> mergedCategorys = Stream.concat(d1.getCategorys().stream(), d2.getCategorys().stream())
                            .distinct()
                            .collect(Collectors.toList());

                    return CustomData.builder()
                            .udid(d1.getUdid())
                            .brands(mergedBrands)
                            .categorys(mergedCategorys)
                            .build();
                });
        // Collect and print the result
        List<Tuple2<String, CustomData>> result = resultRDD.collect();
        result.forEach(System.out::println);

        // Stop Spark
        sc.stop();
    }
}
