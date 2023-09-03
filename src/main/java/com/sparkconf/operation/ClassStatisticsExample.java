package com.sparkconf.operation;

import com.sparkconf.entity.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

import static com.sparkconf.entity.Student.generateRandomStudentData;


public class ClassStatisticsExample {

    public static void main(String[] args) {
        // 创建 Spark 配置
        SparkConf conf = new SparkConf().setAppName("ClassStatisticsExample").setMaster("local");

        // 创建 JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建一个包含随机学生数据的数据集
//        studentsRDD.collect().forEach(student -> System.out.println(student.toString()));

        List<Student> studentsData = generateRandomStudentData(1000);

        JavaRDD<Student> studentsRDD = sc.parallelize(studentsData);


        SparkSession spark = SparkSession.builder()
                .appName("ShowTop10StudentsExample")
                .getOrCreate();

        // 将学生数据转换为 DataFrame
        Dataset<Row> studentsDF = spark.createDataFrame(studentsData, Student.class);

        // 展示前10个学生数据
        studentsDF.show(10);




        // 关闭 SparkContext
        sc.stop();
        sc.close();

    }

}
