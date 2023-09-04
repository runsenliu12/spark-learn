package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SparkMapExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkMapExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 随机生成100个学生数据
        List<Student> studentsData = generateRandomStudents(100);
        JavaRDD<Student> studentsRDD = sc.parallelize(studentsData);

        // 将每个学生的分数加上10分
        JavaRDD<Student> updatedScoresRDD = studentsRDD.map(student -> {
            student.setScore(student.getScore() + 10);
            return student;
        });

        // 计算每个学生的平均分
        double totalScore = updatedScoresRDD.mapToDouble(Student::getScore).sum();
        long totalStudents = updatedScoresRDD.count();
        double averageScore = totalScore / totalStudents;

        // 根据平均分将学生分为不同等级
        JavaRDD<String> studentGradesRDD = updatedScoresRDD.map(student -> {
            if (student.getScore() >= averageScore + 10) {
                return "优秀";
            } else if (student.getScore() >= averageScore - 10) {
                return "良好";
            } else {
                return "及格";
            }
        });

        // 统计每个等级的学生数量
        long excellentCount = studentGradesRDD.filter(grade -> grade.equals("优秀")).count();
        long goodCount = studentGradesRDD.filter(grade -> grade.equals("良好")).count();
        long passCount = studentGradesRDD.filter(grade -> grade.equals("及格")).count();

        System.out.println("优秀学生数量: " + excellentCount);
        System.out.println("良好学生数量: " + goodCount);
        System.out.println("及格学生数量: " + passCount);

        sc.stop();
        sc.close();
    }

    // 随机生成指定数量的学生数据
    private static List<Student> generateRandomStudents(int count) {
        List<Student> students = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            String name = "Student" + i;
            int age = random.nextInt(10) + 18; // 年龄在18到27之间随机生成
            double score = random.nextDouble() * 100; // 分数在0到100之间随机生成
            students.add(new Student(name, age, score));
        }

        return students;
    }

    // 学生类
    static class Student implements Serializable {
        private String name;
        private int age;
        private double score;

        public Student(String name, int age, double score) {
            this.name = name;
            this.age = age;
            this.score = score;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }
    }
}
