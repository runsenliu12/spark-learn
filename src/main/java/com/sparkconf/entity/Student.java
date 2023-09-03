package com.sparkconf.entity;


import java.io.Serializable;
import java.util.*;


import java.util.Map;

public class Student implements Serializable {
    private String name;
    private String gender;
    private String className;
    private Map<String, Integer> subjectScores;

    public Student(String name, String gender, String className, Map<String, Integer> subjectScores) {
        this.name = name;
        this.gender = gender;
        this.className = className;
        this.subjectScores = subjectScores;
    }

    public String getName() {
        return name;
    }

    public String getGender() {
        return gender;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, Integer> getSubjectScores() {
        return subjectScores;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", className='" + className + '\'' +
                ", subjectScores=" + subjectScores +
                '}';
    }


    // 辅助函数：生成随机学生数据
    public static List<Student> generateRandomStudentData(int numStudents) {
        List<Student> students = new ArrayList<>();
        Random rand = new Random();

        String[] genders = {"Male", "Female"};
        String[] classNames = {"Class A", "Class B", "Class C"};
        String[] subjects = {"Math", "Physics", "Chemistry", "Biology", "History"};

        for (int i = 0; i < numStudents; i++) {
            String name = "Student" + (i + 1);
            String gender = genders[rand.nextInt(genders.length)];
            String className = classNames[rand.nextInt(classNames.length)];
            Map<String, Integer> subjectScores = new HashMap<>();
            for (String subject : subjects) {
                subjectScores.put(subject, rand.nextInt(101)); // Random score between 0 and 100
            }
            students.add(new Student(name, gender, className, subjectScores));
        }

        return students;

    }
}
