package com.work.entity;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Data
public class UserInteraction implements Serializable {

    // 预定义的品牌、用户ID和类别选项
    private static final String[] BRANDS = {"Brand1", "Brand2"};
    private static final String[] USER_IDS = {"User1", "User2", "User3"};
    private static final String[] CATEGORIES = {"Category1", "Category2"};



    private String userID;
    private String brand;
    private String category;
    private LocalDateTime time; // 使用LocalDateTime表示日期时间
    private long timestamp; // 使用long表示时间戳（毫秒）

    // 日期范围的起始日期和结束日期
    private static final LocalDateTime START_DATE = LocalDateTime.of(2021, 8, 1, 0, 0, 0);
    private static final LocalDateTime END_DATE = LocalDateTime.of(2021, 8, 3, 0, 0, 0);

    // 生成随机UserInteraction对象
    public static UserInteraction generateRandomUserInteraction() {
        UserInteraction userInteraction = new UserInteraction();

        // 从预定义选项中随机选择品牌、用户ID和类别
        Random random = new Random();
        userInteraction.setBrand(BRANDS[random.nextInt(BRANDS.length)]);
        userInteraction.setUserID(USER_IDS[random.nextInt(USER_IDS.length)]);
        userInteraction.setCategory(CATEGORIES[random.nextInt(CATEGORIES.length)]);

        // 生成随机的LocalDateTime对象和时间戳在指定日期范围内
        LocalDateTime randomLocalDateTime = generateRandomLocalDateTime();
        userInteraction.setTime(randomLocalDateTime);
        userInteraction.setTimestamp(
                TimeUnit.SECONDS.toMillis(randomLocalDateTime.toEpochSecond(ZoneOffset.UTC)));

        return userInteraction;
    }

    // 生成随机的LocalDateTime对象在指定日期范围内
    private static LocalDateTime generateRandomLocalDateTime() {
        Random random = new Random();
        long startSeconds = START_DATE.toEpochSecond(ZoneOffset.UTC);
        long endSeconds = END_DATE.toEpochSecond(ZoneOffset.UTC);
        long randomSeconds = startSeconds + (long) (random.nextDouble() * (endSeconds - startSeconds));
        return LocalDateTime.ofEpochSecond(randomSeconds, 0, ZoneOffset.UTC);
    }

    // 生成指定数量的随机UserInteraction对象
    public static List<UserInteraction> generateRandomInteractions(int count) {
        List<UserInteraction> interactions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            UserInteraction randomUserInteraction = UserInteraction.generateRandomUserInteraction();
            interactions.add(randomUserInteraction);
        }
        return interactions;
    }

    public static void main(String[] args) {
        List<UserInteraction> userInteractions = generateRandomInteractions(100);
        userInteractions.forEach(System.out::println);
    }
}
