package com.sparkconf.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MobileTrafficLog implements Serializable {
    private String mobileNumber;
    private long upTraffic;
    private long downTraffic;

    public MobileTrafficLog(String mobileNumber, long upTraffic, long downTraffic) {
        this.mobileNumber = mobileNumber;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public String getMobileNumber() {
        return mobileNumber;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    @Override
    public String toString() {
        return mobileNumber + " " + upTraffic + " " + downTraffic;
    }

    // 将随机数据生成方法写在类中
    public static List<MobileTrafficLog> generateRandomTrafficData(int count) {
        List<MobileTrafficLog> trafficLogs = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            String mobileNumber = "138" + String.format("%08d", random.nextInt(100000000));
            long upTraffic = random.nextLong() % 1000;
            long downTraffic = random.nextLong() % 1000;
            trafficLogs.add(new MobileTrafficLog(mobileNumber, upTraffic, downTraffic));
        }

        return trafficLogs;
    }


}
