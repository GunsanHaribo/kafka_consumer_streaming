package org.example;

import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class Main {
    public static void main(String[] args) {
        // 기존 코드...
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        // 수정된 코드
        Instant instant = timestamp.toInstant();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        DayOfWeek dayOfWeek = localDateTime.getDayOfWeek();
        String dayOfWeekString = dayOfWeek.toString();

        System.out.println(dayOfWeekString);
    }
}
