package com.numbuzz.buzzintake.model;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Controller {
    private static List<OffsetDateTime> reqTimes;

    private static OffsetDateTime roundDownTo15Minutes(OffsetDateTime dt) {
        int minutes = dt.getMinute();
        int minutesToSubtract = minutes % 15;
        return dt
                .minusMinutes(minutesToSubtract)
                .withSecond(0)
                .withNano(0);
    }
    private static OffsetDateTime roundUpTo15Minutes(OffsetDateTime dt) {
        int minutes = dt.getMinute();
        int minutesToAdd = 15 - minutes % 15;
        return dt
                .plusMinutes(minutesToAdd)
                .withSecond(0)
                .withNano(0);
    }
    public Controller(String template, String start, String end) {

        OffsetDateTime startDt = OffsetDateTime.parse(start);
        OffsetDateTime endDt = OffsetDateTime.parse(end);

        OffsetDateTime timeCursor = roundUpTo15Minutes(startDt);
        reqTimes = new ArrayList<>();
        while(timeCursor.isBefore(endDt)) {
            reqTimes.add(timeCursor);
            timeCursor = timeCursor.plusMinutes(15);
        }
    }
    public Controller(String template) {
        reqTimes = List.of(new OffsetDateTime[]{roundDownTo15Minutes(OffsetDateTime.now())});
    }

    public Controller(String template, String dtStr) {
        OffsetDateTime dt = OffsetDateTime.parse(dtStr);
        reqTimes = List.of(roundDownTo15Minutes(dt));
    }

    public String[] getDtStrings() {
        String[] result = new String[reqTimes.size()];
        for (int i=0; i < reqTimes.size(); i++) {
            result[i] = reqTimes.get(i).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        }
        return result;
    }
}
