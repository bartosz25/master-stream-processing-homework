package com.becomedataengineer;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.awt.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class WindowOutput {

    private String browser;

    private long count;

    private String windowStart;

    private String windowEnd;

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public static WindowOutput valueOf(TimeWindow timeWindow, String browser, long count) {
        WindowOutput output = new WindowOutput();
        output.setBrowser(browser);
        output.setCount(count);
        output.setWindowStart(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timeWindow.getStart()), ZoneId.of("UTC"))
                        .toString());
        output.setWindowEnd(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timeWindow.getEnd()), ZoneId.of("UTC"))
                        .toString());
        return output;
    }

    @Override
    public String toString() {
        return "WindowOutput{" +
                "browser='" + browser + '\'' +
                ", count=" + count +
                ", windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                '}';
    }
}
