package com.becomedataengineer;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Visit {

    private String visitId;
    private String eventTime;

    private String browser;

    public Visit() {}

    public Visit(String visitId, ZonedDateTime eventTime) {
        this.visitId = visitId;
        this.eventTime = eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        List<String> browsers = Arrays.asList("Firefox", "Chrome", "Safari");
        int randomIndex = ThreadLocalRandom.current().nextInt(0, browsers.size());
        this.browser = browsers.get(randomIndex);
    }

    public String getVisitId() {
        return visitId;
    }

    public void setVisitId(String visitId) {
        this.visitId = visitId;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    @Override
    public String toString() {
        return "Visit{" +
                "visitId='" + visitId + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", browser='" + browser + '\'' +
                '}';
    }
}
