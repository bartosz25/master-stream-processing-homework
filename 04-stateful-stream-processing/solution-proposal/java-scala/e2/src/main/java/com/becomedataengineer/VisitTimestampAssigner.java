package com.becomedataengineer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class VisitTimestampAssigner implements TimestampAssigner<String> {

    public static class Supplier implements TimestampAssignerSupplier<String> {

        @Override
        public TimestampAssigner<String> createTimestampAssigner(Context context) {
            return new VisitTimestampAssigner();
        }
    }

    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        try {
            JsonNode event = Json.MAPPER.readTree(element);
            return ZonedDateTime.parse(
                    event.get("eventTime").asText(),
                    DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    .toInstant().atZone(ZoneId.of("UTC")).toInstant()
                    .toEpochMilli();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
