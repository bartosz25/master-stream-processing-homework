package com.becomedataengineer;

import java.util.Arrays;
import java.util.List;

public class KafkaConfiguration {

    public static final String INPUT_TOPIC_NAME = "visits";
    public static final String OUTPUT_TOPIC_NAME = "visits_count";

    public static final List<String> ALL_TOPICS = Arrays.asList(INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME);
}