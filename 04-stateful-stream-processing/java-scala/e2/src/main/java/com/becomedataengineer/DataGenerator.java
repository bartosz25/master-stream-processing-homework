package com.becomedataengineer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.becomedataengineer.KafkaConfiguration.ALL_TOPICS;

public class DataGenerator {

    public static void main(String[] args) throws Exception {
        System.out.println(">>> Creating topics...");
        ALL_TOPICS.forEach(topicName -> {
            try {
                Process deleteTopicCommand = null;
                deleteTopicCommand = Runtime.getRuntime()
                        .exec("docker exec e2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic "+topicName +" --delete");
                deleteTopicCommand.waitFor();
                deleteTopicCommand.exitValue();
                Process createTopicCommand = null;
                createTopicCommand = Runtime.getRuntime()
                        .exec("docker exec e2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic "+topicName +" --create --partitions 2");
                createTopicCommand.waitFor();
                if (createTopicCommand.exitValue() != 0) {
                    throw new RuntimeException("Topic " + topicName + " couldn't be created");
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        System.setProperty("user.timezone", "UTC");
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:29092");
        producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        ZonedDateTime eventTime = ZonedDateTime.now(ZoneId.of("UTC"));
        int toGenerate = 1000000;
        while (toGenerate-- > 0) {
            System.out.println(">>> Generating records");
            eventTime = eventTime.plusSeconds(30);
            try {
                String jsonA = Json.MAPPER.writeValueAsString(new Visit("1", eventTime));
                producer.send(new ProducerRecord<>(KafkaConfiguration.INPUT_TOPIC_NAME,  "1", jsonA));
                String jsonB = Json.MAPPER.writeValueAsString(new Visit("2", eventTime));
                producer.send(new ProducerRecord<>(KafkaConfiguration.INPUT_TOPIC_NAME,  "2", jsonB));
                String jsonC = Json.MAPPER.writeValueAsString(new Visit("3", eventTime));
                producer.send(new ProducerRecord<>(KafkaConfiguration.INPUT_TOPIC_NAME,  "3", jsonC));
                producer.flush();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

}
