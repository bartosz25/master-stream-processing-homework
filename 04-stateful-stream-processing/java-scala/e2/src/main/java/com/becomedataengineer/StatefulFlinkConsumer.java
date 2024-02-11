package com.becomedataengineer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.becomedataengineer.KafkaConfiguration.OUTPUT_TOPIC_NAME;

public class StatefulFlinkConsumer {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);        localEnvironment.setParallelism(2);
        localEnvironment.enableCheckpointing(TimeUnit.SECONDS.toMillis(20));
        localEnvironment.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:29092")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics(KafkaConfiguration.INPUT_TOPIC_NAME)
                .build();

        System.setProperty("user.timezone", "UTC");
        SingleOutputStreamOperator<String> kafkaDataStreamSource = localEnvironment.fromSource(kafkaSource,
                WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(
                        new VisitTimestampAssigner.Supplier()
                ),
                "Kafka Source").uid("kafka source");

        SingleOutputStreamOperator<Visit> visits = kafkaDataStreamSource.map((String inputRecord) -> Json.MAPPER.readValue(inputRecord, Visit.class));

        WindowedStream<Visit, String, TimeWindow> tumblingVisitWindow = visits.keyBy(Visit::getBrowser)
            .window(
                    TumblingEventTimeWindows.of(Time.minutes(5))
            ).allowedLateness(
                    Time.seconds(0)
            ).trigger(
                    EventTimeTrigger.create()
            );

        SingleOutputStreamOperator<String> windowOutput = tumblingVisitWindow.process(new ProcessWindowFunction<Visit, String, String, TimeWindow>() {
            @Override
            public void process(String browser,
                                ProcessWindowFunction<Visit, String, String, TimeWindow>.Context context,
                                Iterable<Visit> elements,
                                Collector<String> outputCollector) {
                try {
                    WindowOutput output = WindowOutput.valueOf(
                            context.window(), browser, IterableUtils.toStream(elements).count()
                    );
                    outputCollector.collect(Json.MAPPER.writeValueAsString(output));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }).uid("windowOutput");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC_NAME)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        windowOutput.sinkTo(sink);
        localEnvironment.execute();
    }

}