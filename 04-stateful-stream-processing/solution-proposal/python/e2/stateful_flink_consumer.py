#!/usr/bin/env python3
import datetime
import json
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration, Time, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, WindowedStream, DataStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows, CountTrigger

from config import get_input_topic_name, get_output_topic_name
from partial_event_time_window_trigger import PartialEventTimeWindowTrigger
from visit import Visit
from visit_timestamp_assigner import VisitTimestampAssigner
from visit_window_processor import VisitWindowProcessor

# This configuration is very important
# Without it, you may encounter class loading-related errors, such as:
# Caused by: java.lang.ClassCastException: cannot assign instance of
#   org.apache.kafka.clients.consumer.OffsetResetStrategy to field
#   org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer.offsetResetStrategy
#   of type org.apache.kafka.clients.consumer.OffsetResetStrategy
#   in instance of org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer
config = Configuration()
config.set_string("classloader.resolve-order", "parent-first")
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(2)
env.configure(config)

checkpoint_interval_30_seconds = 30000
env.enable_checkpointing(checkpoint_interval_30_seconds)
# Unlike PySpark, you have to define the JARs explicitly for Flink
env.add_jars(
    f"file://{os.getcwd()}/kafka-clients-3.2.3.jar",
    f"file://{os.getcwd()}/flink-connector-base-1.17.0.jar",
    f"file://{os.getcwd()}/flink-connector-kafka-1.17.0.jar"
)

kafka_source = KafkaSource.builder().set_bootstrap_servers('localhost:29092') \
    .set_group_id('bde_h06_e2') \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()).set_topics(get_input_topic_name()).build()

watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(VisitTimestampAssigner())
data_source = env.from_source(
    source=kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="Kafka Source"
).uid("Kafka Source")


def map_json_to_visit(json_payload: str) -> Visit:
    event = json.loads(json_payload)
    event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp())
    return Visit(visit_id=event['visit_id'], browser=event['browser'], event_time=event_time)


records_per_visit_counts: WindowedStream = data_source.map(map_json_to_visit) \
    .key_by(lambda visit: visit.browser).window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .allowed_lateness(0) \
    .trigger(PartialEventTimeWindowTrigger())

# must add the Types.STRING() to avoid serialization issues
window_output: DataStream = records_per_visit_counts.process(VisitWindowProcessor(), Types.STRING()).uid(
    "window output")

kafka_sink = KafkaSink.builder().set_bootstrap_servers("localhost:29092") \
    .set_record_serializer(KafkaRecordSerializationSchema.builder()
                           .set_topic(get_output_topic_name())\
                           .set_value_serialization_schema(SimpleStringSchema()).build()).build()

window_output.sink_to(kafka_sink)

env.execute('BDE irregular checkpoint example')
