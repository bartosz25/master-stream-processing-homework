import json
import os
import time
from typing import Dict

import pendulum as pendulum
from pendulum import DateTime
from pyspark.sql import SparkSession

print('Creating topics')
topic_name = 'data_for_backpressure'
os.system(
    f'docker exec p4_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --delete')
os.system(
    f'docker exec p4_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --create --partitions 2')


def row_to_write(id: int, row_value: str, generation_time: DateTime) -> Dict[str, str]:
    return {'value': json.dumps({'id': id, 'row_value': row_value,
                       'generation_time': generation_time.to_iso8601_string()})}


spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0').getOrCreate()

generation_day = pendulum.now('UTC').subtract(days=10)
while pendulum.now('UTC') > generation_day:
    print('Sending data')
    visits_to_generate = spark.createDataFrame(data=[
        row_to_write(1, "A", generation_day.add(seconds=1)), row_to_write(1, "E", generation_day.add(seconds=2)),
        row_to_write(1, "I", generation_day.add(seconds=3)), row_to_write(1, "O", generation_day.add(seconds=4)),
        row_to_write(1, "U", generation_day.add(seconds=5)), row_to_write(1, "AA", generation_day.add(seconds=6)),
        row_to_write(1, "EE", generation_day.add(seconds=7))
    ], schema='value STRING')
    visits_to_generate.write.format("kafka")\
        .option("topic", topic_name).option("kafka.bootstrap.servers", "localhost:29092")\
        .save()

    generation_day = generation_day.add(minutes=1)
    time.sleep(5)
