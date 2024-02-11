import json
import os
import random
import shutil
import time
import uuid
from typing import Dict

import pendulum as pendulum
from pyspark.sql import SparkSession, functions

print('Creating topics')
topic_name = 'visits_raw'
for topic in [topic_name, 'valid_visits']:
    os.system(
        f'docker exec p2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic} --delete')
    os.system(
        f'docker exec p2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic} --create --partitions 2')

files_output_dir = '/tmp/bde/module5/homework/e2/output'
shutil.rmtree(files_output_dir, ignore_errors=True)


def random_visit() -> Dict[str, str]:
    return {'value': json.dumps(
        {'eventId': str(uuid.uuid4()), 'os': random.choice(["macOS", "Windows", "Linux", "x....", "unknown"]),
         'visitedPage': random.choice(
             ["category-1", "category-1/product-1", "index.html", "contact.html", ""])})}


spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0').getOrCreate()

generation_day = pendulum.now('UTC').subtract(days=10)
while pendulum.now('UTC') > generation_day:
    print('Sending data')
    visits_to_generate = spark.createDataFrame(data=[
        random_visit(), random_visit(), random_visit(), random_visit(),
        random_visit(), random_visit(), random_visit(), random_visit(),
        random_visit(), random_visit(), random_visit(), random_visit(),
        random_visit(), random_visit(), random_visit(), random_visit(),
        random_visit(), random_visit(), random_visit(), random_visit()
    ], schema='value STRING')
    visits_to_generate.write.format("kafka") \
        .option("topic", topic_name).option("kafka.bootstrap.servers", "localhost:29092") \
        .save()

    visits_to_generate \
        .withColumn("event_time", functions.lit(generation_day.to_date_string())) \
        .write.format("json").mode('append').partitionBy(["event_time"]).save(files_output_dir)

    generation_day = generation_day.add(days=1)
    time.sleep(0.5)

print('Finished!')
