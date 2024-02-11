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
topic_name = 'visits_raw_for_dlq'
for topic in [topic_name, 'valid_data', 'dead_letter']:
    os.system(
        f'docker exec p3_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic} --delete')
    os.system(
        f'docker exec p3_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic} --create --partitions 2')


def random_visit() -> Dict[str, str]:
    visit_dict = {'eventId': str(uuid.uuid4()), 'os': random.choice(["macOS", "Windows", "Linux", "x....", "unknown"]),
                  'visitedPage': random.choice(
                      ["category-1", "category-1/product-1", None, "index.html", "contact.html", None])}
    if not visit_dict['visitedPage']:
        visit_dict.pop('visitedPage')
    return {'value': json.dumps(visit_dict), 'key': str(uuid.uuid4())}


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
    ], schema='key STRING, value STRING')
    visits_to_generate.write.format("kafka") \
        .option("topic", topic_name).option("kafka.bootstrap.servers", "localhost:29092") \
        .save()

    generation_day = generation_day.add(minutes=1)
    time.sleep(5)
