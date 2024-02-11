import json
import os
import random
import uuid
from datetime import datetime, timedelta
import time
from typing import Dict, List

from pyspark.sql import SparkSession

from e1.e1_config import get_output_topic_name, get_input_topic_name, get_reference_dataset_location

print('>>> Recreating topics...')
for topic_name in [get_input_topic_name(), get_output_topic_name()]:
    print(f'...{topic_name}')
    os.system(
        f'docker exec e1_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --delete')
    os.system(
        f'docker exec e1_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --create --partitions 2')

reference_dataset = """{"id": "ff", "name": "Firefox", "version": "1.0"}
{"id": "ff", "name": "Firefox", "version": "1.1"}
{"id": "ch", "name": "Chrome", "version": "10.0"}
{"id": "ch", "name": "Chrome", "version": "10.1"}
{"id": "sf", "name": "Safari", "version": "100.0"}"""
with open(get_reference_dataset_location(), 'w', encoding='utf-8') as f:
    f.write(reference_dataset)

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()


def visit(user_id: int, event_time: datetime, browser: str, browser_version: str) -> Dict[str, str]:
    return {'value': json.dumps({'userId': user_id,
                                 'eventTime': event_time.isoformat(), 'browserKey': browser,
                                 'browserVersion': browser_version,
                                 'eventId': str(uuid.uuid4()),
                                 'visitedPage': random.choice(["page1", "page2", "page3", "page4", "page5"])})}


generation_time = datetime.utcnow() - timedelta(days=10)
user_ids = [1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5]
browsers = {1: "ff", 2: "ch", 3: "sf", 4: "ff", 5: "ch"}
browser_versions = {1: "1.0", 2: "10.0", 3: "100.0", 4: "1.1", 5: "10.1"}
last_time_per_user = {1: generation_time, 2: generation_time, 3: generation_time, 4: generation_time,
                      5: generation_time}

while generation_time < datetime.utcnow():
    print('Sending records')
    visits_to_send: List[Dict[str, str]] = []
    for i in range(0, 10):
        generated_user_id = random.choice(user_ids)
        should_close_session = random.randint(0, 1000) % 50 == 0
        if should_close_session:
            print(f'Closing session for {generated_user_id}')
            to_add = random.randint(30, 45)
            user_time = last_time_per_user[generated_user_id] + timedelta(minutes=to_add)
        else:
            to_add = random.randint(1, 16)
            user_time = last_time_per_user[generated_user_id] + timedelta(seconds=to_add)
        last_time_per_user[generated_user_id] = user_time

        visits_to_send.append(visit(generated_user_id, user_time, browsers[generated_user_id], browser_versions[generated_user_id]))

    spark.createDataFrame(visits_to_send, "value STRING") \
        .selectExpr("value")\
        .write.format('kafka').option('kafka.bootstrap.servers', 'localhost:29092') \
        .option('topic', get_input_topic_name()).save()

    generation_time = generation_time + timedelta(seconds=120)
    time.sleep(5)
