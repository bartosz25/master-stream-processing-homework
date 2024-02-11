import json
import os
import random
import uuid
from datetime import datetime, timedelta
import time
from typing import Dict, List

from pyspark.sql import SparkSession

from e3.e3_config import get_input_topic_name, get_output_topic_name

print('>>> Recreating topics...')
for topic_name in [get_input_topic_name(), get_output_topic_name()]:
    print(f'...{topic_name}')
    os.system(
        f'docker exec e3_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --delete')
    os.system(
        f'docker exec e3_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --create --partitions 2')


spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()


def visit(user_id: int, event_time: datetime) -> Dict[str, str]:
    return {'value': json.dumps({'userId': user_id,
                                 'eventId': str(uuid.uuid4()),
                                 'eventTime': event_time.isoformat(),
                                 'browser': random.choice(["Firefox", "Chrome", "Safari"]),
                                 'visitedPage': random.choice(["page1", "page2", "page3", "page4", "page5"])})}


generation_time = datetime.utcnow() - timedelta(days=10)
user_ids = [1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5]

while generation_time < datetime.utcnow():
    print('Sending records')
    event_time_for_record = generation_time + timedelta(seconds=120)
    visits_to_send: List[Dict[str, str]] = []
    for i in range(0, 10):
        generated_user_id = random.choice(user_ids)

        visits_to_send.append(visit(generated_user_id, event_time_for_record))

    spark.createDataFrame(visits_to_send, "value STRING") \
        .selectExpr("value")\
        .write.format('kafka').option('kafka.bootstrap.servers', 'localhost:29092') \
        .option('topic', get_input_topic_name()).save()

    generation_time = generation_time + timedelta(seconds=120)

    time.sleep(5)
