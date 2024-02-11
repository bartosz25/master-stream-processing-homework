import json
import os
import random
import time
from datetime import datetime, timedelta

from confluent_kafka import Producer

from config import get_input_topic_name, get_output_topic_name

print('>>> Recreating topics...')
for topic_name in [get_input_topic_name(), get_output_topic_name()]:
    print(f'....{topic_name}')
    os.system(
        f'docker exec e2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --delete')
    os.system(
        f'docker exec e2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic {topic_name} --create --partitions 2')


def visit(visit_id: str, event_time: datetime) -> str:
    return json.dumps({'visit_id': visit_id,
                       'event_time': event_time.isoformat(),
                       'browser': random.choice(["Firefox", "Chrome", "Safari"])})


event_time_for_events = datetime.utcnow()
producer = Producer({'bootstrap.servers': 'localhost:29092'})
while True:
    event_time_for_events += timedelta(seconds=30)
    print('>>>> Generating records')
    producer.produce(topic=get_input_topic_name(), partition=0, value=visit('1', event_time_for_events))
    producer.produce(topic=get_input_topic_name(), partition=1, value=visit('2', event_time_for_events))
    producer.produce(topic=get_input_topic_name(), partition=1, value=visit('3', event_time_for_events))
    producer.flush()

    time.sleep(5)
