import json
import time
import uuid

import pendulum as pendulum
import requests
from pyspark import Row

generation_day = pendulum.now('UTC').subtract(days=10)
while pendulum.now('UTC') > generation_day:
    visits_to_generate = [
        Row(eventId=str(uuid.uuid4()), visitId=1, userId=1, visitedPage="/home", visitTime=generation_day.add(seconds=1).isoformat()),
        Row(eventId=str(uuid.uuid4()), visitId=1, userId=1, visitedPage="/contact", visitTime=generation_day.add(seconds=2).isoformat()),
        Row(eventId=str(uuid.uuid4()), visitId=1, userId=1, visitedPage="/products", visitTime=generation_day.add(seconds=4).isoformat()),
        Row(eventId=str(uuid.uuid4()), visitId=2, userId=2, visitedPage="/my-account", visitTime=generation_day.add(seconds=1).isoformat()),
        Row(eventId=str(uuid.uuid4()), visitId=2, userId=2, visitedPage="/my-orders", visitTime=generation_day.add(seconds=3).isoformat()),
        Row(eventId=str(uuid.uuid4()), visitId=2, userId=2, visitedPage="/my-orders/order/1", visitTime=generation_day.add(seconds=6).isoformat()),
        Row(eventId=str(uuid.uuid4()), visitId=3, userId=3, visitedPage="/home", visitTime=generation_day.add(seconds=10).isoformat())
    ]

    def send_visit_to_api_gateway(visits):
        visits_to_send = {
            'visits': list(map(lambda visit: json.dumps(visit.asDict()), visits))
        }
        request_result = requests.post('http://localhost:8088/visits/ingest', json=json.dumps(visits_to_send))
        print(f'Got response: {request_result}')
        request_result.raise_for_status()

    send_visit_to_api_gateway(visits_to_generate)

    generation_day.add(minutes=1)
    time.sleep(5)
