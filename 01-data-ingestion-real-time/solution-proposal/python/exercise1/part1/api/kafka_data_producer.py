import json
import os
import pathlib
import time

from confluent_kafka import Producer, Message


class KafkaDataProducerCallbacksHolder:

    def __init__(self):
        self.events_to_deliver = []

    def register_event_to_deliver(self, event_payload):
        self.events_to_deliver.append(event_payload)

        def handle_delivery_result(error, result: Message):
            if error:
                print('Failure' + error)
            else:
                print('success')
                self.events_to_deliver.remove(event_payload)

        return handle_delivery_result


class KafkaDataProducer:

    OUTPUT_TOPIC = 'raw_data'

    def __init__(self):
        self.producer = Producer({'bootstrap.servers': 'localhost:29092',
                                  'linger.ms': 1 * 1000,  # 10 seconds
                                  'batch.size': '12KB',
                                  'retries': 3
                                  })
        self.kafka_data_producer_callbacks_holder = KafkaDataProducerCallbacksHolder()

    def produce_data_to_kafka(self, visits_to_ingest):
        for visit_json in visits_to_ingest:
            visit = json.loads(visit_json)
            self.producer.produce(
                topic=KafkaDataProducer.OUTPUT_TOPIC,
                value=bytes(visit_json, encoding='utf-8'),
                key=bytes(str(visit['visitId']), encoding='utf-8'),
                on_delivery=self.kafka_data_producer_callbacks_holder.register_event_to_deliver(visit_json)
            )

        self.producer.flush(timeout=20)

    def save_undelivered_records(self):
        if self.kafka_data_producer_callbacks_holder.events_to_deliver:
            dir_name = '/tmp/bde-3/homework/p1/e1/'
            file_name = f'fallback_{round(time.time() * 1000)}.json'
            os.makedirs(dir_name, exist_ok=True)
            pathlib.Path(f'{dir_name}/{file_name}') \
                .write_text(
                '\n'.join([event for event in self.kafka_data_producer_callbacks_holder.events_to_deliver]))
