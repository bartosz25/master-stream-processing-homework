import json

from flask import Response, request

from exercise1.part1.api.kafka_data_producer import KafkaDataProducer


def create_visits_endpoint(app):
    @app.route('/visits/ingest', methods=['POST'])
    def send_new_visits_to_kafka():
        visits_to_ingest = json.loads(request.get_json(force=True))
        print(f'Got visits to ingest: {visits_to_ingest}')

        kafka_data_producer = KafkaDataProducer()
        kafka_data_producer.produce_data_to_kafka(visits_to_ingest['visits'])
        kafka_data_producer.save_undelivered_records()

        response_status = 200
        return Response(status=response_status, mimetype='application/json')