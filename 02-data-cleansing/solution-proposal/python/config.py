def kafka_input_topic() -> str:
    return 'raw_data'


def checkpoint_location_for_job(job: str) -> str:
    return f'/tmp/bde/module4/homework/checkpoint/{job}'
