from typing import Dict


def get_files_output_dir() -> str:
    return '/tmp/bde/module5/homework/e2/output'


def get_sink_configuration() -> Dict[str, str]:
    return {
        'kafka.bootstrap.servers': 'localhost:29092',
        'topic': 'valid_visits'
    }
