from contextlib import contextmanager
from elasticsearch import Elasticsearch

from constants import ES_PORT


@contextmanager
def open_elasticsearch_connection():
    with Elasticsearch(
        hosts=f'http://elasticsearch:{ES_PORT}/',
        request_timeout=300,
        max_retries=10,
        retry_on_timeout=True,
    ) as es:
        yield es
