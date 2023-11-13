import time

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from logger import logger
from constants import ES_INDEX_STRUCTURE, ES_INDEX_NAME


es = Elasticsearch(
    hosts='http://elasticsearch:9200/',
    request_timeout=300,
    max_retries=10,
    retry_on_timeout=True,
)


def create_es_index():
    logger.info('creating index...')
    connected = False
    while not connected:
        try:
            es.info()
            connected = True
        except ConnectionError:
            logger.info(
                'Elasticsearch not available yet, trying again in 2s...'
            )
            time.sleep(2)
    if not es.indices.exists(index=ES_INDEX_NAME):
        es.indices.create(index=ES_INDEX_NAME, body=ES_INDEX_STRUCTURE)
        logger.info('index was created!')
    else:
        logger.info('index already created!')
