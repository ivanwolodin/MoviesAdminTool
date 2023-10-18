from elasticsearch import Elasticsearch
from constants import ES_INDEX
from db_connections import open_postgres_connection

import time

es = Elasticsearch(
        hosts="http://elasticsearch:9200/",
        request_timeout=300,
        max_retries=10,
        retry_on_timeout=True,
    )


def create_es_index():
    print("creating index...")
    connected = False
    while not connected:
        try:
            es.info()
            connected = True
        except Exception as e:
            print("Elasticsearch not available yet, trying again in 2s...")
            time.sleep(2)
    if not es.indices.exists(index='movies'):
        es.indices.create(
            index='movies',
            body=ES_INDEX
        )
        print("index was created!")
    else:
        print("index already created!")


def extract():
    with open_postgres_connection() as pg_cursor:        
        pg_cursor.execute("SELECT * FROM content.genre LIMIT 10")
        res = pg_cursor.fetchall()
    return res


def transform():
    pass


def load():
    pass
