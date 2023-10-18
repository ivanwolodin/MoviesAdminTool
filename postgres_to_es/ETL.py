from elasticsearch import Elasticsearch
from constants import ES_INDEX
from db_connections import open_postgres_connection

import time
from datetime import datetime

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
    last_modified = datetime(2009, 10, 5, 18, 00)

    with open_postgres_connection() as pg_cursor:
        try:
            pg_cursor.execute("SELECT id, modified FROM content.person  WHERE modified > '{}' ORDER BY modified LIMIT 100;".format(last_modified))
            res = pg_cursor.fetchall()
        except Exception as e:
            print(e)
    return res


def transform():
    pass


def load():
    pass
