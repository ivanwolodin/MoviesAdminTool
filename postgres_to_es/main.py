import time

from es_index import ES_INDEX
from elasticsearch import Elasticsearch

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
        except:
            print("Elasticsearch not available yet, trying again in 2s...")
            time.sleep(2)
    es.indices.create(
        index='movies',
        body=ES_INDEX
    )
    print("index was created!")


if __name__ == "__main__":
    create_es_index()
    while True:
        time.sleep(5)
        print("I am doing this!")
