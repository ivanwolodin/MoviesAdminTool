import time

from etl import ETL
from es_index import create_es_index


if __name__ == '__main__':
    create_es_index()
    etl_obj = ETL()

    while True:
        time.sleep(1)
        etl_obj.run()
