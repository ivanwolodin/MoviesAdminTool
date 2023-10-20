import time

from etl import create_es_index, ETL

if __name__ == "__main__":
    create_es_index()
    etl_obj = ETL()
    etl_obj.data_extractor_obj.collect_data()
    while True:
        time.sleep(100)
        print("I am doing this!")
