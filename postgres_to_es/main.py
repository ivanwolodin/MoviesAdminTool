import time

from etl import create_es_index, ETL

if __name__ == "__main__":
    create_es_index()
    etl_obj = ETL()
    
    while True:
        time.sleep(1)
        etl_obj.run()
