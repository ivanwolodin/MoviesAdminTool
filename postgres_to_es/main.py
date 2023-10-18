import time

from etl import create_es_index, extract

if __name__ == "__main__":
    create_es_index()
    print(extract())
    while True:
        time.sleep(100)
        print("I am doing this!")
