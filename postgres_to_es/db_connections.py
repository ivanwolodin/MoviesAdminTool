import os

import psycopg2

from contextlib import contextmanager
from psycopg2.extras import DictCursor


dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
}
print(dsl)

@contextmanager
def open_postgres_connection():
    pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield pg_conn.cursor()
    finally:
        pg_conn.commit()
        pg_conn.close()
