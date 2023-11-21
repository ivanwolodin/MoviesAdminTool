import psycopg2

from contextlib import contextmanager
from psycopg2.extras import DictCursor

from constants import dsl


@contextmanager
def open_postgres_connection():
    pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield pg_conn.cursor()
    finally:
        pg_conn.commit()
        pg_conn.close()
