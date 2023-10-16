from dataclasses import asdict

from db_connections import open_postgres_connection
from logger import logger


class PostgresSaver:
    def __init__(self):
        pass

    def save_data(self, data: dict) -> bool:
        try:
            logger.info(
                'Inserting chunk into: {0}'.format(data['postgres_table']),
            )
            with open_postgres_connection() as pg_cursor:
                args = ','.join(
                    pg_cursor.mogrify(data['string_pattern'],
                        tuple([asdict(item)[x] for x in data['ordered_keys']]),
                        ).decode()
                    for item in data['res']
                )
                pg_cursor.execute(data['postgres_sql_upsert'].format(args))
        except Exception as e:
            logger.info('Cannot upsert data: Error: {0}'.format(e))
