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


class ETL():
    def __init__(self) -> None:
        self._last_sync_date = datetime.now()
        self.data_extractor_obj = self.DataExtractor()

    class DataExtractor():
        def __init__(self) -> None:
            self._last_modified = datetime(2009, 10, 5, 18, 00)  # must be gotten from the State
            self._person_ids = []
            self._movies_ids = []

        def _get_persons_ids(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    pg_cursor.execute(
                        """SELECT id, modified
                            FROM content.person
                            WHERE modified > '{}'
                            ORDER BY modified
                            LIMIT 100;""".format(self._last_modified)
                        )

                    persons = pg_cursor.fetchall()
                    self._person_ids = [person[0] for person in persons]
                    self._last_modified = persons[100][1]  # save in state
                except Exception as e:
                    print(e)

        def _get_movies_ids(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    pg_cursor.execute(
                        """SELECT fw.id, fw.modified
                            FROM content.film_work fw
                            LEFT JOIN content.person_film_work pfw
                            ON pfw.film_work_id = fw.id
                            WHERE pfw.person_id IN {}
                            ORDER BY fw.modified
                            LIMIT 100;""".format(tuple(self._person_ids))
                    )

                    movies_ids = pg_cursor.fetchall()
                    self._movies_ids = [movie_id[0] for movie_id in movies_ids]

                except Exception as e:
                    print(e)

        def _merge_data(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    pg_cursor.execute(
                        """SELECT
                            fw.id as fw_id,
                            fw.title,
                            fw.description,
                            fw.rating,
                            fw.type,
                            fw.created,
                            fw.modified,
                            pfw.role,
                            p.id,
                            p.full_name,
                            g.name
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw
                                ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p
                                ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw
                                ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g
                                ON g.id = gfw.genre_id
                        WHERE fw.id IN {}; """.format(tuple(self._movies_ids))
                    )
                    data = pg_cursor.fetchall()

                except Exception as e:
                    print(e)

        def collect_data(self):
            self._get_persons_ids()
            self._get_movies_ids()
            self._merge_data()

    class DataTransformer():
        pass

    class DataLoader():
        pass
