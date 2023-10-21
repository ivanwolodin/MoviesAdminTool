from elasticsearch import Elasticsearch
from constants import ES_INDEX
from db_connections import open_postgres_connection

import time
from collections import defaultdict
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
        self.data_transformer = self.DataTransformer()

    def run(self):
        data = self.data_extractor_obj.collect_data()
        self.data_transformer.transform_data(data)
        print(self.data_transformer.chunk)
        self.data_transformer.clear_aux_data()

    class DataExtractor():
        def __init__(self) -> None:
            self._last_modified_person = datetime(2009, 10, 5, 18, 00)  # must be gotten from the State
            self._last_modified_movie = datetime(2009, 10, 5, 18, 00)  # must be gotten from the State
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
                            LIMIT 100;""".format(self._last_modified_person)
                        )
                    persons = pg_cursor.fetchall()
                    if not persons:
                        self._person_ids = []
                        return
                    self._person_ids = [person[0] for person in persons]
                    self._last_modified_person = persons[len(persons) - 1][1]  # save in state
                except Exception as e:
                    print(e)

        def _get_movies_ids(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    if not self._person_ids:
                        self._movies_ids = []
                        return
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
                    if not self._movies_ids:
                        return []
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
                        WHERE fw.id IN {} AND fw.modified > '{}' ; """.format(
                            tuple(self._movies_ids),
                            self._last_modified_movie,
                        )
                    )
                    data = pg_cursor.fetchall()
                    self._last_modified_movie = data[len(data) - 1].get('modified')
                    return data

                except Exception as e:
                    print(e)

        def collect_data(self):
            self._get_persons_ids()
            self._get_movies_ids()
            return self._merge_data()

    class DataTransformer():
        # данные для вставки в ES
        # data = {
        #     "id": "1",
        #     "imdb_rating": 8.5,
        #     "genre": ["драма", "криминал"],
        #     "title": "Тёмный рыцарь",
        #     "description": "Фильм о Бэтмене и Джокере",
        #     "director": "Кристофер Нолан",
        #     "actors_names": ["Кристиан Бэйл", "Хит Леджер"],
        #     "writers_names": ["Джонатан Нолан", "Кристофер Нолан"],
        #     "actors": [
        #         {"id": "1", "name": "Кристиан Бэйл"},
        #         {"id": "2", "name": "Хит Леджер"}
        #     ],
        #     "writers": [
        #         {"id": "1", "name": "Джонатан Нолан"},
        #         {"id": "2", "name": "Кристофер Нолан"}
        #     ]
        # }

        # # вставка данных
        # res = es.index(index="movies", body=data)

        def __init__(self) -> None:
            self.clear_aux_data()

        def clear_aux_data(self):
            self.aux_dict = defaultdict(lambda: {
                'imdb_rating': None,
                'genre': None,
                'title': None,
                'description': None,
                'actors_names': [],
                'actors': [],
                'writers_names': [],
                'writers': [],
                'director': None
            })
            self.chunk = []

        def transform_data(self, data: list) -> list:
            if not data:
                return

            for row in data:
                if self.aux_dict.get(row['fw_id']) is None:
                    self.aux_dict[row['fw_id']]['imdb_rating'] = row['rating']
                    self.aux_dict[row['fw_id']]['genre'] = row['type']
                    self.aux_dict[row['fw_id']]['title'] = row['title']
                    self.aux_dict[row['fw_id']]['description'] = row['description']

                if row['role'] == 'actor' and row['full_name'] not in self.aux_dict[row['fw_id']]['actors_names']:
                    self.aux_dict[row['fw_id']]['actors_names'].append(row['full_name'])
                    self.aux_dict[row['fw_id']]['actors'].append(
                        {
                            'id': row['id'],
                            'name': row['full_name']
                        }
                    )
                if row['role'] == 'writer' and row['full_name'] not in self.aux_dict[row['fw_id']]['writers_names']:
                    self.aux_dict[row['fw_id']]['writers_names'].append(row['full_name'])
                    self.aux_dict[row['fw_id']]['writers'].append(
                        {
                            'id': row['id'],
                            'name': row['full_name']
                        }
                    )
                if row['role'] == 'director' and row['full_name'] != self.aux_dict[row['fw_id']]['director']:
                    self.aux_dict[row['fw_id']]['director'] = row['full_name']

            for k, v in self.aux_dict.items():
                filmwork = {
                    "id": k,
                    "imdb_rating": v['imdb_rating'],
                    "genre": v['genre'],
                    "title": v['title'],
                    "description": v['description'],

                    "director": v['director'],
                    "actors_names": v['actors_names'],
                    "writers_names": v['writers_names'],
                    "actors": v['actors'],
                    "writers": v['writers'],
                }
                self.chunk.append(filmwork)

    class DataLoader():
        pass
