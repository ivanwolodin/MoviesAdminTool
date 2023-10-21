from backoff import backoff
from db_connections import open_postgres_connection
from elasticsearch import helpers
from es_index import es
from logger import logger
from state_worker import state
from sql import sql_selects

from collections import defaultdict
from datetime import datetime


class ETL():
    def __init__(self) -> None:
        self._last_sync_date = datetime.now()
        self.data_extractor_obj = self.DataExtractor()
        self.data_transformer = self.DataTransformer()
        self.data_loader = self.DataLoader()

    def run(self):
        raw_data = self.data_extractor_obj.collect_data()
        es_data = self.data_transformer.transform_data(raw_data)
        self.data_loader.load_to_es(es_data)

    class DataExtractor():
        def __init__(self) -> None:
            self._last_modified_person = datetime(2009, 10, 5, 18, 00)
            self._person_ids = []
            self._movies_ids = []

        @backoff()
        def _get_persons_ids(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    if state.get_state('last_sync') is None:
                        self._last_modified_person = datetime(2009, 10, 5, 18, 00)
                    else:
                        self._last_modified_person = datetime.fromisoformat(state.get_state('last_sync'))

                    pg_cursor.execute(
                        sql_selects.get('persons').format(self._last_modified_person)
                    )
                    persons = pg_cursor.fetchall()
                    if not persons:
                        self._person_ids = []
                        return
                    self._person_ids = [person[0] for person in persons]

                    state.set_state(
                        key='last_sync',
                        value=persons[len(persons) - 1][1].isoformat(),
                    )
                    
                except Exception as e:
                    logger.error(e)

        @backoff()
        def _get_movies_ids(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    if not self._person_ids:
                        self._movies_ids = []
                        return
                    pg_cursor.execute(
                        sql_selects.get('movies_by_persons').format(tuple(self._person_ids))
                    )
                    movies_ids = pg_cursor.fetchall()
                    self._movies_ids = set([movie_id[0] for movie_id in movies_ids])

                except Exception as e:
                    logger.error(e)

        @backoff()
        def _merge_data(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    if not self._movies_ids:
                        return []
                    pg_cursor.execute(
                        sql_selects.get('persons_genres_film_works_by_movies').format(
                            tuple(self._movies_ids),
                        )
                    )
                    data = pg_cursor.fetchall()
                    return data

                except Exception as e:
                    logger.error(e)

        def collect_data(self):
            self._get_persons_ids()
            self._get_movies_ids()
            return self._merge_data()

    class DataTransformer():
        def __init__(self) -> None:
            self._clear_aux_data()
            self._how_many = 0

        def _clear_aux_data(self):
            self._aux_dict = defaultdict(lambda: {
                'imdb_rating': '',
                'genre': '',
                'title': '',
                'description': '',
                'actors_names': [],
                'actors': [],
                'writers_names': [],
                'writers': [],
                'director': ''
            })

        def transform_data(self, data: list) -> list:
            if not data:
                return

            for row in data:
                if self._aux_dict.get(row['fw_id']) is None:
                    self._aux_dict[row['fw_id']]['imdb_rating'] = row['rating']
                    self._aux_dict[row['fw_id']]['genre'] = row['type']
                    self._aux_dict[row['fw_id']]['title'] = row['title']
                    self._aux_dict[row['fw_id']]['description'] = row['description']

                if row['role'] == 'actor' and row['full_name'] not in self._aux_dict[row['fw_id']]['actors_names']:
                    self._aux_dict[row['fw_id']]['actors_names'].append(row['full_name'])
                    self._aux_dict[row['fw_id']]['actors'].append(
                        {
                            'id': row['id'],
                            'name': row['full_name']
                        }
                    )
                if row['role'] == 'writer' and row['full_name'] not in self._aux_dict[row['fw_id']]['writers_names']:
                    self._aux_dict[row['fw_id']]['writers_names'].append(row['full_name'])
                    self._aux_dict[row['fw_id']]['writers'].append(
                        {
                            'id': row['id'],
                            'name': row['full_name']
                        }
                    )
                if row['role'] == 'director' and row['full_name'] != self._aux_dict[row['fw_id']]['director']:
                    self._aux_dict[row['fw_id']]['director'] = row['full_name']

            chunk = []
            for k, v in self._aux_dict.items():
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
                chunk.append(filmwork)

            self._clear_aux_data()
            self._how_many += len(chunk)
            logger.info(f'Total proccessed: {self._how_many}')
            return chunk

    class DataLoader():
        @backoff()
        def load_to_es(self, data):
            if not data:
                return
            actions = [
                {
                    '_index': 'movies',
                    '_id': row['id'],
                    '_source': row,
                }
                for row in data
            ]
            res = helpers.bulk(es, actions)
            logger.info(res)
