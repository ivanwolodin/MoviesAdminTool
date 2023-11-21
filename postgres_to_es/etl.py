import uuid

from backoff import backoff
from db_connections import open_postgres_connection
from elasticsearch import helpers
from es_index import es
from logger import logger
from constants import (
    SELECT_PERSONS,
    SELECT_MOVIES_BY_PERSONS,
    SELECT_PERSONS_GENRES_FILM_WORKS_BY_MOVIES,
    SELECT_MOVIES_WITH_NO_PERSONS,
    LAST_MODIFIED_DATA,
    STATE_JSON_KEY,
)
from state_worker import state
from sql import sql_selects

from collections import defaultdict
from datetime import datetime


class ETL:
    def __init__(self) -> None:
        self.data_extractor_obj = self.Extractor()
        self.data_transformer = self.Transformer()
        self.data_loader = self.Loader()

    def run(self) -> None:
        raw_data = self.data_extractor_obj.collect_data()
        es_data = self.data_transformer.transform_data(raw_data)
        self.data_loader.load_to_es(es_data)

    class Extractor:
        def __init__(self) -> None:
            self._person_ids: list[uuid.UUID] = []
            self._movies_ids: list[uuid.UUID] = []
            self._last_modified_person: datetime

        @backoff()
        def _get_persons_ids(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    if state.get_state(STATE_JSON_KEY) is None:
                        self._last_modified_person: datetime = (
                            LAST_MODIFIED_DATA
                        )
                    else:
                        self._last_modified_person: datetime = (
                            datetime.fromisoformat(
                                state.get_state(STATE_JSON_KEY)
                            )
                        )

                    pg_cursor.execute(
                        sql_selects.get(SELECT_PERSONS).format(
                            self._last_modified_person
                        )
                    )
                    persons = pg_cursor.fetchall()
                    if not persons:
                        self._person_ids = []
                        return
                    self._person_ids = [person[0] for person in persons]

                    state.state = (
                        STATE_JSON_KEY,
                        persons[len(persons) - 1][1].isoformat(),
                    )

                except Exception as e:
                    logger.error(e)

        @backoff()
        def _get_movies_ids(self) -> None:
            with open_postgres_connection() as pg_cursor:
                try:
                    if not self._person_ids:
                        pg_cursor.execute(
                            sql_selects.get(SELECT_MOVIES_WITH_NO_PERSONS)
                        )
                    else:
                        pg_cursor.execute(
                            sql_selects.get(SELECT_MOVIES_BY_PERSONS).format(
                                tuple(set(self._person_ids))
                            )
                        )
                    movies_ids = pg_cursor.fetchall()

                    self._movies_ids = set(
                        [movie_id[0] for movie_id in movies_ids]
                    )

                except Exception as e:
                    logger.error(e)

        @backoff()
        def _get_merged_data(self) -> list[tuple]:
            with open_postgres_connection() as pg_cursor:
                try:
                    if not self._movies_ids:
                        return []
                    pg_cursor.execute(
                        sql_selects.get(
                            SELECT_PERSONS_GENRES_FILM_WORKS_BY_MOVIES
                        ).format(
                            tuple(self._movies_ids),
                        )
                    )
                    data = pg_cursor.fetchall()
                    return data

                except Exception as e:
                    logger.error(e)

        def collect_data(self) -> list[tuple]:
            self._get_persons_ids()
            self._get_movies_ids()
            return self._get_merged_data()

    class Transformer:
        def __init__(self) -> None:
            self._clear_aux_data()
            self._how_many_inserted: int = 0

        def _clear_aux_data(self) -> None:
            self._aux_dict = defaultdict(
                lambda: {
                    'imdb_rating': '',
                    'genre': '',
                    'title': '',
                    'description': '',
                    'actors_names': [],
                    'actors': [],
                    'writers_names': [],
                    'writers': [],
                    'director': '',
                }
            )

        def transform_data(self, data: list) -> list:
            if not data:
                return []

            for row in data:
                if self._aux_dict.get(row['fw_id']) is None:
                    self._aux_dict[row['fw_id']]['imdb_rating'] = row['rating']
                    self._aux_dict[row['fw_id']]['genre'] = row['name']
                    self._aux_dict[row['fw_id']]['title'] = row['title']
                    self._aux_dict[row['fw_id']]['description'] = row[
                        'description'
                    ]

                if (
                    row['role'] == 'actor'
                    and row['full_name']
                    not in self._aux_dict[row['fw_id']]['actors_names']
                ):
                    self._aux_dict[row['fw_id']]['actors_names'].append(
                        row['full_name']
                    )
                    self._aux_dict[row['fw_id']]['actors'].append(
                        {'id': row['id'], 'name': row['full_name']}
                    )
                if (
                    row['role'] == 'writer'
                    and row['full_name']
                    not in self._aux_dict[row['fw_id']]['writers_names']
                ):
                    self._aux_dict[row['fw_id']]['writers_names'].append(
                        row['full_name']
                    )
                    self._aux_dict[row['fw_id']]['writers'].append(
                        {'id': row['id'], 'name': row['full_name']}
                    )
                if (
                    row['role'] == 'director'
                    and row['full_name']
                    != self._aux_dict[row['fw_id']]['director']
                ):
                    self._aux_dict[row['fw_id']]['director'] = row['full_name']

            chunk = []
            for k, v in self._aux_dict.items():
                filmwork = {
                    'id': k,
                    'imdb_rating': v['imdb_rating'],
                    'genre': v['genre'],
                    'title': v['title'],
                    'description': v['description'],
                    'director': v['director'],
                    'actors_names': v['actors_names'],
                    'writers_names': v['writers_names'],
                    'actors': v['actors'],
                    'writers': v['writers'],
                }
                chunk.append(filmwork)

            self._clear_aux_data()
            self._how_many_inserted += len(chunk)
            logger.info(f'Total processed: {self._how_many_inserted}')
            return chunk

    class Loader:
        @backoff()
        def load_to_es(self, data) -> None:
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
