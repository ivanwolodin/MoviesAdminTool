from dataclasses import fields

from models import (
    FilmWork,
    Genre,
    GenreFilmWork,
    Person,
    PersonFilmWork,
)

db_dataclasses_structure = (
    {
        'dataclass': FilmWork, 
        'order_by': 'created_at',
        'fields': [field.name for field in fields(FilmWork)],
        'sqlite_table': 'film_work',
        'postgres_table': 'film_work',
        'postgres_sql_upsert': """INSERT INTO content.film_work (
                                 id,
                                 title,
                                 description,
                                 creation_date,
                                 rating,
                                 type,
                                 created,
                                 modified
                                ) 
                                VALUES {0}
                                ON CONFLICT (id) DO NOTHING""",
        'string_pattern': '(%s, %s, %s, %s, %s, %s, %s, %s)',
        'ordered_keys': [
            'id', 
            'title', 
            'description', 
            'creation_date', 
            'rating', 
            'type', 
            'created_at', 
            'updated_at',
        ],
    },
    {
        'dataclass': Genre, 
        'order_by': 'created_at',
        'fields': [field.name for field in fields(Genre)],
        'sqlite_table': 'genre',
        'postgres_table': 'genre',
        'postgres_sql_upsert': """
                                INSERT INTO content.genre (
                                 id, 
                                 name, 
                                 description,
                                 created, 
                                 modified
                                ) 
                                VALUES {}
                                ON CONFLICT (id) DO NOTHING """,
        'string_pattern': '(%s, %s, %s, %s, %s)',
        'ordered_keys': [
            'id', 
            'name', 
            'description', 
            'created_at', 
            'updated_at',
        ],
    },
    {
        'dataclass': Person, 
        'order_by': 'created_at',
        'fields': [field.name for field in fields(Person)],
        'sqlite_table': 'person',
        'postgres_table': 'person',
        'postgres_sql_upsert': """
                                INSERT INTO content.person (
                                 id, 
                                 full_name, 
                                 created, 
                                 modified
                                ) 
                                VALUES {}
                                ON CONFLICT (id) DO NOTHING """,
        'string_pattern': '(%s, %s, %s, %s)',
        'ordered_keys': [
            'id', 
            'full_name', 
            'created_at', 
            'updated_at',
        ],
    },
    {
        'dataclass': GenreFilmWork, 
        'order_by': 'created_at',
        'fields': [field.name for field in fields(GenreFilmWork)],
        'sqlite_table': 'genre_film_work',
        'postgres_table': 'genre_film_work',
        'postgres_sql_upsert': """
                                INSERT INTO content.genre_film_work (
                                 id, 
                                 genre_id, 
                                 film_work_id,
                                 created
                                ) 
                                VALUES {}
                                ON CONFLICT (id) DO NOTHING """,
        'string_pattern': '(%s, %s, %s, %s)',
        'ordered_keys': [
            'id', 
            'genre_id', 
            'film_work_id', 
            'created_at',
        ],
    },
    {
        'dataclass': PersonFilmWork, 
        'order_by': 'created_at',
        'fields': [field.name for field in fields(PersonFilmWork)],
        'sqlite_table': 'person_film_work',
        'postgres_table': 'person_film_work',
        'postgres_sql_upsert': """
                                INSERT INTO content.person_film_work (
                                 id, 
                                 film_work_id, 
                                 person_id,
                                 created, 
                                 role
                                ) 
                                VALUES {}
                                ON CONFLICT (id) DO NOTHING """,
        'string_pattern': '(%s, %s, %s, %s, %s)',
        'ordered_keys': [
            'id', 
            'film_work_id', 
            'person_id', 
            'created_at',
            'role',
        ],
    },
)
