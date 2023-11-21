from constants import (
    SELECT_PERSONS,
    SELECT_MOVIES_BY_PERSONS,
    SELECT_PERSONS_GENRES_FILM_WORKS_BY_MOVIES,
    SELECT_MOVIES_WITH_NO_PERSONS,
)

sql_selects = {
    SELECT_PERSONS: """
                    SELECT id, modified
                    FROM content.person
                    WHERE modified > '{}'
                    ORDER BY modified
                    LIMIT 100
                    """,
    SELECT_MOVIES_BY_PERSONS: """
                            SELECT fw.id, fw.modified
                            FROM content.film_work fw
                            LEFT JOIN content.person_film_work pfw
                            ON pfw.film_work_id = fw.id
                            WHERE pfw.person_id IN {}
                            OR pfw.film_work_id IS NULL
                            ORDER BY fw.modified
                            LIMIT 500
                            """,
    SELECT_PERSONS_GENRES_FILM_WORKS_BY_MOVIES: """
                            SELECT
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
                            WHERE fw.id IN {} ; 
                            """,
    SELECT_MOVIES_WITH_NO_PERSONS: """
                            SELECT fw.id, fw.modified
                            FROM content.film_work fw
                            LEFT JOIN content.person_film_work pfw
                            ON pfw.film_work_id = fw.id
                            WHERE pfw.film_work_id IS NULL
                            ORDER BY fw.modified
                            LIMIT 500
                            """,
}
