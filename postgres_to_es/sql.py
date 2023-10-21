sql_selects = {
    'persons': """SELECT id, modified
                    FROM content.person
                    WHERE modified > '{}'
                    ORDER BY modified
                    LIMIT 100;""",
    'movies_by_persons': """SELECT fw.id, fw.modified
                            FROM content.film_work fw
                            LEFT JOIN content.person_film_work pfw
                            ON pfw.film_work_id = fw.id
                            WHERE pfw.person_id IN {}
                            ORDER BY fw.modified
                            LIMIT 100;""",
    'persons_genres_film_works_by_movies': """SELECT
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
                            WHERE fw.id IN {} ; """,
}
