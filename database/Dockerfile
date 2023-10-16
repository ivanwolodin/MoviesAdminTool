FROM postgres:latest
WORKDIR /var/lib/postgresql
COPY create_fixtures.sql /docker-entrypoint-initdb.d/create_fixtures.sql
