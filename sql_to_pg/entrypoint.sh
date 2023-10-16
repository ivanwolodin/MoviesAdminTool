#!/bin/sh
set -e
export DB_HOST=db
echo Starting postgres


until PGPASSWORD=$DB_PASSWORD psql -h db -U app -d movies_database -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  echo $DB_USER
  echo $DB_PASSWORD
  sleep 1
done
echo PostgreSQL started

echo Transeferring from sqlite to Postgres 
python ./load_data.py 
    

