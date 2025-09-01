#!/bin/sh
# wait-for-postgres.sh

set -e

host="$1"
shift
cmd="$@"

until PGPASSWORD=airflow psql -h "$host" -U "airflow" -c '\q'; do
  >&2 echo "Postgres no está disponible todavía - durmiendo"
  sleep 1
done

>&2 echo "Postgres está disponible - ejecutando comando"
exec $cmd