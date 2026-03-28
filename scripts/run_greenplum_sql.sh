#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: bash scripts/run_greenplum_sql.sh <sql_file_path>"
  exit 1
fi

SQL_FILE="$1"

docker cp "$SQL_FILE" greenplum:/tmp/run.sql

docker exec greenplum bash -lc "
PGPASSWORD=${GREENPLUM_PASSWORD:-gpadmin} \
/usr/local/greenplum-db-6.21.0/bin/psql \
-h 127.0.0.1 \
-p 5432 \
-U ${GREENPLUM_USER:-gpadmin} \
-d ${GREENPLUM_DB:-dwh} \
-f /tmp/run.sql
"