#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: bash scripts/run_greenplum_query.sh \"<sql_query>\""
  exit 1
fi

QUERY="$1"

docker exec greenplum bash -lc "
PGPASSWORD=${GREENPLUM_PASSWORD:-gpadmin} \
/usr/local/greenplum-db-6.21.0/bin/psql \
-h 127.0.0.1 \
-p 5432 \
-U ${GREENPLUM_USER:-gpadmin} \
-d ${GREENPLUM_DB:-dwh} \
-c \"$QUERY\"
"