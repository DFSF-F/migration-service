#!/bin/bash
set -e

DB_NAME="${GREENPLUM_DB:-dwh}"
DB_USER="${GREENPLUM_USER:-gpadmin}"
DB_PASSWORD="${GREENPLUM_PASSWORD:-gpadmin}"

echo "Waiting for Greenplum TCP connection..."

until docker exec greenplum bash -lc "
PGPASSWORD=${DB_PASSWORD} /usr/local/greenplum-db-6.21.0/bin/psql \
-h 127.0.0.1 \
-p 5432 \
-U ${DB_USER} \
-d ${DB_NAME} \
-c 'select 1;' >/dev/null 2>&1
"
do
  sleep 3
done

echo "Greenplum is available. Applying base privileges and creating schemas..."

docker exec -u gpdb greenplum bash -lc "
source /usr/local/greenplum-db-6.21.0/greenplum_path.sh
export MASTER_DATA_DIRECTORY=/srv/gpmaster/gpsne-1

/usr/local/greenplum-db-6.21.0/bin/psql -d ${DB_NAME} -c \"grant create on database ${DB_NAME} to ${DB_USER};\"
/usr/local/greenplum-db-6.21.0/bin/psql -d ${DB_NAME} -c \"create schema if not exists raw authorization ${DB_USER};\"
/usr/local/greenplum-db-6.21.0/bin/psql -d ${DB_NAME} -c \"create schema if not exists dds authorization ${DB_USER};\"
/usr/local/greenplum-db-6.21.0/bin/psql -d ${DB_NAME} -c \"create schema if not exists dm authorization ${DB_USER};\"
/usr/local/greenplum-db-6.21.0/bin/psql -d ${DB_NAME} -c \"grant usage, create on schema raw to ${DB_USER};\"
/usr/local/greenplum-db-6.21.0/bin/psql -d ${DB_NAME} -c \"grant usage, create on schema dds to ${DB_USER};\"
/usr/local/greenplum-db-6.21.0/bin/psql -d ${DB_NAME} -c \"grant usage, create on schema dm to ${DB_USER};\"
"

echo "Greenplum base initialization completed successfully."