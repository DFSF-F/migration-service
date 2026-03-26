#!/bin/bash
set -e

echo "Waiting for Greenplum to become available..."

until PGPASSWORD="$GREENPLUM_PASSWORD" /usr/local/greenplum-db-6.21.0/bin/psql \
  -h greenplum \
  -p 5432 \
  -U "$GREENPLUM_USER" \
  -d "$GREENPLUM_DB" \
  -c "select 1;" >/dev/null 2>&1
do
  sleep 5
done

echo "Greenplum is available. Applying grants..."

PGPASSWORD="$GREENPLUM_PASSWORD" /usr/local/greenplum-db-6.21.0/bin/psql \
  -h greenplum \
  -p 5432 \
  -U "$GREENPLUM_USER" \
  -d "$GREENPLUM_DB" <<SQL
grant usage on schema public to $GREENPLUM_USER;
grant create on schema public to $GREENPLUM_USER;
grant all privileges on database $GREENPLUM_DB to $GREENPLUM_USER;
alter default privileges in schema public grant all on tables to $GREENPLUM_USER;
alter default privileges in schema public grant all on sequences to $GREENPLUM_USER;
alter default privileges in schema public grant all on functions to $GREENPLUM_USER;
SQL

echo "Greenplum grants applied successfully."