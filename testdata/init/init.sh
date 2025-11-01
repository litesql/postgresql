#!/bin/bash
set -e

echo "host replication postgres 0.0.0.0/32 md5" >> /var/lib/postgresql/data/pg_hba.conf

echo "wal_level=logical" >> /var/lib/postgresql/data/postgresql.conf
echo "max_wal_senders=4" >> /var/lib/postgresql/data/postgresql.conf
echo "max_replication_slots=10" >> /var/lib/postgresql/data/postgresql.conf

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER replication_user WITH REPLICATION PASSWORD 'changeit';

EOSQL