#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
    CREATE DATABASE stm_postgres_database OWNER "$POSTGRES_USER";
    CREATE DATABASE exo_postgres_database OWNER "$POSTGRES_USER";

    GRANT ALL PRIVILEGES ON DATABASE stm_postgres_database TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON DATABASE exo_postgres_database TO "$POSTGRES_USER";
EOSQL

for db in stm_postgres_database exo_postgres_database; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<-EOSQL
        GRANT ALL ON SCHEMA public TO "$POSTGRES_USER";
EOSQL
done
