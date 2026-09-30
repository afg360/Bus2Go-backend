-- To be used inside the Docker container to initialise the psql database
-- TODO Use env variables for username, password, database names, etc.

CREATE USER docker WITH PASSWORD 'password';
CREATE DATABASE stm_server OWNER docker;
CREATE DATABASE exo_server OWNER docker;

GRANT ALL PRIVILEGES ON DATABASE stm_server TO docker;
GRANT ALL PRIVILEGES ON DATABASE exo_server TO docker;

GRANT ALL ON SCHEMA public TO docker;
