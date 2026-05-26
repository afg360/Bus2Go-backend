-- Do be used inside the Docker container to initialise the psql database

CREATE USER docker WITH PASSWORD 'password';
CREATE DATABASE bus2go OWNER docker;
CREATE DATABASE bus2go_exo OWNER docker;

GRANT ALL PRIVILEGES ON DATABASE bus2go TO docker;
GRANT ALL PRIVILEGES ON DATABASE bus2go_exo TO docker;

GRANT ALL ON SCHEMA public TO docker;
