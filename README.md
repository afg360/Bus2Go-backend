# Bus2Go-backend

> [!WARNING] The application is still under development at the moment.

> [!WARNING] Although a Dockerfile exists, it hasn't been updated yet. Coming soon.

This is the backend application for the Bus2Go mobile application, 
available at [https://github.com/afg360/bus2go](https://github.com/afg360/bus2go).
It acts as a proxy server. It hosts a copy of the database similar 
to the one defined in the client app, and periodically updates the 
real time data by making api calls to the transit agencies servers.

## Initialising

Before doing anything, be sure to setup the python virtual environments
needed for the application and the utility scripts.
Simply run `python3 -m venv <your-virtual-env-name>` to create a new
python virtual environment. Once created, activate it.

On Linux: `source ./<your-virtual-env-name>/bin/activate`

On Windows: `./<your-virtual-env-name>/Scripts/activate`

Then install the required packages, using `pip install -r requirements.txt`

You then may run the script `python3 ./data/init_pg_db.py` to initialise the 
postgres databases on the system. You must run it from this 
directory. 
Once your postgres databases are initialised, you may create sqlite3
databases from the postgres databases using the 
`build_sqlite3_dbs.py` script. The databases will be located at
"./data".

## Configuring

Before starting the server, be sure to configure it correctly,
using a ".env" file in this directory.
The available fields are:

`HOST` -> A string value representing the ip address or domain name of the server.

`PORT` -> An integer value representing the open port for the server to listen from.

`SSL_CERT_PATH` -> Only necessary for when `DEBUG_MODE` is set to `False` (see below). A string value representing the relative path from here to where the SSL certificate path is (usually in the form foo.cert.pem).

`SSL_KEY_PATH` -> Only necessary for when `DEBUG_MODE` is set to `False` (see below). The relative path from here to where the SSL certificate private key path is (usually in the form foo.key.pem).

`STM_TOKEN` -> The api key for the stm real-time data apis.

`EXO_TOKEN` -> The api key for the exo real-time data apis.

`DB_1_NAME` -> The postgres database name for the STM database.

`DB_2_NAME` -> The postgres database name for the Exo database.

`DB_USERNAME` -> The username of the postgres databases.

`DB_PASSWORD` -> The password of the postgres databases.

`DEBUG_MODE` -> A python boolean to represent if the server should be in debug mode or not.

Example config:

```
HOST=127.0.0.1
PORT=8000
SSL_CERT_PATH=./my-ssl-certs/my_cert.cert.pem
SSL_KEY_PATH=./my-ssl-certs/my_key.key.pem
STM_TOKEN=m175tmt0k3n
EXO_TOKEN=m173x0t0k3n
DB_1_NAME=my_stm_database
DB_2_NAME=my_exo_database
DB_USERNAME=my_username
DB_PASSWORD=my_password
DEBUG_MODE=False
```

## Running

With the virtual environment still active, run `python3 entry.py`
to activate the server!

You may access documentation for the api endpoints by going to [http\[s\]://HOST:PORT/docs]()
