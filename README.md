# Bus2Go-backend

> [!WARNING] 
> The application is still under development at the moment.
> Debug secrets may still be present, so use at your own risk

This is the backend application for the Bus2Go mobile application, 
available at [https://github.com/afg360/bus2go](https://github.com/afg360/bus2go).
It mainly acts as a proxy server. It hosts a copy of the database similar 
to the one defined in the client app, and periodically updates the 
real time data by making api calls to the transit agencies servers.
It also hosts sqlite versions of the schema, ready to be downloaded by the
client app.

## Initialising

Be sure to have docker compose installed on your system.
To start the server, simply run `docker compose --profile api up`
This will initialise a postgres database, and prepopulate it with the
latest data from providers if it doesn't exist already or is out of date.
It will also generate the various sqlite db files if not ready already.

## Configuring

Before starting the server, be sure to configure it correctly,
using a ".env" file in this directory.
The [./.env.example](./.env.example) file documents how the env vars should be setup

## Running

With the virtual environment still active, run `python3 entry.py`
to activate the server!

Documentation for the different api endpoints is available through [http://HOST:PORT/docs]()
