# Bus2Go-backend

> [!WARNING] 
> The application is still under development at the moment.

This is the backend application for the Bus2Go mobile application, 
available at [https://github.com/afg360/bus2go](https://github.com/afg360/bus2go).
It mainly acts as a proxy server. It hosts a copy of the database similar 
to the one defined in the client app, and periodically updates the 
real time data by making api calls to the transit agencies servers.
It also hosts sqlite databases holding static data, ready to be downloaded by the
client app.

## Initialising

Be sure to have docker compose installed on your system.
To start the server, simply run 
```
docker compose --profile api up
```
This will initialise a postgres database, and prepopulate it with the
latest data from providers if it doesn't exist already or is out of date.
It will also generate the various sqlite db files if not ready already.

## Configuring

Before starting the server, be sure to configure it correctly,
using a ".env" file in this directory.
The [./.env.example](./.env.example) file documents how the env vars should be setup

Since the mobile app only accepts encrypted connections, SSL must be configured in
the server side in order for the app to work properly. There are 2 methods to achieve this:

1. Directly using self-signed certificates in the project


2. Using a reverse-proxy

