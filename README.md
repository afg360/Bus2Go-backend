# Bus2Go-backend

> [!WARNING] 
> The application is still under development at the moment. Updates may break certain things.

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
```bash
docker compose --profile api up
```
This will initialise a postgres database, and prepopulate it with the
latest data from providers if it doesn't exist already or is out of date.
It will also generate the various sqlite db files if not ready already.

## Configuring

Before starting the server, be sure to configure it correctly,
using a ".env" file in this directory.
The [./.env.example](./.env.example) file documents how the env vars should be setup.

> A new [./api_keys.env.example](./api_keys.env.example) has been created to list all your api keys for the realtime
feature, but the feature is not yet implemented. The file is therefore not required to be populated yet.

Since the mobile app only accepts encrypted connections, SSL must be configured in
the server side in order for the app to work properly. There are 2 methods to achieve this when self-hosting
locally:

### 1. Directly using self-signed certificates in the project

Use openssl to generate a public-private key pair using the command:
```bash
openssl req -x509 -newkey rsa:4096 -keyout ./ssl-certs/private.key.pem -out ./ssl-certs/domain.cert.pem -sha256 -days 30 -nodes -subj "/O=<my-internal-ip>/CN=Bus2Go"
```

Replace "<my-internal-ip>" with the ip address of the computer from where you will be installing the server.

### 2. Using a reverse-proxy

Depending on the reverse-proxy you use, some (such as Caddy) automatically handle this, while others (like nginx) will need you to run the command to generate the self-signed certificates yourself.

More information on how to install and configure the server is available at [bus2go.app/guides/server](bus2go.app/guides/server).
