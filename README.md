# Bus2Go-backend
> [!WARNING]
> The application is still under development at the moment.

This is the backend application for the Bus2Go mobile application, available at [https://github.com/afg360/bus2go](https://github.com/afg360/bus2go).

It acts as a proxy server. It hosts a copy of the database similar to the one defined in the client app, and periodically updates the real time data by making api calls to the transit agencies servers.

## Initialising
Before doing anything, be sure to have a copy of the database required. Initialisation scripts are available at [https://github.com/afg360/Bus2Go/tree/master/scripts](https://github.com/afg360/Bus2Go/tree/master/scripts). You must move or copy the db file to the directory of this file.

You also must have the right api keys to the transit agencies to be able to make the api calls to their servers. Write them in a file named ".env", the syntax look like this:
```
<AGENCY-NAME>_TOKEN=<your-api-token>
```
For the moment only the stm api is enabled.
You may also have other configurations defined in the [./src/settings.py](./src/settings.py) file, such as the database
information (user, password, etc.).

The scripts will either create a python virtual environment, or a docker container. 
> [!WARNING]
> For the moment it doesnt seem to work properly.

Run the command (on linux)
```bash
chmod u+x init.sh
./init.sh
```
or (on windows)
```powershell
./init.ps1
```

If you didn't use docker, you can start the server by running the "start.sh" script on unix systems (be sure to set 
the correct execute permissions), by running the command (under the virtual environment):
```
./start.sh
```
