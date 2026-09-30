# syntax=docker/dockerfile:1

FROM python:3.13-slim

WORKDIR /app

#use this so that the requirements.txt file not be inside the docker container, only present during build time
RUN --mount=type=bind,source=./requirements.txt,target=/tmp/requirements.txt pip install -r /tmp/requirements.txt

#Setup bus2go databases
RUN mkdir -p ./data/downloads/{stm,exo}

COPY ./assets ./assets
COPY ./ssl-certs ./ssl-certs
COPY ./entry.py ./entry.py
COPY ./.env ./.env

#Build the core package at build time
RUN --mount=type=bind,source=./core,target=/tmp/core,rw pip install --no-cache-dir /tmp/core

COPY ./src ./src  

ENTRYPOINT ./entry.py
