#!/bin/sh


if [ ! -f "./stm_info.db" ]; then
	echo "You did not initialise the database!"
	echo "Aborting"
	exit 1
fi

python3 ./setup_map_table.py

if ! command -v docker &> /dev/null; then
    #echo "Docker is not installed. Please install Docker to proceed."
    #exit 1
	echo "Docker is not installed. Creating a virtual environment locally"
	python3 -m venv venv
	source ./venv/bin/activate
	pip install -r requirements.txt
else
	#using legacy docker building...
	sudo docker build -t test-backend:latest .
	sudo docker run -p 127.0.0.1:8000:8000 test-backend:latest
fi

