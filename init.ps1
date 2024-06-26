# Check if stm_info.db file exists
if (-not (Test-Path "./stm_info.db")) {
    echo "You did not initialise the database!"
    echo "Aborting"
    exit 1
}

py .\setup_map_table.py

if (!(Get-Command docker.exe -ErrorAction SilentlyContinue)) {
    echo "Docker is not installed. Creating a virtual environment locally"
	py -m venv venv
	./venv/Scripts/activate
	pip install -r requirements.txt
} else {
	docker build -t test-backend:latest .
	docker run -p 127.0.0.1:8000:8000 test-backend:latest
}



