# syntax=docker/dockerfile:1

# for now it is very large, may need to change
FROM python:3

WORKDIR /app

COPY config main.py database.py gtfs_realtime_pb2.py stm_info.db /app/

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

EXPOSE 8000

CMD ["uvicorn", "main:app", "--port", "8000"]
