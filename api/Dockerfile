FROM python:latest

WORKDIR /api

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt


ENV DB_HOST=postgres-container
ENV DB_PORT=5432
ENV DB_NAME=mydatabase
ENV DB_USER=myuser
ENV DB_PASSWORD=mypassword

COPY ./api api


CMD ["python", "api/main.py"]
