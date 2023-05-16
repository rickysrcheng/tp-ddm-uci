FROM postgres:15.1-alpine

LABEL author="Ricky Cheng"
LABEL description="Postgres for cs 223 project 1"
LABEL version="1.0"

COPY create.sql /docker-entrypoint-initdb.d/