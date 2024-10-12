FROM postgres:16.3-alpine

COPY actors.csv /docker-entrypoint-initdb.d/actors.csv
COPY init.sql /docker-entrypoint-initdb.d/