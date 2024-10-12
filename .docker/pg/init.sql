ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
ALTER SYSTEM SET max_worker_processes = 10;

CREATE TABLE actors (
    nconst TEXT PRIMARY KEY,
    primaryname TEXT,
    birthyear INTEGER,
    deathyear INTEGER,
    primaryprofession TEXT,
    knownfortitles TEXT
);

COPY actors FROM '/docker-entrypoint-initdb.d/actors.csv' CSV HEADER;