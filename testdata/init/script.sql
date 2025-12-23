CREATE USER rep_user WITH REPLICATION PASSWORD 'secret';

CREATE EXTENSION "postgis";

CREATE TABLE users(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    location GEOMETRY(Point, 4326),
    details JSONB
);

ALTER TABLE users REPLICA IDENTITY FULL;

CREATE PUBLICATION my_publication FOR TABLE users;

