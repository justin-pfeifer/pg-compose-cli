CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    uid uuid not null,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);