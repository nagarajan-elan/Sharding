CREATE DATABASE mydatabase1;
CREATE DATABASE mydatabase2;

\connect mydatabase0
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL DEFAULT ''
);

\connect mydatabase1
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL DEFAULT ''
);

\connect mydatabase2
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL DEFAULT ''
);