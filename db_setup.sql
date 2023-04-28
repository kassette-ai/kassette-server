create database kassette;

CREATE USER kassette_user WITH PASSWORD 'password';

GRANT CONNECT ON DATABASE kassette TO kassette_user;

\connect kassette;

create schema events;

ALTER DEFAULT PRIVILEGES IN SCHEMA events
    GRANT ALL ON TABLES TO kassette_user;

CREATE TABLE queue_table (
                             id int not null primary key generated always as identity,
                             type varchar(255) not null,
                             source varchar(255) not null,
                             queue_time	timestamptz default now(),
                             payload	jsonb
);

CREATE TABLE transform (
    id int not null primary key generated always as identity,
    type varchar(255) not null,
    source varchar(255) not null,
    transform jsonb
)


