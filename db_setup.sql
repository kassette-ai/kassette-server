create database kassette;

CREATE USER kassette_user WITH PASSWORD 'password';

GRANT CONNECT ON DATABASE kassette TO kassette_user;

\connect kassette;

create schema events;






