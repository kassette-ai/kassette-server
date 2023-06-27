create database kassette;

CREATE USER kassette_user WITH PASSWORD 'password';

GRANT CONNECT ON DATABASE kassette TO kassette_user;

\connect kassette;

create schema events;




create database warehouse;
grant all on database warehouse to kassette_user;
\connect warehouse
GRANT ALL ON SCHEMA public TO kassette_user;


