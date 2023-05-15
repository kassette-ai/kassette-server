create database camunda;

CREATE USER agent WITH PASSWORD 'password';

GRANT CONNECT ON DATABASE camunda TO agent;

\connect camunda;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON TABLES TO agent;

CREATE TABLE act_hi_actinst (
    id_ int not null primary key,
    act_name_ varchar(255),
    start_time_ varchar(255),
    end_time_ varchar(255),
    act_type_ varchar(255),
    assignee_ varchar(255)
)

insert into act_hi_actinst(id_, act_name_, start_time_, end_time_, act_type_, assignee_) values ( 3,'runtest', '100', '120', 'str', 'jim' );
