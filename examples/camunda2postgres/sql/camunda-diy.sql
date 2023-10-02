CREATE DATABASE workflow;
\c workflow
CREATE ROLE camundadiy WITH LOGIN PASSWORD 'camundadiy';
GRANT ALL PRIVILEGES on DATABASE workflow to camundadiy; 
GRANT ALL ON SCHEMA public TO camundadiy;
