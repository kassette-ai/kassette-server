--
-- PostgreSQL database dump
--

-- Dumped from database version 15.2
-- Dumped by pg_dump version 15.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: service_catalogue; Type: TABLE; Schema: public; Owner: kassette_user
--

CREATE TABLE public.service_catalogue (
    id bigint NOT NULL,
    name character varying(255) NOT NULL,
    type character varying(255) NOT NULL,
    access character varying(255) NOT NULL,
    category character varying(255) NOT NULL,
    url text NOT NULL,
    notes text NOT NULL,
    metadata jsonb,
    iconurl text NOT NULL
);


ALTER TABLE public.service_catalogue OWNER TO kassette_user;

--
-- Name: service_catalogue_id_seq; Type: SEQUENCE; Schema: public; Owner: kassette_user
--

CREATE SEQUENCE public.service_catalogue_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.service_catalogue_id_seq OWNER TO kassette_user;

--
-- Name: service_catalogue_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: kassette_user
--

ALTER SEQUENCE public.service_catalogue_id_seq OWNED BY public.service_catalogue.id;


--
-- Name: service_catalogue id; Type: DEFAULT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.service_catalogue ALTER COLUMN id SET DEFAULT nextval('public.service_catalogue_id_seq'::regclass);


--
-- Data for Name: service_catalogue; Type: TABLE DATA; Schema: public; Owner: kassette_user
--

COPY public.service_catalogue (id, name, type, access, category, url, notes, metadata, iconurl) FROM stdin;
1	Camunda	Source	Agent	Workflow	https://camunda.com/	Camunda workflow management	{}	static/icons/camunda.png
2	Postgres	Destination	DBPolling	Database	https://www.postgresql.org	Write to Postgres relational Database	[{"name": "Host", "type": "text", "keyID": "host"}, {"name": "Port", "type": "text", "keyID": "port"}, {"name": "User", "type": "text", "keyID": "user"}, {"name": "Database", "type": "text", "keyID": "database"}, {"name": "Password", "type": "text", "keyID": "password"}, {"name": "SSL Mode", "type": "text", "keyID": "ssl_mode"}]	static/icons/postgres.png
3	PowerBI	Destination	Rest	Business Intelligence	https://powerbi.microsoft.com	Ingest your Data into PowerBI Platform	[{"name": "Endpoint", "type": "text", "keyID": "endpoint"}]	static/icons/powerbi.png
4	AWS S3	Destination	Rest	File Storage	https://aws.amazon.com/s3/	Export Data into AWS S3	[{"name": "Bucket Name", "type": "text", "keyID": "bucket"}, {"name": "AWS Region", "type": "text", "keyID": "region"}, {"name": "Access Key", "type": "text", "keyID": "accessKey"}, {"name": "Secret Key", "type": "text", "keyID": "secretKey"}]	static/icons/s3.png
11	Postgres	Source	DBPolling	Database	https://www.postgresql.org	Pull Data directly from Postgres instance	[{"name": "Host", "type": "text", "keyID": "host"}, {"name": "Port", "type": "text", "keyID": "port"}, {"name": "User", "type": "text", "keyID": "user"}, {"name": "Database", "type": "text", "keyID": "database"}, {"name": "Password", "type": "text", "keyID": "password"}, {"name": "SSL Mode", "type": "text", "keyID": "ssl_mode"}]	static/icons/postgres.png
12	Service Now	Destination	Rest	Process Management	https://www.servicenow.com	Send to Service Now	[{"name": "User", "type": "text", "keyID": "user"}, {"name": "Password", "type": "password", "keyID": "password"}]	static/icons/servicenow.png
10	SAP	Destination	Rest	Process Management	https://www.sap.com	Send Data to SAP	[{"name": "User", "type": "text", "keyID": "user"}, {"name": "Password", "type": "password", "keyID": "password"}]	static/icons/sap.png
9	Anaplan	Destination	Rest	Workforce Planning	https://www.anaplan.com	Send Data to Anaplan for analysis	[{"name": "User", "type": "text", "keyID": "user"}, {"name": "Password", "type": "password", "keyID": "password"}]	static/icons/anaplan.png
7	Kafka	Source	Rest	Queue	https://kafka.apache.org/	Pull Data from Kafka queue	[{"name": "User", "type": "text", "keyID": "user"}, {"name": "Password", "type": "password", "keyID": "password"}, {"name": "URL", "type": "text", "keyID": "url"}]	static/icons/kafka.png
8	AMQP	Source	AMQP	Queue	https://kassette.ai/	Consume events from AMQP-compatible Broker	[{"name": "User", "type": "text", "keyID": "user"}, {"name": "Password", "type": "password", "keyID": "password"}, {"name": "URL", "type": "text", "keyID": "url"}]	static/icons/amqp.png
5	Google Analytics	Source	Rest	Web Analytics	https://analytics.google.com/analytics/web	Pull Data from Google Analytics	[{"name": "Client ID", "type": "text", "keyID": "client_id"}, {"name": "Client Secret", "type": "text", "keyID": "client_secret"}]	static/icons/google.png
6	Javascript	Source	Rest	Web Analytics	https://kassette.ai/sdk/javascript	Send data from internal WEB Applications	[{"name": "Client ID", "type": "text", "keyID": "client_id"}, {"name": "Client Secret", "type": "text", "keyID": "client_secret"}]	static/icons/javascript.png
\.


--
-- Name: service_catalogue_id_seq; Type: SEQUENCE SET; Schema: public; Owner: kassette_user
--

SELECT pg_catalog.setval('public.service_catalogue_id_seq', 12, true);


--
-- Name: service_catalogue service_catalogue_pkey; Type: CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.service_catalogue
    ADD CONSTRAINT service_catalogue_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

