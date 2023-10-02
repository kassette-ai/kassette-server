--
-- PostgreSQL database dump
--

-- Dumped from database version 15.2 (Debian 15.2-1.pgdg110+1)
-- Dumped by pg_dump version 15.3

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
-- Name: source; Type: TABLE; Schema: public; Owner: kassette_user
--

CREATE TABLE public.source (
    id bigint NOT NULL,
    name character varying(255) NOT NULL,
    service_id integer,
    write_key text NOT NULL,
    customer_id integer,
    config jsonb NOT NULL,
    status character varying(255) NOT NULL
);


ALTER TABLE public.source OWNER TO kassette_user;

--
-- Name: source_id_seq; Type: SEQUENCE; Schema: public; Owner: kassette_user
--

CREATE SEQUENCE public.source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.source_id_seq OWNER TO kassette_user;

--
-- Name: source_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: kassette_user
--

ALTER SEQUENCE public.source_id_seq OWNED BY public.source.id;


--
-- Name: source id; Type: DEFAULT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.source ALTER COLUMN id SET DEFAULT nextval('public.source_id_seq'::regclass);


--
-- Data for Name: source; Type: TABLE DATA; Schema: public; Owner: kassette_user
--

COPY public.source (id, name, service_id, write_key, customer_id, config, status) FROM stdin;
1	my camunda	1	9fa370f301bb5fbe170d6def04a1775a	1	{"schema": "{\\"table_name\\":\\"eventlog\\", \\"schema_fields\\":[{\\"name\\":\\"event_id\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"process_instance\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"task_name\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"task_type\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"task_seq\\",\\"type\\":\\"INT\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"process_id\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"process_name\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"assignee\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"task_start_time\\",\\"type\\":\\"TIMESTAMP\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"task_end_time\\",\\"type\\":\\"TIMESTAMP\\",\\"mode\\":\\"view\\",\\"primar y_key\\":false},{\\"name\\":\\"task_duration\\",\\"type\\":\\"INT\\",\\"mode\\":\\"view\\"},{\\"name\\":\\"business_key\\",\\"type\\":\\"STRING\\",\\"mode\\":\\"view\\"}]}"}	enabled
\.


--
-- Name: source_id_seq; Type: SEQUENCE SET; Schema: public; Owner: kassette_user
--

SELECT pg_catalog.setval('public.source_id_seq', 1, true);


--
-- Name: source source_pkey; Type: CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.source
    ADD CONSTRAINT source_pkey PRIMARY KEY (id);


--
-- Name: source source_service_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.source
    ADD CONSTRAINT source_service_id_fkey FOREIGN KEY (service_id) REFERENCES public.service_catalogue(id);


--
-- PostgreSQL database dump complete
--

--
-- PostgreSQL database dump
--

-- Dumped from database version 15.2 (Debian 15.2-1.pgdg110+1)
-- Dumped by pg_dump version 15.3

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
-- Name: destination; Type: TABLE; Schema: public; Owner: kassette_user
--

CREATE TABLE public.destination (
    id bigint NOT NULL,
    name character varying(255) NOT NULL,
    service_id integer,
    customer_id integer,
    config jsonb NOT NULL,
    status character varying(255) NOT NULL
);


ALTER TABLE public.destination OWNER TO kassette_user;

--
-- Name: destination_id_seq; Type: SEQUENCE; Schema: public; Owner: kassette_user
--

CREATE SEQUENCE public.destination_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.destination_id_seq OWNER TO kassette_user;

--
-- Name: destination_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: kassette_user
--

ALTER SEQUENCE public.destination_id_seq OWNED BY public.destination.id;


--
-- Name: destination id; Type: DEFAULT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.destination ALTER COLUMN id SET DEFAULT nextval('public.destination_id_seq'::regclass);


--
-- Data for Name: destination; Type: TABLE DATA; Schema: public; Owner: kassette_user
--

COPY public.destination (id, name, service_id, customer_id, config, status) FROM stdin;
1	my postgres	2	1	{"host": "postgres", "port": "5432", "user": "kassette_user", "schema": "{\\"table_name\\":\\"eventlog\\", \\"schema_fields\\":[{\\"name\\":\\"event_id\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"process_instance\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"task_name\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"task_type\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"task_seq\\",\\"type\\":\\"int\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"process_id\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"process_name\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"assignee\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"task_start_time\\",\\"type\\":\\"TIMESTAMP\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"task_end_time\\",\\"type\\":\\"TIMESTAMP\\",\\"mode\\":\\"view\\",\\"primar y_key\\":false},{\\"name\\":\\"task_duration\\",\\"type\\":\\"int\\",\\"mode\\":\\"view\\",\\"primary_key\\":false},{\\"name\\":\\"business_key\\",\\"type\\":\\"VARCHAR\\",\\"mode\\":\\"view\\",\\"primary_key\\":false}]}", "database": "warehouse", "password": "password", "ssl_mode": "disable", "batch_size": "100"}	enabled
\.


--
-- Name: destination_id_seq; Type: SEQUENCE SET; Schema: public; Owner: kassette_user
--

SELECT pg_catalog.setval('public.destination_id_seq', 1, true);


--
-- Name: destination destination_pkey; Type: CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.destination
    ADD CONSTRAINT destination_pkey PRIMARY KEY (id);


--
-- Name: destination destination_service_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.destination
    ADD CONSTRAINT destination_service_id_fkey FOREIGN KEY (service_id) REFERENCES public.service_catalogue(id);


--
-- PostgreSQL database dump complete
--

--
-- PostgreSQL database dump
--

-- Dumped from database version 15.2 (Debian 15.2-1.pgdg110+1)
-- Dumped by pg_dump version 15.3

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
-- Name: connection; Type: TABLE; Schema: public; Owner: kassette_user
--

CREATE TABLE public.connection (
    id bigint NOT NULL,
    source_id integer,
    destination_id integer,
    transforms jsonb NOT NULL
);


ALTER TABLE public.connection OWNER TO kassette_user;

--
-- Name: connection_id_seq; Type: SEQUENCE; Schema: public; Owner: kassette_user
--

CREATE SEQUENCE public.connection_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.connection_id_seq OWNER TO kassette_user;

--
-- Name: connection_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: kassette_user
--

ALTER SEQUENCE public.connection_id_seq OWNED BY public.connection.id;


--
-- Name: connection id; Type: DEFAULT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.connection ALTER COLUMN id SET DEFAULT nextval('public.connection_id_seq'::regclass);


--
-- Data for Name: connection; Type: TABLE DATA; Schema: public; Owner: kassette_user
--

COPY public.connection (id, source_id, destination_id, transforms) FROM stdin;
1	1	1	[]
\.


--
-- Name: connection_id_seq; Type: SEQUENCE SET; Schema: public; Owner: kassette_user
--

SELECT pg_catalog.setval('public.connection_id_seq', 1, true);


--
-- Name: connection connection_pkey; Type: CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.connection
    ADD CONSTRAINT connection_pkey PRIMARY KEY (id);


--
-- Name: connection connection_destination_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.connection
    ADD CONSTRAINT connection_destination_id_fkey FOREIGN KEY (destination_id) REFERENCES public.destination(id);


--
-- Name: connection connection_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: kassette_user
--

ALTER TABLE ONLY public.connection
    ADD CONSTRAINT connection_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.source(id);


--
-- PostgreSQL database dump complete
--

