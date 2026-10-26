-- Runs once, on first start of an empty Postgres volume.
-- The warehouse database is created by POSTGRES_DB; Airflow gets its own metadata database.

CREATE DATABASE airflow;

-- Layered warehouse schemas: raw (append-only landing) -> staging (cleaned) -> marts (analytics).
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
