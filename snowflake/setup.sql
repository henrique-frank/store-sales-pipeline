-- Store Sales Pipeline — Snowflake Setup
-- Run this as SYSADMIN (or equivalent role with CREATE DATABASE privileges)

-- ============================================================
-- DATABASE + SCHEMAS
-- ============================================================

CREATE DATABASE IF NOT EXISTS SALES_DW;
USE DATABASE SALES_DW;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- ============================================================
-- FILE FORMAT + STAGE (Bronze layer)
-- ============================================================

USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT FF_CSV
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 0
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE STAGE STG_INBOX
    FILE_FORMAT = FF_CSV;

-- ============================================================
-- BRONZE TABLES
-- ============================================================

-- Stores raw (3 data columns + metadata)
CREATE TABLE IF NOT EXISTS BRONZE.STORES_RAW (
    store_group     STRING,
    store_token     STRING,
    store_name      STRING,
    batch_date      DATE,
    file_name       STRING,
    load_ts         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sales raw (7 data columns to handle source_id discrepancy + metadata)
CREATE TABLE IF NOT EXISTS BRONZE.SALES_RAW (
    store_token       STRING,
    transaction_id    STRING,
    receipt_token     STRING,
    transaction_time  STRING,
    amount            STRING,
    source_id         STRING,
    user_role         STRING,
    batch_date        DATE,
    file_name         STRING,
    load_ts           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Ingestion log for idempotency and audit
CREATE TABLE IF NOT EXISTS BRONZE.INGESTION_LOG (
    file_id       INTEGER AUTOINCREMENT,
    file_type     STRING NOT NULL,
    batch_date    DATE NOT NULL,
    file_name     STRING NOT NULL,
    content_hash  STRING NOT NULL,
    row_count     INTEGER DEFAULT 0,
    status        STRING DEFAULT 'LOADED',
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (content_hash)
);

-- ============================================================
-- WAREHOUSE
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
