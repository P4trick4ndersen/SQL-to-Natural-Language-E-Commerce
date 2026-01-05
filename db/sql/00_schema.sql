-- SQL-to-Natural-Language E-Commerce
-- Schema for Kaggle "Online Retail" style dataset (invoice line items)

BEGIN;

-- Separate schemas to keep things clean
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 0) Drop views FIRST (they depend on tables)
DROP VIEW IF EXISTS analytics.monthly_revenue;
DROP VIEW IF EXISTS analytics.invoice_summary;

-- 1) Drop tables (children first, then parents)
DROP TABLE IF EXISTS core.invoice_lines;
DROP TABLE IF EXISTS core.invoices;
DROP TABLE IF EXISTS core.products;
DROP TABLE IF EXISTS core.customers;

-- Staging table (safe to drop/recreate too)
DROP TABLE IF EXISTS staging.retail_raw;

-- 2) STAGING: raw CSV loaded as-is
CREATE TABLE staging.retail_raw (
  invoice_no    TEXT,
  stock_code    TEXT,
  description   TEXT,
  quantity      INTEGER,
  invoice_date  TIMESTAMP,
  unit_price    NUMERIC(10, 2),
  customer_id   INTEGER,
  country       TEXT
);

-- 3) CORE: normalized tables
CREATE TABLE core.customers (
  customer_id INTEGER PRIMARY KEY,
  country     TEXT,
  first_seen  TIMESTAMP,
  last_seen   TIMESTAMP
);

CREATE TABLE core.products (
  stock_code  TEXT PRIMARY KEY,
  description TEXT
);

CREATE TABLE core.invoices (
  invoice_no     TEXT PRIMARY KEY,
  invoice_date   TIMESTAMP NOT NULL,
  customer_id    INTEGER NULL REFERENCES core.customers(customer_id),
  country        TEXT,
  is_cancelled   BOOLEAN NOT NULL DEFAULT FALSE
);

-- IMPORTANT: use surrogate key to avoid duplicate composite-key conflicts
CREATE TABLE core.invoice_lines (
  line_id     BIGSERIAL PRIMARY KEY,
  invoice_no  TEXT NOT NULL REFERENCES core.invoices(invoice_no) ON DELETE CASCADE,
  stock_code  TEXT NOT NULL REFERENCES core.products(stock_code),
  quantity    INTEGER NOT NULL,
  unit_price  NUMERIC(10, 2) NOT NULL,
  line_total  NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- 4) ANALYTICS views for easier querying + Natural Language mapping
CREATE VIEW analytics.invoice_summary AS
SELECT
  i.invoice_no,
  i.invoice_date,
  i.customer_id,
  i.country,
  i.is_cancelled,
  SUM(l.line_total) AS invoice_total
FROM core.invoices i
JOIN core.invoice_lines l ON l.invoice_no = i.invoice_no
GROUP BY 1,2,3,4,5;

CREATE VIEW analytics.monthly_revenue AS
SELECT
  DATE_TRUNC('month', invoice_date) AS month,
  SUM(invoice_total) AS revenue
FROM analytics.invoice_summary
WHERE is_cancelled = FALSE
GROUP BY 1
ORDER BY 1;

COMMIT;