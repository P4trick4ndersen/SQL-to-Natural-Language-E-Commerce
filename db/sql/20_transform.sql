BEGIN;

-- 0) Safety: remove empty junk
-- (Keeping the negative quantities because returns/cancellations are real signals)
-- Dropping rows missing key identifiers
-- (invoice_no and stock_code are required to model invoice lines)
-- NOTE: customer_id can be null in this dataset

-- 1) PRODUCTS: one row per stock_code
INSERT INTO core.products (stock_code, description)
SELECT
  stock_code,
  MAX(description) FILTER (WHERE description IS NOT NULL AND BTRIM(description) <> '') AS description
FROM staging.retail_raw
WHERE stock_code IS NOT NULL AND BTRIM(stock_code) <> ''
GROUP BY stock_code
ON CONFLICT (stock_code) DO UPDATE
SET description = COALESCE(EXCLUDED.description, core.products.description);

-- 2) CUSTOMERS: aggregate first/last seen, keep a country (most frequent / max)
INSERT INTO core.customers (customer_id, country, first_seen, last_seen)
SELECT
  customer_id,
  MAX(country) AS country,
  MIN(invoice_date) AS first_seen,
  MAX(invoice_date) AS last_seen
FROM staging.retail_raw
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ON CONFLICT (customer_id) DO UPDATE
SET country    = COALESCE(EXCLUDED.country, core.customers.country),
    first_seen = LEAST(core.customers.first_seen, EXCLUDED.first_seen),
    last_seen  = GREATEST(core.customers.last_seen, EXCLUDED.last_seen);

-- 3) INVOICES: header table (one row per invoice_no)
-- Cancellation flag: invoice numbers starting with 'C' in this dataset
INSERT INTO core.invoices (invoice_no, invoice_date, customer_id, country, is_cancelled)
SELECT
  invoice_no,
  MIN(invoice_date) AS invoice_date,
  MAX(customer_id)  AS customer_id,
  MAX(country)      AS country,
  (invoice_no LIKE 'C%') AS is_cancelled
FROM staging.retail_raw
WHERE invoice_no IS NOT NULL AND BTRIM(invoice_no) <> ''
GROUP BY invoice_no
ON CONFLICT (invoice_no) DO UPDATE
SET invoice_date = EXCLUDED.invoice_date,
    customer_id  = EXCLUDED.customer_id,
    country      = EXCLUDED.country,
    is_cancelled = EXCLUDED.is_cancelled;

-- 4) INVOICE LINES: detail table
-- We'll rebuild from scratch to keep it simple for the portfolio
TRUNCATE core.invoice_lines;

INSERT INTO core.invoice_lines (invoice_no, stock_code, quantity, unit_price)
SELECT
  invoice_no,
  stock_code,
  quantity,
  unit_price
FROM staging.retail_raw
WHERE invoice_no IS NOT NULL AND BTRIM(invoice_no) <> ''
  AND stock_code IS NOT NULL AND BTRIM(stock_code) <> '';

COMMIT;