BEGIN;

-- Drop in dependency order
DROP VIEW IF EXISTS analytics.customer_revenue;
DROP VIEW IF EXISTS analytics.product_revenue;
DROP VIEW IF EXISTS analytics.country_revenue;
DROP VIEW IF EXISTS analytics.daily_revenue;
DROP VIEW IF EXISTS analytics.weekly_revenue;

-- 1) Daily revenue (based on invoice_summary)
CREATE VIEW analytics.daily_revenue AS
SELECT
  DATE_TRUNC('day', invoice_date)::date AS day,
  SUM(invoice_total) AS revenue
FROM analytics.invoice_summary
WHERE is_cancelled = FALSE
GROUP BY 1
ORDER BY 1;

-- 2) Weekly revenue
CREATE VIEW analytics.weekly_revenue AS
SELECT
  DATE_TRUNC('week', invoice_date)::date AS week,
  SUM(invoice_total) AS revenue
FROM analytics.invoice_summary
WHERE is_cancelled = FALSE
GROUP BY 1
ORDER BY 1;

-- 3) Country revenue
CREATE VIEW analytics.country_revenue AS
SELECT
  country,
  SUM(invoice_total) AS revenue,
  COUNT(DISTINCT invoice_no) AS invoices
FROM analytics.invoice_summary
WHERE is_cancelled = FALSE
GROUP BY 1
ORDER BY revenue DESC;

-- 4) Product revenue (join invoice_lines -> invoices, exclude cancelled)
-- NOTE: This uses line_total; returns (negative quantities) will reduce revenue
CREATE VIEW analytics.product_revenue AS
SELECT
  l.stock_code,
  p.description,
  SUM(l.line_total) AS revenue,
  SUM(l.quantity) AS units_sold,
  COUNT(DISTINCT l.invoice_no) AS invoice_count
FROM core.invoice_lines l
JOIN core.invoices i ON i.invoice_no = l.invoice_no
LEFT JOIN core.products p ON p.stock_code = l.stock_code
WHERE i.is_cancelled = FALSE
GROUP BY 1,2
ORDER BY revenue DESC;

-- 5) Customer revenue (customer lifetime value-ish)
CREATE VIEW analytics.customer_revenue AS
SELECT
  i.customer_id,
  MAX(i.country) AS country,
  COUNT(DISTINCT i.invoice_no) AS invoices,
  SUM(l.line_total) AS revenue,
  MIN(i.invoice_date)::date AS first_purchase_date,
  MAX(i.invoice_date)::date AS last_purchase_date
FROM core.invoices i
JOIN core.invoice_lines l ON l.invoice_no = i.invoice_no
WHERE i.is_cancelled = FALSE
  AND i.customer_id IS NOT NULL
GROUP BY i.customer_id
ORDER BY revenue DESC;

COMMIT;