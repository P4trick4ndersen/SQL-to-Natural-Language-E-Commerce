import os
import re

from openai import OpenAI
import sqlglot
from sqlglot import exp

# --- OpenAI client ---
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY not found. Put it in backend/.env and restart uvicorn.")

client = OpenAI(api_key=api_key)

# --- Safeguards ---
ALLOWED_TABLES = {
    "analytics.invoice_summary",
    "analytics.monthly_revenue",
    "core.invoice_lines",
    "core.invoices",
    "core.products",
    "core.customers",
    "staging.retail_raw",
    "analytics.daily_revenue",
    "analytics.weekly_revenue",
    "analytics.country_revenue",
    "analytics.product_revenue",
    "analytics.customer_revenue",
}

MAX_LIMIT = 200

SCHEMA_HINT = """
Database objects you can query (Postgres):

analytics.invoice_summary(
  invoice_no, invoice_date, customer_id, country, is_cancelled, invoice_total
)

analytics.monthly_revenue(
  month, revenue
)

core.invoices(
  invoice_no, invoice_date, customer_id, country, is_cancelled
)

core.invoice_lines(
  invoice_no, stock_code, quantity, unit_price, line_total
)

core.products(
  stock_code, description
)

core.customers(
  customer_id, country, first_seen, last_seen
)

staging.retail_raw(
  invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id, country
)

Rules:
- Only generate a single SELECT query.
- Do NOT use INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/COPY/TRUNCATE.
- Use schema-qualified names like analytics.invoice_summary
- Prefer analytics.invoice_summary for revenue/order-level questions.
- Prefer using analytics.daily_revenue / weekly_revenue / monthly_revenue for time-series revenue questions.
- Prefer analytics.product_revenue for product ranking questions.
- Prefer analytics.customer_revenue for customer spending questions.
- Prefer analytics.country_revenue for country questions.

Time grouping rules (IMPORTANT):
- For day grouping, return a DATE column:
  * Use DATE_TRUNC('day', invoice_date)::date AS day  OR  invoice_date::date AS day
- For week grouping, use DATE_TRUNC('week', invoice_date)::date AS week
- For month grouping, use DATE_TRUNC('month', invoice_date)::date AS month
- Avoid returning timestamps for time buckets unless the user explicitly asks for timestamps.

- Always include a LIMIT (<= 200).
"""

SYSTEM_PROMPT = f"""
You write safe PostgreSQL SELECT queries.

{SCHEMA_HINT}

Return ONLY the SQL query text. No markdown. No explanation.
"""


def _extract_table_refs(parsed: exp.Expression) -> set[str]:
    tables = set()
    for t in parsed.find_all(exp.Table):
        name = t.name
        schema = t.db
        if schema:
            tables.add(f"{schema}.{name}".lower())
        else:
            tables.add(name.lower())
    return tables


def validate_sql(sql: str) -> str:
    sql = sql.strip().strip(";").strip()

    banned = r"\b(insert|update|delete|drop|alter|create|truncate|copy|grant|revoke)\b"
    if re.search(banned, sql, flags=re.IGNORECASE):
        raise ValueError("Only SELECT queries are allowed.")

    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        raise ValueError("SQL could not be parsed. Please rephrase your question.")

    # Ensure it's SELECT (or contains a SELECT)
    if not isinstance(parsed, exp.Select) and not parsed.find(exp.Select):
        raise ValueError("Only SELECT queries are allowed.")

    refs = _extract_table_refs(parsed)

    # Require schema-qualified tables
    for ref in refs:
        if "." not in ref:
            raise ValueError(
                f"Use schema-qualified tables (e.g., analytics.invoice_summary). Found: {ref}"
            )

    # Whitelist tables/views
    for ref in refs:
        if ref not in ALLOWED_TABLES:
            raise ValueError(f"Table/view not allowed: {ref}")

    # Enforce LIMIT
    limit_expr = parsed.args.get("limit")
    if limit_expr is None:
        sql = f"{sql}\nLIMIT {MAX_LIMIT}"
    else:
        # If LIMIT is present, require it be a simple integer and clamp
        try:
            n = int(limit_expr.expression.name)
            if n > MAX_LIMIT:
                sql = re.sub(r"(?i)\blimit\s+\d+\b", f"LIMIT {MAX_LIMIT}", sql)
        except Exception:
            raise ValueError("LIMIT must be a simple integer <= 200.")

    return sql


def generate_sql(question: str) -> str:
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
    )

    sql = resp.choices[0].message.content.strip()
    return validate_sql(sql)