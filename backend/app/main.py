from fastapi import FastAPI
from dotenv import load_dotenv
import os
import psycopg

load_dotenv()

app = FastAPI(title="SQL-to-Natural-Language E-Commerce API")

DATABASE_URL = os.getenv("DATABASE_URL")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/metrics/monthly-revenue")
def monthly_revenue(limit: int = 12):
    sql = """
        SELECT month, revenue
        FROM analytics.monthly_revenue
        ORDER BY month
        LIMIT %s;
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

    return [{"month": str(r[0]), "revenue": float(r[1])} for r in rows]
