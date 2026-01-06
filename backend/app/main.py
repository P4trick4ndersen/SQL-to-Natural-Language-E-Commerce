from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import psycopg

from datetime import date, datetime
from decimal import Decimal

from app.nl_router import generate_sql

app = FastAPI(title="SQL-to-Natural-Language E-Commerce API")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set. Check backend/.env")


class NLQuery(BaseModel):
    question: str


def json_safe(value):
    """Convert common Postgres/Python types into JSON-friendly values."""
    if isinstance(value, datetime):
        # If it's exactly midnight, return date-only string (nicer for charts)
        if (
            value.hour == 0 and value.minute == 0 and value.second == 0
            and value.microsecond == 0
        ):
            return value.date().isoformat()
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    return value


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


@app.post("/nl/query")
def nl_query(payload: NLQuery):
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    try:
        sql = generate_sql(question)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]

        json_rows = [
            {cols[i]: json_safe(r[i]) for i in range(len(cols))}
            for r in rows
        ]

        return {
            "question": question,
            "sql": sql,
            "columns": cols,
            "rows": json_rows,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL execution error: {e}")