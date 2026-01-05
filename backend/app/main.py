from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
import os
import psycopg

from app.nl_router import classify_question, INTENTS


app = FastAPI(title="SQL-to-Natural-Language E-Commerce API")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set. Check backend/.env")


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
def nl_query(payload: dict):
    question = payload.get("question")
    if not question:
        raise HTTPException(status_code=400, detail="Missing 'question'")

    # 1) Model chooses intent + params
    parsed = classify_question(question)
    intent = parsed.get("intent")
    params = parsed.get("params", {})

    if intent not in INTENTS:
        raise HTTPException(status_code=400, detail=f"Unsupported intent: {intent}")

    # 2) Merge defaults with model params
    merged_params = {**INTENTS[intent]["defaults"], **params}
    sql = INTENTS[intent]["sql"]

    # 3) Run template SQL
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, merged_params)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]

    return {
        "question": question,
        "intent": intent,
        "params": merged_params,
        "rows": [dict(zip(cols, r)) for r in rows],
    }