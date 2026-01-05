import os
import json
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Allowed intents -> SAFE SQL templates (no model-generated SQL)
INTENTS = {
    "monthly_revenue": {
        "sql": """
            SELECT month, revenue
            FROM analytics.monthly_revenue
            ORDER BY month
            LIMIT %(limit)s;
        """,
        "defaults": {"limit": 12},
    },
    "revenue_in_month": {
        "sql": """
            SELECT month, revenue
            FROM analytics.monthly_revenue
            WHERE month = %(month)s::timestamp
            LIMIT 1;
        """,
        "defaults": {"month": "2011-03-01 00:00:00"},
    },
    "top_countries": {
        "sql": """
            SELECT country, SUM(invoice_total) AS revenue
            FROM analytics.invoice_summary
            WHERE is_cancelled = FALSE
            GROUP BY country
            ORDER BY revenue DESC
            LIMIT %(limit)s;
        """,
        "defaults": {"limit": 10},
    },
}

SYSTEM_PROMPT = f"""
You are an intent classifier for analytics questions.
Return ONLY valid JSON with keys: intent, params.

Allowed intents: {list(INTENTS.keys())}

Rules:
- If user asks for revenue by month, use "monthly_revenue".
- If user asks for revenue in a specific month (e.g., March 2011), use "revenue_in_month"
  and set params.month to "YYYY-MM-01 00:00:00".
- If user asks about country revenue/top countries, use "top_countries".
- Always include params (can be empty).
- Never output SQL. Never output extra text.
"""

def classify_question(question: str) -> dict:
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
        response_format={"type": "json_object"},
    )

    content = resp.choices[0].message.content
    return json.loads(content)