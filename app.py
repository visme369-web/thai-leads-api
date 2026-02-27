from __future__ import annotations

import os
from functools import wraps
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)
VERSION = "1.0.0"

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = Path(
    os.getenv(
        "LEADS_CSV_PATH",
        "leads.csv",
    )
)


def _find_column(columns: list[str], candidates: list[str]) -> str | None:
    normalized = {c.strip().lower(): c for c in columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    out = out.fillna("")

    for col in out.columns:
        out[col] = out[col].astype(str).str.strip()

    return out


def _prepare_leads(df: pd.DataFrame) -> pd.DataFrame:
    columns = list(df.columns)

    company_col = _find_column(columns, ["Company Name", "Company", "Business Name"])
    industry_col = _find_column(columns, ["Industry", "Firm Type", "Type", "Category"])
    city_col = _find_column(columns, ["City", "Province", "State", "Location"])
    website_col = _find_column(columns, ["Website", "Web", "URL"])
    phone_col = _find_column(columns, ["Phone", "Telephone", "Mobile", "Contact Number"])
    email_col = _find_column(columns, ["Email", "E-mail", "Mail"])
    priority_col = _find_column(columns, ["Priority", "Lead Priority"])

    prepared = pd.DataFrame()
    prepared["company"] = df[company_col] if company_col else ""

    if industry_col:
        prepared["industry"] = df[industry_col]
    else:
        prepared["industry"] = "Interior Designer"

    prepared["city"] = df[city_col] if city_col else ""
    prepared["website"] = df[website_col] if website_col else ""
    prepared["phone"] = df[phone_col] if phone_col else ""
    prepared["email"] = df[email_col] if email_col else ""

    if priority_col:
        prepared["priority"] = df[priority_col].str.upper()
    else:
        prepared["priority"] = "LOW"

    prepared["priority"] = prepared["priority"].replace({"": "LOW"})

    for col in prepared.columns:
        prepared[col] = prepared[col].astype(str).str.strip()

    return prepared


try:
    RAW_DF = pd.read_csv(CSV_PATH)
    CLEAN_DF = _clean_dataframe(RAW_DF)
    LEADS_DF = _prepare_leads(CLEAN_DF)
except Exception as exc:
    raise RuntimeError(f"Failed to load CSV from {CSV_PATH}: {exc}") from exc


def require_api_key(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        key = request.headers.get("X-RapidAPI-Key") or request.args.get("api_key")
        if not key or not str(key).strip():
            return (
                jsonify({"error": "Missing API key. Provide X-RapidAPI-Key header or api_key query param."}),
                401,
            )
        return fn(*args, **kwargs)

    return wrapper


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-RapidAPI-Key"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": VERSION})


@app.route("/leads", methods=["GET"])
@require_api_key
def get_leads():
    industry = request.args.get("industry", "").strip().lower()
    city = request.args.get("city", "").strip().lower()
    priority = request.args.get("priority", "").strip().upper()

    limit_raw = request.args.get("limit", "10")
    offset_raw = request.args.get("offset", "0")

    try:
        limit = int(limit_raw)
        offset = int(offset_raw)
    except ValueError:
        return jsonify({"error": "limit and offset must be integers"}), 400

    if limit < 1 or limit > 50:
        return jsonify({"error": "limit must be between 1 and 50"}), 400

    if offset < 0:
        return jsonify({"error": "offset must be >= 0"}), 400

    if priority and priority not in {"HIGH", "MEDIUM", "LOW"}:
        return jsonify({"error": "priority must be one of HIGH, MEDIUM, LOW"}), 400

    filtered = LEADS_DF

    if industry:
        filtered = filtered[filtered["industry"].str.lower().str.contains(industry, na=False)]

    if city:
        filtered = filtered[filtered["city"].str.lower().str.contains(city, na=False)]

    if priority:
        filtered = filtered[filtered["priority"] == priority]

    total = int(len(filtered))
    page = filtered.iloc[offset : offset + limit]

    return (
        jsonify(
            {
                "total": total,
                "results": page.to_dict(orient="records"),
                "limit": limit,
                "offset": offset,
            }
        ),
        200,
    )


@app.route("/leads/count", methods=["GET"])
@require_api_key
def get_counts():
    by_priority = LEADS_DF["priority"].value_counts().to_dict()
    by_industry = LEADS_DF["industry"].value_counts().to_dict()

    return (
        jsonify(
            {
                "total": int(len(LEADS_DF)),
                "by_priority": by_priority,
                "by_industry": by_industry,
            }
        ),
        200,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
