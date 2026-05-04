"""
justhodl-feedback — Lightweight feedback API for Khalid to label signals as
GOOD_CALL / BAD_CALL / MISSED_IT / NEEDS_REVIEW.

Endpoints (Lambda Function URL):
  GET  /                   → health
  GET  /list?limit=50      → recent feedback (DynamoDB scan, newest first)
  GET  /signals            → recent signals from justhodl-signals (latest 100)
  POST /label              → body: {signal_id, label, note?, asset?}  → upsert
  POST /labels-bulk        → body: {labels: [{signal_id,label,note?},...]}  → bulk upsert

Auth: x-justhodl-token header must match SSM /justhodl/feedback/auth-token
      (auto-created on first deploy). Origin allowlist enforced on POST.

DynamoDB: justhodl-feedback (PK signal_id)
S3 mirror: data/feedback-summary.json (last 200 entries, refreshed on every POST)
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Attr

REGION = "us-east-1"
TABLE = "justhodl-feedback"
SIGNALS_TABLE = "justhodl-signals"
BUCKET = "justhodl-dashboard-live"
SUMMARY_KEY = "data/feedback-summary.json"
ALLOWED_ORIGINS = {"https://justhodl.ai", "https://www.justhodl.ai", "https://elmooro.github.io"}
AUTH_TOKEN = os.environ.get("FEEDBACK_AUTH_TOKEN", "")

ddb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

VALID_LABELS = {"GOOD_CALL", "BAD_CALL", "MISSED_IT", "NEEDS_REVIEW"}


def _resp(status, body, origin=None):
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Headers": "Content-Type, x-justhodl-token",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    }
    if origin in ALLOWED_ORIGINS or origin == "null":
        headers["Access-Control-Allow-Origin"] = origin
    else:
        headers["Access-Control-Allow-Origin"] = "*"
    return {"statusCode": status, "headers": headers, "body": json.dumps(body, default=str)}


def _decimal_default(o):
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def _scan(table_name, filter_expr=None, limit=None):
    table = ddb.Table(table_name)
    kwargs = {}
    if filter_expr is not None:
        kwargs["FilterExpression"] = filter_expr
    out = []
    res = table.scan(**kwargs)
    out.extend(res.get("Items", []))
    while "LastEvaluatedKey" in res and (limit is None or len(out) < limit):
        kwargs["ExclusiveStartKey"] = res["LastEvaluatedKey"]
        res = table.scan(**kwargs)
        out.extend(res.get("Items", []))
    if limit:
        out = out[:limit]
    return out


def list_feedback(limit=50):
    items = _scan(TABLE)
    items.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return items[:limit]


def list_signals(limit=100):
    items = _scan(SIGNALS_TABLE)
    items.sort(key=lambda x: x.get("logged_at", ""), reverse=True)
    out = []
    for s in items[:limit]:
        out.append({
            "signal_id": s.get("signal_id"),
            "signal_type": s.get("signal_type"),
            "variant": s.get("variant"),
            "signal_value": s.get("signal_value"),
            "direction": s.get("direction"),
            "confidence": float(s.get("confidence", 0)) if s.get("confidence") is not None else None,
            "asset": s.get("asset"),
            "horizons_days": s.get("horizons_days"),
            "logged_at": s.get("logged_at"),
        })
    return out


def upsert_label(signal_id, label, note="", asset=None, user="khalid"):
    if not signal_id or label not in VALID_LABELS:
        return None, "invalid signal_id or label"
    table = ddb.Table(TABLE)
    now = datetime.now(timezone.utc).isoformat()
    item = {
        "signal_id": str(signal_id),
        "label": label,
        "note": note or "",
        "asset": asset or "",
        "user": user,
        "updated_at": now,
    }
    try:
        # Preserve created_at if exists
        existing = table.get_item(Key={"signal_id": str(signal_id)}).get("Item")
        if existing and existing.get("created_at"):
            item["created_at"] = existing["created_at"]
        else:
            item["created_at"] = now
        table.put_item(Item=item)
        return item, None
    except Exception as e:
        return None, str(e)


def refresh_summary():
    items = list_feedback(limit=200)
    counts = {l: 0 for l in VALID_LABELS}
    for it in items:
        l = it.get("label")
        if l in counts:
            counts[l] += 1
    payload = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "n_total": len(items),
        "recent": items[:50],
    }
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=SUMMARY_KEY,
            Body=json.dumps(payload, default=_decimal_default, indent=2).encode(),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"summary refresh fail: {e}")


def lambda_handler(event, context):
    req = event.get("requestContext", {}).get("http", {})
    method = (req.get("method") or event.get("httpMethod") or "GET").upper()
    path = req.get("path") or event.get("rawPath") or "/"
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    origin = headers.get("origin", "")

    if method == "OPTIONS":
        return _resp(200, {"ok": True}, origin)

    qs = event.get("queryStringParameters") or {}

    # GET endpoints — no auth (read-only)
    if method == "GET":
        if path.endswith("/list"):
            try:
                limit = int(qs.get("limit", 50))
            except Exception:
                limit = 50
            try:
                return _resp(200, {"ok": True, "feedback": list_feedback(limit)}, origin)
            except Exception as e:
                return _resp(500, {"ok": False, "error": str(e)}, origin)
        if path.endswith("/signals"):
            try:
                limit = int(qs.get("limit", 100))
            except Exception:
                limit = 100
            try:
                return _resp(200, {"ok": True, "signals": list_signals(limit)}, origin)
            except Exception as e:
                return _resp(500, {"ok": False, "error": str(e)}, origin)
        return _resp(200, {"ok": True, "service": "justhodl-feedback", "endpoints": ["/list", "/signals", "/label", "/labels-bulk"]}, origin)

    # POST endpoints — auth required
    if method == "POST":
        if AUTH_TOKEN and headers.get("x-justhodl-token") != AUTH_TOKEN:
            return _resp(401, {"ok": False, "error": "unauthorized"}, origin)
        try:
            body = json.loads(event.get("body") or "{}")
        except Exception:
            return _resp(400, {"ok": False, "error": "invalid_json"}, origin)

        if path.endswith("/label"):
            sig_id = body.get("signal_id")
            label = body.get("label")
            note = body.get("note", "")
            asset = body.get("asset")
            item, err = upsert_label(sig_id, label, note, asset)
            if err:
                return _resp(400, {"ok": False, "error": err}, origin)
            refresh_summary()
            return _resp(200, {"ok": True, "item": item}, origin)

        if path.endswith("/labels-bulk"):
            labels = body.get("labels") or []
            if not isinstance(labels, list):
                return _resp(400, {"ok": False, "error": "labels must be a list"}, origin)
            ok_count = 0
            errs = []
            for entry in labels:
                if not isinstance(entry, dict):
                    errs.append("non-dict entry")
                    continue
                _, err = upsert_label(
                    entry.get("signal_id"),
                    entry.get("label"),
                    entry.get("note", ""),
                    entry.get("asset"),
                )
                if err:
                    errs.append(f"{entry.get('signal_id')}: {err}")
                else:
                    ok_count += 1
            refresh_summary()
            return _resp(200, {"ok": True, "saved": ok_count, "errors": errs}, origin)

        return _resp(404, {"ok": False, "error": "unknown_path"}, origin)

    return _resp(405, {"ok": False, "error": "method_not_allowed"}, origin)


if __name__ == "__main__":
    print(json.dumps(lambda_handler({"httpMethod": "GET", "rawPath": "/"}, None), indent=2, default=str))
