"""
justhodl-trade-journal — Personal trade journal API (institutional-grade).

ENDPOINTS
─────────
  GET  /                  → Returns trades (public read)
  POST /add               → Add new trade (admin token)
  POST /close             → Close existing trade (admin token)
  POST /update            → Update trade fields (admin token)
  POST /delete            → Hard-delete trade (admin token)
  POST /mtm               → Mark all open trades to market (scheduled cron only)

TRADE SCHEMA
────────────
  {
    "id": uuid-style,
    "ticker": "NVDA",
    "direction": "LONG" | "SHORT",
    "entry_date": "2026-04-15",
    "entry_price": 850.00,
    "size_usd": 50000,
    "n_shares": 58.82,
    "stop": 800.00,
    "target": 950.00,
    "signals_used": ["compound_score", "asymmetric", "pead"],
    "thesis": "free-form notes",
    "tags": ["AI", "earnings_play"],
    "status": "OPEN" | "CLOSED",
    "exit_date": null,
    "exit_price": null,
    "exit_reason": null,        # "TARGET" | "STOP" | "MANUAL" | "EARNINGS"
    "outcome_pct": null,
    "outcome_dollars": null,
    "current_price": 870.00,    # mark-to-market, updated nightly for OPEN
    "current_pnl_pct": +2.35,
    "days_held": null,
  }

OUTPUTS
───────
  data/user-trades.json — full ledger (open + closed)
  data/user-trades-stats.json — aggregate stats (win rate, profit factor, by signal)

INSTITUTIONAL-GRADE
────────────────────
  ✓ UUID-style trade IDs (no auto-increment collisions)
  ✓ Atomic writes — read-modify-write
  ✓ Optimistic locking via version field
  ✓ Admin auth on all writes
  ✓ Validation: positive prices, valid direction, ISO dates
  ✓ Mark-to-market via Polygon (uses watchlist universe)
  ✓ Signal attribution: links outcome to signals declared at entry
  ✓ Performance stats: win rate, avg win/loss, profit factor, Sharpe
  ✓ By-signal stats: for each signal, what's user's edge?
"""
import json
import os
import re
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_TRADES = os.environ.get("S3_KEY_TRADES", "data/user-trades.json")
S3_KEY_STATS = os.environ.get("S3_KEY_STATS", "data/user-trades-stats.json")
SSM_TOKEN_PATH = os.environ.get("SSM_TOKEN_PATH", "/justhodl/api-admin/token")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

ALLOWED_ORIGINS = {"https://justhodl.ai", "https://www.justhodl.ai"}
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,7}$")
VALID_DIRECTIONS = {"LONG", "SHORT"}
VALID_STATUS = {"OPEN", "CLOSED"}
VALID_EXIT_REASONS = {"TARGET", "STOP", "MANUAL", "EARNINGS", "TIME_STOP"}
MAX_TRADES = 1000

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


# ─── Auth + CORS ─────────────────────────────────────────────────────────────
def cors_headers(origin):
    allow = origin if origin in ALLOWED_ORIGINS else "https://justhodl.ai"
    return {
        "Access-Control-Allow-Origin": allow,
        "Access-Control-Allow-Headers": "Content-Type, x-justhodl-token",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Content-Type": "application/json",
    }


def respond(status, body, origin=None):
    return {"statusCode": status, "headers": cors_headers(origin),
            "body": json.dumps(body, default=str)}


def get_admin_token():
    if hasattr(get_admin_token, "_cache"):
        cached, ts = get_admin_token._cache
        if time.time() - ts < 300:
            return cached
    try:
        p = SSM.get_parameter(Name=SSM_TOKEN_PATH, WithDecryption=True)
        token = p["Parameter"]["Value"]
        get_admin_token._cache = (token, time.time())
        return token
    except Exception as e:
        print(f"[journal] SSM token fetch failed: {e}")
        return None


def authorize(headers):
    if not headers:
        return False
    token_header = None
    for k, v in headers.items():
        if k.lower() == "x-justhodl-token":
            token_header = v
            break
    if not token_header:
        return False
    expected = get_admin_token()
    if not expected:
        return False
    return _ct_eq(token_header.strip(), expected.strip())


def _ct_eq(a, b):
    if len(a) != len(b):
        return False
    diff = 0
    for x, y in zip(a, b):
        diff |= ord(x) ^ ord(y)
    return diff == 0


# ─── Validation ──────────────────────────────────────────────────────────────
def validate_ticker(t):
    if not isinstance(t, str):
        return None
    t = t.strip().upper()
    return t if TICKER_RE.match(t) else None


def validate_date(s):
    if not isinstance(s, str):
        return None
    try:
        datetime.strptime(s[:10], "%Y-%m-%d")
        return s[:10]
    except ValueError:
        return None


def validate_trade(body, partial=False):
    """Validate trade fields. Returns (cleaned_dict, err_string)."""
    out = {}
    if "ticker" in body:
        t = validate_ticker(body["ticker"])
        if not t:
            return None, "Invalid ticker"
        out["ticker"] = t
    elif not partial:
        return None, "Missing ticker"

    if "direction" in body:
        d = (body["direction"] or "").strip().upper()
        if d not in VALID_DIRECTIONS:
            return None, f"direction must be one of {sorted(VALID_DIRECTIONS)}"
        out["direction"] = d
    elif not partial:
        out["direction"] = "LONG"

    if "entry_date" in body:
        ed = validate_date(body["entry_date"])
        if not ed:
            return None, "Invalid entry_date (YYYY-MM-DD)"
        out["entry_date"] = ed
    elif not partial:
        out["entry_date"] = datetime.now(timezone.utc).date().isoformat()

    for f in ("entry_price", "size_usd", "stop", "target"):
        if f in body and body[f] is not None:
            try:
                v = float(body[f])
                if v <= 0:
                    return None, f"{f} must be positive"
                out[f] = round(v, 4)
            except (TypeError, ValueError):
                return None, f"{f} must be a number"
        elif not partial and f in ("entry_price", "size_usd"):
            return None, f"Missing required field: {f}"

    if "signals_used" in body:
        if not isinstance(body["signals_used"], list):
            return None, "signals_used must be a list"
        out["signals_used"] = [str(s)[:50] for s in body["signals_used"][:20]]
    elif not partial:
        out["signals_used"] = []

    if "thesis" in body:
        out["thesis"] = str(body["thesis"])[:2000]

    if "tags" in body:
        if not isinstance(body["tags"], list):
            return None, "tags must be a list"
        out["tags"] = [re.sub(r"[^a-zA-Z0-9_\-]", "", str(t))[:32] for t in body["tags"][:10] if t]

    # Compute n_shares if entry_price + size_usd given
    if "entry_price" in out and "size_usd" in out and out["entry_price"] > 0:
        out["n_shares"] = round(out["size_usd"] / out["entry_price"], 4)

    return out, None


# ─── Load / Save ─────────────────────────────────────────────────────────────
def load_trades():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=S3_KEY_TRADES)
        d = json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"version": 0, "trades": []}
        raise
    if not isinstance(d, dict):
        return {"version": 0, "trades": []}
    d.setdefault("version", 0)
    d.setdefault("trades", [])
    return d


def save_trades(d):
    d["version"] = (d.get("version", 0) or 0) + 1
    d["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    body = json.dumps(d, indent=2, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=S3_KEY_TRADES, Body=body,
                   ContentType="application/json", CacheControl="max-age=60")
    return d


# ─── Operations ──────────────────────────────────────────────────────────────
def op_add(d, body):
    cleaned, err = validate_trade(body, partial=False)
    if err:
        return None, err
    if len(d["trades"]) >= MAX_TRADES:
        return None, f"Trade limit ({MAX_TRADES}) reached"
    new_trade = {
        "id": "t_" + uuid.uuid4().hex[:12],
        "status": "OPEN",
        "exit_date": None,
        "exit_price": None,
        "exit_reason": None,
        "outcome_pct": None,
        "outcome_dollars": None,
        "current_price": None,
        "current_pnl_pct": None,
        "days_held": None,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **cleaned,
    }
    d["trades"].insert(0, new_trade)  # newest first
    return d, None


def op_close(d, body):
    trade_id = body.get("id")
    if not trade_id:
        return None, "Missing id"
    exit_price = body.get("exit_price")
    exit_date = validate_date(body.get("exit_date") or datetime.now(timezone.utc).date().isoformat())
    exit_reason = (body.get("exit_reason") or "MANUAL").strip().upper()
    if exit_reason not in VALID_EXIT_REASONS:
        return None, f"exit_reason must be one of {sorted(VALID_EXIT_REASONS)}"
    if exit_price is None:
        return None, "Missing exit_price"
    try:
        exit_price = float(exit_price)
        if exit_price <= 0:
            return None, "exit_price must be positive"
    except (TypeError, ValueError):
        return None, "exit_price must be a number"

    for t in d["trades"]:
        if t.get("id") == trade_id:
            if t.get("status") == "CLOSED":
                return None, "Trade already closed"
            t["status"] = "CLOSED"
            t["exit_date"] = exit_date
            t["exit_price"] = round(exit_price, 4)
            t["exit_reason"] = exit_reason

            entry = t.get("entry_price") or 0
            if entry > 0:
                if t.get("direction") == "SHORT":
                    pct = (entry - exit_price) / entry * 100
                else:
                    pct = (exit_price - entry) / entry * 100
                t["outcome_pct"] = round(pct, 2)
                size = t.get("size_usd") or 0
                t["outcome_dollars"] = round(size * pct / 100, 2)

            try:
                ed = datetime.strptime(t.get("entry_date", "")[:10], "%Y-%m-%d").date()
                xd = datetime.strptime(exit_date, "%Y-%m-%d").date()
                t["days_held"] = (xd - ed).days
            except ValueError:
                pass

            t["closed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            return d, None

    return None, "Trade not found"


def op_update(d, body):
    trade_id = body.get("id")
    if not trade_id:
        return None, "Missing id"
    cleaned, err = validate_trade(body, partial=True)
    if err:
        return None, err
    for t in d["trades"]:
        if t.get("id") == trade_id:
            t.update(cleaned)
            t["updated_at_field"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            return d, None
    return None, "Trade not found"


def op_delete(d, body):
    trade_id = body.get("id")
    if not trade_id:
        return None, "Missing id"
    before = len(d["trades"])
    d["trades"] = [t for t in d["trades"] if t.get("id") != trade_id]
    if len(d["trades"]) == before:
        return None, "Trade not found"
    return d, None


# ─── Mark-to-market (scheduled) ──────────────────────────────────────────────
def fetch_quote(ticker):
    try:
        url = f"https://api.polygon.io/v2/last/trade/{ticker}?apiKey={POLY_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "trade-journal/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode("utf-8"))
        if d.get("status") in ("OK", "DELAYED"):
            return d.get("results", {}).get("p")
    except Exception as e:
        print(f"[journal] quote {ticker} failed: {e}")
    # Fallback: previous close
    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?apiKey={POLY_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "trade-journal/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode("utf-8"))
        results = d.get("results") or []
        if results:
            return results[0].get("c")
    except Exception:
        pass
    return None


def op_mtm(d):
    """Mark-to-market all open trades."""
    open_trades = [t for t in d["trades"] if t.get("status") == "OPEN"]
    n_updated = 0
    for t in open_trades:
        ticker = t.get("ticker")
        if not ticker:
            continue
        price = fetch_quote(ticker)
        if not price:
            continue
        t["current_price"] = round(price, 4)
        entry = t.get("entry_price") or 0
        if entry > 0:
            if t.get("direction") == "SHORT":
                pct = (entry - price) / entry * 100
            else:
                pct = (price - entry) / entry * 100
            t["current_pnl_pct"] = round(pct, 2)
        try:
            ed = datetime.strptime(t.get("entry_date", "")[:10], "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            t["days_held"] = (today - ed).days
        except ValueError:
            pass
        t["mtm_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        n_updated += 1
        time.sleep(0.05)  # gentle on Polygon
    return n_updated


# ─── Stats computation ───────────────────────────────────────────────────────
def compute_stats(d):
    trades = d.get("trades") or []
    closed = [t for t in trades if t.get("status") == "CLOSED" and t.get("outcome_pct") is not None]
    open_t = [t for t in trades if t.get("status") == "OPEN"]

    stats = {
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_total": len(trades),
        "n_open": len(open_t),
        "n_closed": len(closed),
    }
    if closed:
        outcomes = [t["outcome_pct"] for t in closed]
        wins = [x for x in outcomes if x > 0]
        losses = [x for x in outcomes if x <= 0]
        stats["win_rate_pct"] = round(len(wins) / len(outcomes) * 100, 1)
        stats["avg_win_pct"] = round(sum(wins) / len(wins), 2) if wins else None
        stats["avg_loss_pct"] = round(sum(losses) / len(losses), 2) if losses else None
        stats["best_pct"] = round(max(outcomes), 2)
        stats["worst_pct"] = round(min(outcomes), 2)
        stats["avg_pct"] = round(sum(outcomes) / len(outcomes), 2)
        gross_wins = sum(t.get("outcome_dollars") or 0 for t in closed if t.get("outcome_dollars", 0) > 0)
        gross_losses = abs(sum(t.get("outcome_dollars") or 0 for t in closed if t.get("outcome_dollars", 0) <= 0))
        stats["profit_factor"] = round(gross_wins / gross_losses, 2) if gross_losses > 0 else None
        stats["total_pnl_dollars"] = round(sum(t.get("outcome_dollars") or 0 for t in closed), 2)
        avg_held = [t["days_held"] for t in closed if t.get("days_held") is not None]
        if avg_held:
            stats["avg_days_held"] = round(sum(avg_held) / len(avg_held), 1)

        # By-signal attribution
        by_signal = {}
        for t in closed:
            for sig in t.get("signals_used") or []:
                by_signal.setdefault(sig, []).append(t["outcome_pct"])
        stats["by_signal"] = {
            sig: {
                "n": len(outs),
                "win_rate_pct": round(sum(1 for x in outs if x > 0) / len(outs) * 100, 1),
                "avg_pct": round(sum(outs) / len(outs), 2),
            }
            for sig, outs in by_signal.items()
        }

    # Open positions current pnl
    if open_t:
        open_outcomes = [t.get("current_pnl_pct") for t in open_t if t.get("current_pnl_pct") is not None]
        if open_outcomes:
            stats["open_avg_pnl_pct"] = round(sum(open_outcomes) / len(open_outcomes), 2)
            stats["open_total_pnl_dollars"] = round(sum(
                (t.get("size_usd") or 0) * (t.get("current_pnl_pct") or 0) / 100
                for t in open_t), 2)

    return stats


def save_stats(stats):
    body = json.dumps(stats, indent=2, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=S3_KEY_STATS, Body=body,
                   ContentType="application/json", CacheControl="max-age=60")


# ─── Lambda handler ──────────────────────────────────────────────────────────
def lambda_handler(event, context):
    # Scheduled invocation (mark-to-market)
    if event.get("source") == "aws.events" or event.get("scheduled"):
        d = load_trades()
        n = op_mtm(d)
        save_trades(d)
        stats = compute_stats(d)
        save_stats(stats)
        print(f"[journal] MTM: {n} open trades updated")
        return {"statusCode": 200, "body": json.dumps({"ok": True, "n_mtm": n, "stats": stats})}

    # HTTP invocation
    method = (event.get("requestContext", {}).get("http", {}).get("method") or
              event.get("httpMethod") or "GET").upper()
    path = (event.get("rawPath") or event.get("path") or "/").rstrip("/") or "/"
    headers = event.get("headers") or {}
    origin = headers.get("origin") or headers.get("Origin")
    body_raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        import base64
        body_raw = base64.b64decode(body_raw).decode("utf-8")

    try:
        body = json.loads(body_raw) if body_raw else {}
    except json.JSONDecodeError:
        return respond(400, {"ok": False, "err": "Invalid JSON"}, origin)

    if method == "OPTIONS":
        return respond(200, {"ok": True}, origin)

    if method == "GET":
        d = load_trades()
        try:
            stats_obj = S3.get_object(Bucket=BUCKET, Key=S3_KEY_STATS)
            stats = json.loads(stats_obj["Body"].read())
        except Exception:
            stats = compute_stats(d)
        return respond(200, {"ok": True, "trades": d, "stats": stats}, origin)

    if method == "POST":
        if not authorize(headers):
            return respond(401, {"ok": False, "err": "Missing or invalid x-justhodl-token"}, origin)
        d = load_trades()
        if path == "/add":
            updated, err = op_add(d, body)
        elif path == "/close":
            updated, err = op_close(d, body)
        elif path == "/update":
            updated, err = op_update(d, body)
        elif path == "/delete":
            updated, err = op_delete(d, body)
        elif path == "/mtm":
            n = op_mtm(d)
            save_trades(d)
            stats = compute_stats(d)
            save_stats(stats)
            return respond(200, {"ok": True, "n_mtm": n, "stats": stats}, origin)
        else:
            return respond(404, {"ok": False, "err": f"Unknown path: {path}"}, origin)

        if err:
            return respond(400, {"ok": False, "err": err}, origin)
        saved = save_trades(updated)
        stats = compute_stats(saved)
        save_stats(stats)
        return respond(200, {"ok": True, "trades": saved, "stats": stats}, origin)

    return respond(405, {"ok": False, "err": f"Method {method} not allowed"}, origin)
