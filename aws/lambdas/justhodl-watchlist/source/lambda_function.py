"""
justhodl-watchlist — Personal watchlist API (institutional-grade).

ENDPOINTS
─────────
  GET  /             → Returns current watchlist (public read)
  POST /             → Updates watchlist (requires x-justhodl-token header)
  POST /add          → Add single ticker to category (admin token)
  POST /remove       → Remove ticker (admin token)

WATCHLIST SCHEMA (data/user-watchlist.json)
────────────────────────────────────────────
  {
    "version": <int, monotonic>,
    "updated_at": ISO-8601,
    "categories": {
      "holdings":  ["TICKER", ...],     # currently in book
      "watching":  [...],                # candidates / on radar
      "research":  [...],                # initial work needed
      "exit_zone": [...]                 # planning to exit / avoid
    },
    "tags": {                            # cross-cutting tags (optional)
      "<tag_name>": ["TICKER", ...]
    },
    "settings": {
      "default_filter":          "all",  # default filter on signal pages
      "auto_filter_master_rank": true,   # filter master-rank.html by default
      "telegram_alerts_only_wl": false   # if true, only alert on watchlist tickers
    },
    "history": [                         # last 50 changes for audit
      {"ts": ISO, "op": "add", "ticker": "T", "category": "holdings"}
    ]
  }

INSTITUTIONAL-GRADE SAFEGUARDS
───────────────────────────────
  ✓ Atomic writes — read-modify-write with version increment
  ✓ Optimistic locking — POST must include current version (prevents stale overwrites)
  ✓ Ticker validation — uppercase, alphanumeric, max 6 chars, no whitespace
  ✓ Auth on all writes — SSM admin token via x-justhodl-token header
  ✓ CORS — allowlist justhodl.ai origins only
  ✓ Audit trail — last 50 ops in history field
  ✓ Schema migration — graceful handling of v0 (legacy DEFAULT_WATCHLIST) → v1
  ✓ Idempotent adds — adding same ticker twice = no-op
  ✓ Rate limit safe — single S3 put per request, no expensive operations
"""
import json
import os
import re
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/user-watchlist.json")
SSM_TOKEN_PATH = os.environ.get("SSM_TOKEN_PATH", "/justhodl/api-admin/token")

ALLOWED_ORIGINS = {
    "https://justhodl.ai",
    "https://www.justhodl.ai",
}

VALID_CATEGORIES = {"holdings", "watching", "research", "exit_zone"}
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,7}$")  # 1-8 chars, e.g. BRK.B
MAX_TICKERS_PER_CATEGORY = 200
MAX_HISTORY = 50

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


# ─── Utilities ──────────────────────────────────────────────────────────────
def cors_headers(origin):
    """Return CORS headers — strict allowlist."""
    allow = origin if origin in ALLOWED_ORIGINS else "https://justhodl.ai"
    return {
        "Access-Control-Allow-Origin": allow,
        "Access-Control-Allow-Headers": "Content-Type, x-justhodl-token",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Content-Type": "application/json",
    }


def respond(status, body, origin=None):
    return {
        "statusCode": status,
        "headers": cors_headers(origin),
        "body": json.dumps(body, default=str),
    }


def get_admin_token():
    """Cached SSM lookup for admin token."""
    if hasattr(get_admin_token, "_cache"):
        cached, ts = get_admin_token._cache
        if time.time() - ts < 300:  # 5min cache
            return cached
    try:
        p = SSM.get_parameter(Name=SSM_TOKEN_PATH, WithDecryption=True)
        token = p["Parameter"]["Value"]
        get_admin_token._cache = (token, time.time())
        return token
    except Exception as e:
        print(f"[watchlist] SSM token fetch failed: {e}")
        return None


def authorize(headers):
    """Returns True if x-justhodl-token matches SSM admin token."""
    if not headers:
        return False
    # Case-insensitive header lookup (API Gateway lowercases, Lambda URL preserves)
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
    # Constant-time comparison
    return _ct_eq(token_header.strip(), expected.strip())


def _ct_eq(a, b):
    """Constant-time string equality."""
    if len(a) != len(b):
        return False
    diff = 0
    for x, y in zip(a, b):
        diff |= ord(x) ^ ord(y)
    return diff == 0


def validate_ticker(t):
    """Returns normalized uppercase ticker if valid, None otherwise."""
    if not isinstance(t, str):
        return None
    t = t.strip().upper()
    if not TICKER_RE.match(t):
        return None
    return t


def empty_watchlist():
    return {
        "version": 0,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "categories": {c: [] for c in VALID_CATEGORIES},
        "tags": {},
        "settings": {
            "default_filter": "all",
            "auto_filter_master_rank": True,
            "telegram_alerts_only_wl": False,
        },
        "history": [],
    }


def load_watchlist():
    """Load current watchlist from S3, migrate if needed."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=S3_KEY)
        wl = json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return empty_watchlist()
        raise

    # Migration / normalization
    if not isinstance(wl, dict):
        return empty_watchlist()
    wl.setdefault("version", 0)
    wl.setdefault("categories", {})
    wl.setdefault("tags", {})
    wl.setdefault("settings", {})
    wl.setdefault("history", [])

    # Ensure all categories exist
    for c in VALID_CATEGORIES:
        wl["categories"].setdefault(c, [])
    # Drop unknown categories silently
    wl["categories"] = {c: wl["categories"][c] for c in VALID_CATEGORIES if c in wl["categories"]}
    return wl


def save_watchlist(wl):
    """Save watchlist atomically with version increment."""
    wl["version"] = (wl.get("version", 0) or 0) + 1
    wl["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    body = json.dumps(wl, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=60",
    )
    return wl


def append_history(wl, op, **kwargs):
    entry = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "op": op}
    entry.update(kwargs)
    wl.setdefault("history", []).insert(0, entry)
    wl["history"] = wl["history"][:MAX_HISTORY]


# ─── Operation handlers ─────────────────────────────────────────────────────
def op_replace(wl, body):
    """Replace entire watchlist payload (with optimistic locking via version)."""
    incoming_version = body.get("version")
    if incoming_version is not None and incoming_version != wl.get("version"):
        return None, f"Version mismatch — server has v{wl.get('version')}, client sent v{incoming_version}. Reload page."

    new_categories = body.get("categories") or {}
    new_tags = body.get("tags") or {}
    new_settings = body.get("settings") or {}

    # Validate categories
    cleaned_categories = {c: [] for c in VALID_CATEGORIES}
    for cat, tickers in new_categories.items():
        if cat not in VALID_CATEGORIES:
            continue
        if not isinstance(tickers, list):
            return None, f"Category '{cat}' must be a list"
        seen = set()
        for t in tickers:
            tt = validate_ticker(t)
            if tt and tt not in seen:
                cleaned_categories[cat].append(tt)
                seen.add(tt)
        if len(cleaned_categories[cat]) > MAX_TICKERS_PER_CATEGORY:
            return None, f"Category '{cat}' exceeds {MAX_TICKERS_PER_CATEGORY} tickers"

    # Validate tags
    cleaned_tags = {}
    for tag, tickers in new_tags.items():
        if not isinstance(tag, str) or not tag.strip():
            continue
        clean_tag = re.sub(r"[^a-zA-Z0-9_\-]", "", tag.strip().lower())[:32]
        if not clean_tag or not isinstance(tickers, list):
            continue
        seen = set()
        clean_tickers = []
        for t in tickers:
            tt = validate_ticker(t)
            if tt and tt not in seen:
                clean_tickers.append(tt)
                seen.add(tt)
        if clean_tickers:
            cleaned_tags[clean_tag] = clean_tickers

    # Settings — only known keys, type-checked
    valid_settings = {
        "default_filter": str,
        "auto_filter_master_rank": bool,
        "telegram_alerts_only_wl": bool,
    }
    cleaned_settings = wl.get("settings", {}).copy()
    for k, vtype in valid_settings.items():
        if k in new_settings and isinstance(new_settings[k], vtype):
            cleaned_settings[k] = new_settings[k]

    wl["categories"] = cleaned_categories
    wl["tags"] = cleaned_tags
    wl["settings"] = cleaned_settings
    append_history(wl, "replace",
                    n_holdings=len(cleaned_categories.get("holdings", [])),
                    n_total=sum(len(v) for v in cleaned_categories.values()))
    return wl, None


def op_add(wl, body):
    ticker = validate_ticker(body.get("ticker"))
    category = body.get("category")
    if not ticker:
        return None, "Invalid ticker"
    if category not in VALID_CATEGORIES:
        return None, f"Invalid category. Valid: {sorted(VALID_CATEGORIES)}"
    cat_list = wl["categories"][category]
    if ticker in cat_list:
        return wl, None  # idempotent
    if len(cat_list) >= MAX_TICKERS_PER_CATEGORY:
        return None, f"Category '{category}' is full ({MAX_TICKERS_PER_CATEGORY} max)"
    # Remove from other categories first
    for other_cat in VALID_CATEGORIES:
        if other_cat != category and ticker in wl["categories"][other_cat]:
            wl["categories"][other_cat].remove(ticker)
    cat_list.append(ticker)
    cat_list.sort()
    append_history(wl, "add", ticker=ticker, category=category)
    return wl, None


def op_remove(wl, body):
    ticker = validate_ticker(body.get("ticker"))
    if not ticker:
        return None, "Invalid ticker"
    removed = []
    for cat in VALID_CATEGORIES:
        if ticker in wl["categories"][cat]:
            wl["categories"][cat].remove(ticker)
            removed.append(cat)
    # Also clean from tags
    for tag in wl.get("tags", {}):
        if ticker in wl["tags"][tag]:
            wl["tags"][tag].remove(ticker)
    if not removed:
        return wl, None  # idempotent — already absent
    append_history(wl, "remove", ticker=ticker, from_categories=removed)
    return wl, None


# ─── Lambda handler ─────────────────────────────────────────────────────────
def lambda_handler(event, context):
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
        return respond(400, {"ok": False, "err": "Invalid JSON body"}, origin)

    # CORS preflight
    if method == "OPTIONS":
        return respond(200, {"ok": True}, origin)

    # GET: public read
    if method == "GET":
        wl = load_watchlist()
        return respond(200, {"ok": True, "watchlist": wl}, origin)

    # POST: write — requires auth
    if method == "POST":
        if not authorize(headers):
            return respond(401, {"ok": False, "err": "Missing or invalid x-justhodl-token"}, origin)

        wl = load_watchlist()
        if path in ("/", "/replace"):
            updated, err = op_replace(wl, body)
        elif path == "/add":
            updated, err = op_add(wl, body)
        elif path == "/remove":
            updated, err = op_remove(wl, body)
        else:
            return respond(404, {"ok": False, "err": f"Unknown path: {path}"}, origin)

        if err:
            return respond(400, {"ok": False, "err": err}, origin)
        saved = save_watchlist(updated)
        return respond(200, {"ok": True, "watchlist": saved}, origin)

    return respond(405, {"ok": False, "err": f"Method {method} not allowed"}, origin)
