"""justhodl-tv-notes-crawler — Autonomous TradingView notes harvester.

Runs on schedule (daily 06:00 UTC) using only your TradingView session
cookie from SSM. No browser, no F12, no manual steps. Hits TV's internal
REST API exactly as your browser does, harvests every note across every
watchlist and chart layout, and writes directly to your Brain.

SSM parameters read:
  /justhodl/tradingview/sessionid      (SecureString — your TV session cookie)
  /justhodl/tvnotes/ingest-token       (SecureString — ingest lambda auth)
  /justhodl/brain/uid                  (String — brain UUID)

Output: data/tradingview-notes.json (mirror) + brain upsert
"""
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

S3  = boto3.client("s3",  region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")

BUCKET      = "justhodl-dashboard-live"
MIRROR_KEY  = "data/tradingview-notes.json"
STATUS_KEY  = "data/tv-crawler-status.json"
TV_BASE     = "https://www.tradingview.com"
MAX_NOTES   = 5000
THROTTLE_S  = 0.25   # seconds between TV API calls — be gentle

# ── SSM helpers ────────────────────────────────────────────────────────────
def _ssm(name, env_key=None):
    if env_key:
        v = os.environ.get(env_key)
        if v:
            return v
    try:
        return SSM.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except SSM.exceptions.ParameterNotFound:
        return None
    except Exception as e:
        print("[ssm] %s: %s" % (name, e))
        return None

# ── TV HTTP client ──────────────────────────────────────────────────────────
def tv_get(path, session, params=None, timeout=20):
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    url = TV_BASE + path + qs
    req = urllib.request.Request(url, headers={
        "Cookie":           "sessionid=%s" % session,
        "User-Agent":       ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/124.0.0.0 Safari/537.36"),
        "Accept":           "application/json, text/plain, */*",
        "Accept-Language":  "en-US,en;q=0.9",
        "Referer":          "https://www.tradingview.com/",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":           "https://www.tradingview.com",
    })
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                ct = r.headers.get("Content-Type", "")
                raw = r.read().decode("utf-8", "replace")
                if "json" in ct or raw.lstrip().startswith("{") or raw.lstrip().startswith("["):
                    return json.loads(raw), r.getcode()
                return None, r.getcode()
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                print("[tv] %s -> HTTP %d (session expired?)" % (path, e.code))
                return None, e.code
            if e.code == 429:
                print("[tv] rate-limited on %s, sleeping 15s" % path)
                time.sleep(15)
            elif attempt < 2:
                time.sleep(2 + attempt * 2)
        except Exception as e_:
            if attempt == 2:
                print("[tv] %s error: %s" % (path, e_))
            else:
                time.sleep(2 + attempt)
    return None, None

# ── Note normalizer ─────────────────────────────────────────────────────────
def _note_id(sym, ts, text):
    raw = "%s|%s|%s" % (sym, ts, str(text)[:160])
    return "tv-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

def norm(obj, sym_hint=None):
    """Return a normalised note dict or None."""
    if not isinstance(obj, dict):
        return None
    text = (obj.get("text") or obj.get("note") or obj.get("content") or
            obj.get("body") or obj.get("description") or "")
    text = str(text).strip()
    if len(text) < 3:
        return None
    sym = (obj.get("symbol") or obj.get("ticker") or obj.get("symbol_full") or
           obj.get("s") or sym_hint or "UNTAGGED")
    sym = str(sym).upper()[:30]
    title = str(obj.get("title") or obj.get("name") or "")[:200]
    created = obj.get("created") or obj.get("created_at") or obj.get("ts")
    updated = obj.get("updated") or obj.get("updated_at")
    for t in (created, updated):
        if isinstance(t, str):
            try:
                created = int(datetime.fromisoformat(
                    t.replace("Z", "+00:00")).timestamp() * 1000)
                break
            except Exception:
                pass
    if not isinstance(created, (int, float)):
        created = int(time.time() * 1000)
    body = "[TV:%s] " % sym
    if title and title.lower() not in text.lower():
        body += title + " — "
    body += text[:7900]
    nid = _note_id(sym, created, text)
    return {"id": nid, "symbol": sym, "text": body, "title": title,
            "created": int(created), "source": "tradingview-crawler"}

def mine_all(obj, sym_hint=None, depth=0):
    """Recursively extract note-shaped objects from any JSON structure."""
    results = []
    if not obj or depth > 8:
        return results
    if isinstance(obj, list):
        for item in obj:
            results.extend(mine_all(item, sym_hint, depth + 1))
        return results
    if not isinstance(obj, dict):
        return results
    sym = (obj.get("symbol") or obj.get("ticker") or obj.get("s") or sym_hint)
    has_text = any(obj.get(k) for k in ("text", "note", "content", "body", "description"))
    has_id   = any(obj.get(k) for k in ("id", "created", "created_at", "updated_at"))
    if has_text and has_id:
        n = norm(obj, sym_hint)
        if n:
            results.append(n)
    for v in obj.values():
        if isinstance(v, (dict, list)):
            results.extend(mine_all(v, sym or sym_hint, depth + 1))
    return results

# ── Step 1: get user profile ────────────────────────────────────────────────
def get_username(session):
    for path in ("/api/v2/user/", "/api/v1/user/"):
        data, code = tv_get(path, session)
        if data:
            return (data.get("username") or data.get("user", {}).get("username") or
                    data.get("id") or data.get("user_id"))
    return None

# ── Step 2: enumerate watchlist symbols ─────────────────────────────────────
def get_watchlist_symbols(session):
    symbols = set()
    endpoints = [
        ("/api/v2/lists/",          {"limit": 100}),
        ("/lists/",                 {}),
        ("/api/v1/lists/",          {}),
        ("/api/v2/lists/",          {"include_symbols": 1}),
        ("/api/v2/watchlists/",     {}),
    ]
    for path, params in endpoints:
        data, code = tv_get(path, session, params)
        if not data:
            time.sleep(THROTTLE_S)
            continue
        arr = (data.get("data") or data.get("lists") or data.get("watchlists") or
               data.get("results") or (data if isinstance(data, list) else []))
        for lst in (arr if isinstance(arr, list) else []):
            if not isinstance(lst, dict):
                continue
            items = (lst.get("symbols") or lst.get("items") or
                     lst.get("data") or lst.get("list_symbols") or [])
            for it in (items if isinstance(items, list) else []):
                s = it if isinstance(it, str) else (
                    it.get("symbol") or it.get("s") or it.get("ticker"))
                if s and len(str(s)) < 25:
                    symbols.add(str(s).upper())
        if symbols:
            break
        time.sleep(THROTTLE_S)
    print("[crawler] watchlist symbols: %d" % len(symbols))
    return list(symbols)

# ── Step 3: bulk notes pull (no symbol filter) ───────────────────────────────
BULK_PATHS = [
    ("/note-manager/api/notes/",   {"limit": 5000}),
    ("/note-manager/api/notes/",   {"page_size": 5000}),
    ("/api/v1/text_notes/",        {"limit": 5000}),
    ("/api/v1/text-notes/",        {"limit": 5000}),
    ("/textnotes/list/",           {}),
    ("/api/v2/notes/",             {"limit": 5000}),
    ("/note-manager/api/",         {}),
    ("/api/v2/chart-notes/",       {"limit": 5000}),
]

def pull_bulk_notes(session):
    notes = []
    for path, params in BULK_PATHS:
        data, code = tv_get(path, session, params)
        if data:
            found = mine_all(data)
            if found:
                notes.extend(found)
                print("[crawler] bulk %s -> %d notes" % (path, len(found)))
                break  # one working endpoint is enough
        time.sleep(THROTTLE_S)
    return notes

# ── Step 4: per-symbol notes ────────────────────────────────────────────────
SYM_PATHS = [
    lambda s: ("/note-manager/api/notes/",  {"symbol": s, "limit": 200}),
    lambda s: ("/api/v1/text_notes/",       {"symbol": s, "limit": 200}),
    lambda s: ("/api/v1/text-notes/",       {"symbol": s, "limit": 200}),
    lambda s: ("/textnotes/list/",          {"symbol": s}),
    lambda s: ("/api/v2/symbols/" + urllib.parse.quote(s, safe="") + "/notes/", None),
    lambda s: ("/api/v1/symbols/notes/",    {"symbol": s}),
    lambda s: ("/note-manager/api/notes/",  {"symbol_id": s, "page_size": 200}),
]

def pull_symbol_notes(session, symbol):
    for path_fn in SYM_PATHS:
        path, params = path_fn(symbol)
        data, code = tv_get(path, session, params)
        if data:
            found = mine_all(data, symbol)
            if found:
                return found
        time.sleep(THROTTLE_S * 0.5)
    return []

# ── Step 5: chart layouts (text annotations / drawings) ─────────────────────
def pull_chart_layouts(session, username):
    notes = []
    params = {"sort": "recent", "limit": 50}
    if username:
        params["author"] = username
    data, _ = tv_get("/api/v2/chart-layouts/", session, params)
    if not data:
        data, _ = tv_get("/api/v2/chart-layouts/", session, {"limit": 50})
    if not data:
        return notes
    arr = data.get("data") or data.get("layouts") or (data if isinstance(data, list) else [])
    print("[crawler] chart layouts: %d" % len(arr))
    for layout in (arr[:30] if isinstance(arr, list) else []):
        if not isinstance(layout, dict):
            continue
        lid = layout.get("id") or layout.get("chart_id")
        sym = layout.get("symbol") or layout.get("name")
        if lid:
            content, _ = tv_get("/api/v2/chart-layouts/%s/content/" % lid, session)
            if not content:
                content, _ = tv_get("/api/v2/chart-layouts/%s/" % lid, session)
            if content:
                found = mine_all(content, sym)
                notes.extend(found)
        time.sleep(THROTTLE_S)
    return notes

# ── S3 mirror read/write ────────────────────────────────────────────────────
def mirror_read():
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key=MIRROR_KEY)["Body"].read())
        return d if isinstance(d.get("notes"), list) else {"notes": []}
    except Exception:
        return {"notes": []}

def mirror_write(notes_list):
    doc = {"updated": datetime.now(timezone.utc).isoformat(),
           "count": len(notes_list), "source": "justhodl-tv-notes-crawler",
           "notes": notes_list[-15000:]}
    S3.put_object(Bucket=BUCKET, Key=MIRROR_KEY,
                  Body=json.dumps(doc, ensure_ascii=False).encode("utf-8"),
                  ContentType="application/json; charset=utf-8",
                  CacheControl="max-age=300")
    return doc

# ── Brain upsert ────────────────────────────────────────────────────────────
def brain_upsert(notes, ingest_url, token):
    ok_total = err_total = 0
    BASES = [ingest_url, ingest_url]  # retry same endpoint
    chunk_size = 200
    for i in range(0, len(notes), chunk_size):
        chunk = notes[i:i + chunk_size]
        payload = json.dumps({"token": token, "notes": chunk}).encode("utf-8")
        success = False
        for base in BASES:
            try:
                req = urllib.request.Request(
                    base, data=payload, method="POST",
                    headers={"Content-Type": "application/json",
                             "User-Agent": "justhodl-tv-crawler/2.0"})
                with urllib.request.urlopen(req, timeout=30) as r:
                    d = json.loads(r.read())
                    if d.get("ok") or d.get("brain_upserted"):
                        ok_total += d.get("brain_upserted", len(chunk))
                        success = True
                        break
            except Exception as e:
                print("[brain] chunk %d err: %s" % (i, e))
        if not success:
            err_total += len(chunk)
        time.sleep(0.3)
    return ok_total, err_total

# ── Main handler ────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print("[crawler] starting at", now.isoformat())

    session      = _ssm("/justhodl/tradingview/sessionid", "TV_SESSION")
    ingest_url   = _ssm("/justhodl/tvnotes/ingest-url",   "TV_INGEST_URL")
    token        = _ssm("/justhodl/tvnotes/ingest-token", "INGEST_TOKEN")

    if not session:
        msg = ("TV_SESSION not in SSM. Add it: aws ssm put-parameter "
               "--name /justhodl/tradingview/sessionid --type SecureString "
               "--value YOUR_TV_SESSIONID_COOKIE --overwrite --region us-east-1")
        print("[crawler] MISSING SESSION:", msg)
        status = {"ok": False, "error": "session_missing",
                  "instructions": msg, "updated": now.isoformat()}
        S3.put_object(Bucket=BUCKET, Key=STATUS_KEY,
                      Body=json.dumps(status).encode(),
                      ContentType="application/json")
        return {"statusCode": 500, "body": json.dumps(status)}

    if not ingest_url:
        try:
            from botocore.config import Config
            lam = boto3.client("lambda", region_name="us-east-1")
            ingest_url = lam.get_function_url_config(
                FunctionName="justhodl-tv-notes-ingest")["FunctionUrl"].rstrip("/")
        except Exception as e:
            print("[crawler] ingest_url fallback failed: %s" % e)

    # ── Phase 1: discover username ──────────────────────────────────────
    username = get_username(session)
    print("[crawler] username: %s" % username)
    session_ok = username is not None

    # ── Phase 2: watchlist symbols ──────────────────────────────────────
    symbols = get_watchlist_symbols(session) if session_ok else []

    # ── Phase 3: bulk notes ─────────────────────────────────────────────
    all_notes = pull_bulk_notes(session) if session_ok else []
    seen_ids = {n["id"] for n in all_notes}
    print("[crawler] after bulk pull: %d notes" % len(all_notes))

    # ── Phase 4: per-symbol (only for symbols not yet covered) ──────────
    covered_syms = {n["symbol"] for n in all_notes}
    uncovered = [s for s in symbols if s not in covered_syms]
    print("[crawler] per-symbol sweep: %d symbols" % len(uncovered))
    for sym in uncovered[:500]:   # hard cap
        found = pull_symbol_notes(session, sym)
        for n in found:
            if n["id"] not in seen_ids:
                all_notes.append(n)
                seen_ids.add(n["id"])

    # ── Phase 5: chart layouts ───────────────────────────────────────────
    layout_notes = pull_chart_layouts(session, username)
    for n in layout_notes:
        if n["id"] not in seen_ids:
            all_notes.append(n)
            seen_ids.add(n["id"])
    print("[crawler] after layout scan: %d notes total" % len(all_notes))

    # ── Phase 6: merge with existing mirror ─────────────────────────────
    existing = mirror_read()
    ex_map = {n.get("id"): n for n in existing["notes"]}
    for n in all_notes:
        ex_map[n["id"]] = n
    merged = list(ex_map.values())[-MAX_NOTES:]
    doc = mirror_write(merged)
    print("[crawler] mirror written: %d notes" % len(merged))

    # ── Phase 7: upsert to brain ─────────────────────────────────────────
    brain_ok = brain_err = 0
    if ingest_url and token and all_notes:
        brain_ok, brain_err = brain_upsert(all_notes, ingest_url, token)
        print("[crawler] brain upsert: ok=%d err=%d" % (brain_ok, brain_err))

    elapsed = round(time.time() - t0, 1)
    status = {
        "ok": session_ok and brain_err == 0,
        "updated": now.isoformat(),
        "username": username,
        "session_valid": session_ok,
        "notes_crawled": len(all_notes),
        "notes_in_mirror": len(merged),
        "symbols_covered": len({n["symbol"] for n in all_notes}),
        "brain_upserted": brain_ok,
        "brain_errors": brain_err,
        "elapsed_seconds": elapsed,
        "phases": ["username", "watchlists", "bulk_notes",
                   "per_symbol", "chart_layouts", "mirror", "brain"],
    }
    S3.put_object(Bucket=BUCKET, Key=STATUS_KEY,
                  Body=json.dumps(status).encode(),
                  ContentType="application/json")
    print("[crawler] done in %.1fs" % elapsed)
    return {"statusCode": 200, "body": json.dumps(status)}
