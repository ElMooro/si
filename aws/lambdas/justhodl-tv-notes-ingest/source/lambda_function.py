"""justhodl-tv-notes-ingest — the landing zone for TradingView notes.

Receives note batches POSTed from Khalid's OWN browser session on
tradingview.com (the /tools/tv-export.js extractor), and writes them into the
real Brain store via the same worker route brain.html uses — so the Brain's
own junk filter, dedupe and index ceiling stay in force. Also mirrors the raw
harvest to data/tradingview-notes.json for engine-side provenance.

Auth: shared ingest token (SSM /justhodl/tvnotes/ingest-token). The token is
a spam barrier for a personal endpoint, not cryptographic identity — writes
are additionally shape-validated, size-capped and idempotent (deterministic
ids, so re-runs upsert instead of duplicating).

Modes (POST JSON): {token, notes:[...]}            -> ingest
                   {token, selftest:true, notes}    -> dry-run validation only
                   {token, delete_ids:[...]}        -> remove from brain+mirror
GET -> health {ok, mirror_count}. OPTIONS -> CORS preflight.
"""
import base64
import hashlib
import json
import os
import time
import urllib.request

import boto3

S3 = boto3.client("s3")
SSM = boto3.client("ssm")
BUCKET = "justhodl-dashboard-live"
MIRROR_KEY = "data/tradingview-notes.json"
BRAIN_BASES = ["https://api.justhodl.ai",
               "https://justhodl-data-proxy.raafouis.workers.dev"]
MAX_NOTES = 2000
MAX_TEXT = 8000

CORS = {"Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "content-type"}


def _ssm(name, env_key):
    v = os.environ.get(env_key)
    if v:
        return v
    return SSM.get_parameter(Name=name,
                             WithDecryption=True)["Parameter"]["Value"]


def _resp(code, body):
    return {"statusCode": code, "headers": {**CORS,
            "Content-Type": "application/json"},
            "body": json.dumps(body, default=str)}


def _mirror_read():
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET,
                                     Key=MIRROR_KEY)["Body"].read())
        return d if isinstance(d.get("notes"), list) else {"notes": []}
    except Exception:
        return {"notes": []}


def _mirror_write(doc):
    doc["updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    doc["count"] = len(doc["notes"])
    S3.put_object(Bucket=BUCKET, Key=MIRROR_KEY,
                  Body=json.dumps(doc, ensure_ascii=False,
                                  default=str).encode("utf-8"),
                  ContentType="application/json; charset=utf-8",
                  CacheControl="max-age=300")


def _brain_put(payload, uid):
    body = json.dumps(payload).encode("utf-8")
    last = None
    for base in BRAIN_BASES:
        try:
            req = urllib.request.Request(
                "%s/brain?uid=%s" % (base, uid), data=body, method="PUT",
                headers={"Content-Type": "text/plain",
                         "User-Agent": "justhodl-tv-ingest/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read().decode("utf-8", "replace"))
                if d.get("ok"):
                    return d
                last = d
        except Exception as e:
            last = {"error": str(e)}
    return last or {"error": "brain unreachable"}


def _norm(raw):
    """Normalize one harvested TV note into a Brain note. None = rejected."""
    if not isinstance(raw, dict):
        return None
    text = str(raw.get("text") or raw.get("note") or
               raw.get("content") or "").strip()
    if len(text) < 3:
        return None
    symbol = str(raw.get("symbol") or raw.get("ticker") or
                 raw.get("s") or "UNTAGGED").upper().strip()[:24]
    title = str(raw.get("title") or "").strip()
    created = raw.get("created") or raw.get("updated") or raw.get("ts")
    try:
        created = int(float(created))
        if created < 10 ** 12:  # seconds -> ms
            created *= 1000
    except (TypeError, ValueError):
        created = int(time.time() * 1000)
    body = "[TV:%s] " % symbol
    if title and title.lower() not in text.lower():
        body += title + " — "
    body += text
    if len(body) > MAX_TEXT:
        body = body[:MAX_TEXT - 1] + "…"
    nid = "tv-" + hashlib.sha1(
        ("%s|%s|%s" % (symbol, created, text[:160])).encode("utf-8")
    ).hexdigest()[:16]
    return {"id": nid, "cat": "thesis", "text": body,
            "created": created, "pinned": False,
            "_symbol": symbol}


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {})
              .get("method") or event.get("httpMethod") or "POST").upper()
    if method == "OPTIONS":
        return {"statusCode": 204, "headers": CORS, "body": ""}
    if method == "GET":
        return _resp(200, {"ok": True, "service": "tv-notes-ingest",
                           "mirror_count": len(_mirror_read()["notes"])})

    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8", "replace")
    try:
        req = json.loads(body)
    except Exception:
        return _resp(400, {"ok": False, "error": "bad json"})

    token = _ssm("/justhodl/tvnotes/ingest-token", "INGEST_TOKEN")
    if str(req.get("token") or "") != token:
        return _resp(403, {"ok": False, "error": "bad token"})
    uid = _ssm("/justhodl/brain/uid", "BRAIN_UID")

    # ---- delete mode (used by ops self-test cleanup) --------------------
    if req.get("delete_ids"):
        ids = [str(i) for i in req["delete_ids"]][:50]
        for i in ids:
            _brain_put({"delete": i}, uid)
        m = _mirror_read()
        before = len(m["notes"])
        m["notes"] = [n for n in m["notes"] if n.get("id") not in set(ids)]
        _mirror_write(m)
        return _resp(200, {"ok": True, "deleted": ids,
                           "mirror_removed": before - len(m["notes"])})

    # ---- ingest ----------------------------------------------------------
    raw = req.get("notes") or []
    if not isinstance(raw, list) or not raw:
        return _resp(400, {"ok": False, "error": "notes[] required"})
    raw = raw[:MAX_NOTES]
    notes, rejected = [], 0
    for r in raw:
        n = _norm(r)
        if n:
            notes.append(n)
        else:
            rejected += 1
    # in-batch dedupe by id
    seen, uniq = set(), []
    for n in notes:
        if n["id"] not in seen:
            seen.add(n["id"])
            uniq.append(n)
    notes = uniq

    if req.get("selftest"):
        return _resp(200, {"ok": True, "dryrun": True,
                           "would_ingest": len(notes),
                           "rejected": rejected,
                           "sample": notes[:2]})

    # brain upserts in chunks (route enforces its own filters + dedupe)
    brain_ok = brain_err = 0
    for i in range(0, len(notes), 300):
        chunk = [{k: v for k, v in n.items() if not k.startswith("_")}
                 for n in notes[i:i + 300]]
        d = _brain_put({"notes_upsert": chunk}, uid)
        if d.get("ok"):
            brain_ok += len(chunk)
        else:
            brain_err += len(chunk)
            print("[brain] chunk fail: %s" % json.dumps(d)[:300])

    # mirror merge (id-idempotent)
    m = _mirror_read()
    have = {n.get("id") for n in m["notes"]}
    added = 0
    for n in notes:
        if n["id"] not in have:
            m["notes"].append({"id": n["id"], "symbol": n["_symbol"],
                               "text": n["text"], "created": n["created"],
                               "source": "tradingview"})
            added += 1
    m["notes"] = m["notes"][-15000:]
    _mirror_write(m)

    out = {"ok": brain_err == 0, "received": len(raw),
           "normalized": len(notes), "rejected": rejected,
           "brain_upserted": brain_ok, "brain_failed": brain_err,
           "watchlists_saved": wl_saved,
           "mirror_added": added, "mirror_total": len(m["notes"])}
    print(json.dumps(out))
    return _resp(200 if out["ok"] else 502, out)
