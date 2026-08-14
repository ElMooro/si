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
from datetime import datetime, timezone   # ops4061: _save_sources crashed 3x on this missing import
import os
import time
import urllib.request

import boto3

S3 = boto3.client("s3")
SSM = boto3.client("ssm")
BUCKET = "justhodl-dashboard-live"
MIRROR_KEY = "data/tradingview-notes.json"
WATCHLISTS_KEY = "data/tv-watchlists.json"
# ops4030: watchlist cap 200 silently dropped 291 of Khalid's 491 lists
SOURCES_KEY = "data/tv-sources.json"   # ops4016 sources: TV per-metric attribution
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




def _save_descs(descs):
    """Accrete symbol -> description. Additive: never drops a known one."""
    if not descs or not isinstance(descs, dict):
        return 0
    key = "data/tv-descriptions.json"
    try:
        cur = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        m = cur.get("descriptions") or {}
    except Exception:
        m = {}
    before = len(m)
    for k, v in descs.items():
        if isinstance(v, str) and v and k not in m:
            m[k] = v
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps({"generated_at":
                                   datetime.now(timezone.utc).isoformat(),
                                   "n": len(m), "descriptions": m}),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[ingest] descriptions {before} -> {len(m)}")
    return len(m) - before


def _save_sources(sources, diag=None):
    """ops4016: TradingView per-metric attribution — the canonical source
    map ('you can see where tradingview is pulling their data from').
    Merged by symbol so every browse session enriches it."""
    # ops4052: diag ships even at ZERO sources — the file is born either
    # way, so the server can finally SEE why capture fails without
    # screenshots.
    if (not sources or not isinstance(sources, list)) and not diag:
        return 0
    sources = sources if isinstance(sources, list) else []
    try:
        cur = json.loads(S3.get_object(Bucket=BUCKET,
                                       Key=SOURCES_KEY)["Body"].read())
    except Exception:
        cur = {"sources": {}}
    m = cur.get("sources") or {}
    n = 0
    for it in sources[:4000]:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip()[:40]
        src = str(it.get("source") or "").strip()[:120]
        if not sym or not src:
            continue
        m[sym] = {"source": src,
                  "description": str(it.get("description") or "")[:160],
                  "updated": datetime.now(timezone.utc).isoformat()}
        n += 1
    S3.put_object(Bucket=BUCKET, Key=SOURCES_KEY,
                  Body=json.dumps({"generated_at":
                                   datetime.now(timezone.utc).isoformat(),
                                   "n_symbols": len(m),
                                   "last_harvest_diag": diag,
                                   "sources": m}),
                  ContentType="application/json", CacheControl="max-age=300")
    return n


def _save_watchlists(watchlists):
    """ops 3158: watchlists ARE the predictive unit — mirror them for the
    tracker engine. Merge by list id: latest sync wins per list."""
    if not watchlists or not isinstance(watchlists, list):
        return 0
    from datetime import datetime, timezone
    clean = []
    for l in watchlists[:1200]:
        if not isinstance(l, dict):
            continue
        syms = [str(x)[:40] for x in (l.get("symbols") or [])
                if isinstance(x, str)][:500]
        name = str(l.get("name") or "").strip()[:120]
        if name and syms:
            clean.append({"id": str(l.get("id") or name)[:80],
                          "name": name, "symbols": syms,
                          "n": len(syms), "color": l.get("color")})
    if not clean:
        return 0
    try:
        prev = json.loads(S3.get_object(Bucket=BUCKET,
                          Key=WATCHLISTS_KEY)["Body"].read())
        by_id = {l["id"]: l for l in prev.get("lists") or []}
    except Exception:
        by_id = {}
    for l in clean:
        by_id[l["id"]] = l
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "source": "tv-extension", "n_lists": len(by_id),
           "lists": sorted(by_id.values(), key=lambda x: x["name"].lower())}
    S3.put_object(Bucket=BUCKET, Key=WATCHLISTS_KEY,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json")
    return len(clean)

def _save_bars(bars):
    """Store TV chart bars raw, append-only, one doc per symbol.

    Deliberately does NOT touch data/warm/fred-scoped/* — a browser can
    post anything, so bars land in their own prefix and an ops pass with
    splice-validation decides what is trustworthy enough to merge.
    """
    if not isinstance(bars, list) or not bars:
        return {"ok": False, "error": "no bars"}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    n_series = n_new = 0
    index = []
    for it in bars[:400]:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip()[:60]
        rows = it.get("bars")
        if not sym or not isinstance(rows, list) or not rows:
            continue
        safe = "".join(c if (c.isalnum() or c in "-_.") else "_"
                       for c in sym)
        key = "data/warm/tv-bars/%s.json" % safe
        try:
            cur = json.loads(S3.get_object(Bucket=BUCKET,
                                           Key=key)["Body"].read())
            have = {int(b[0]): b for b in (cur.get("bars") or [])
                    if isinstance(b, list) and b}
        except Exception:
            cur, have = {"symbol": sym, "source": "tradingview-ws"}, {}
        before = len(have)
        for b in rows:
            if isinstance(b, list) and len(b) >= 5:
                try:
                    have[int(b[0])] = [int(b[0])] + [
                        (None if v is None else float(v))
                        for v in b[1:5]]
                except Exception:
                    continue
        merged = [have[k] for k in sorted(have)]
        cur["bars"] = merged
        cur["n"] = len(merged)
        cur["updated_at"] = now
        if merged:
            cur["first_ts"] = merged[0][0]
            cur["last_ts"] = merged[-1][0]
            cur["first_date"] = datetime.fromtimestamp(
                merged[0][0], tz=timezone.utc).strftime("%Y-%m-%d")
            cur["last_date"] = datetime.fromtimestamp(
                merged[-1][0], tz=timezone.utc).strftime("%Y-%m-%d")
        S3.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps(cur, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        n_series += 1
        n_new += len(have) - before
        index.append({"symbol": sym, "key": key, "n": cur["n"],
                      "first": cur.get("first_date"),
                      "last": cur.get("last_date")})
        print("[tv-bars] %s n=%d (+%d) %s -> %s"
              % (sym, cur["n"], len(have) - before,
                 cur.get("first_date"), cur.get("last_date")))
    try:
        idx_key = "data/warm/tv-bars/_index.json"
        try:
            idx = json.loads(S3.get_object(
                Bucket=BUCKET, Key=idx_key)["Body"].read())
        except Exception:
            idx = {"symbols": {}}
        for e in index:
            idx["symbols"][e["symbol"]] = e
        idx["updated_at"] = now
        idx["n_symbols"] = len(idx["symbols"])
        S3.put_object(Bucket=BUCKET, Key=idx_key,
                      Body=json.dumps(idx, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
    except Exception as e:
        print("[tv-bars] index: %s" % str(e)[:120])
    return {"ok": True, "series": n_series, "new_bars": n_new,
            "index": index[:40]}


def _save_series(series):
    """Store archived observations, one doc per series, append-only."""
    if not isinstance(series, list) or not series:
        return {"ok": False, "error": "no series"}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out, idx_add = [], {}
    for it in series[:60]:
        if not isinstance(it, dict):
            continue
        sid = "".join(c for c in str(it.get("id") or "").strip()
                      if c.isalnum())[:40]
        rows = it.get("rows")
        if not sid or not isinstance(rows, list) or len(rows) < 50:
            out.append({"id": sid, "ok": False,
                        "error": "need >=50 rows, got %s"
                                 % (len(rows) if isinstance(rows, list)
                                    else None)})
            continue
        clean = {}
        for row in rows:
            try:
                d = str(row[0])[:10]
                v = row[1]
                if len(d) == 10 and d[4] == "-" and v is not None:
                    float(v)
                    clean[d] = str(v)
            except Exception:
                continue
        if len(clean) < 50:
            out.append({"id": sid, "ok": False,
                        "error": "only %d parseable" % len(clean)})
            continue
        key = "data/warm/archived-fred/%s.json" % sid
        try:
            cur = json.loads(S3.get_object(Bucket=BUCKET,
                                           Key=key)["Body"].read())
            have = dict((o["date"], o["value"])
                        for o in cur.get("observations") or [])
        except Exception:
            cur, have = {}, {}
        before = len(have)
        have.update(clean)          # append-only union
        obs = [{"date": d, "value": have[d]} for d in sorted(have)]
        doc = {"id": sid, "source": it.get("source") or "wayback",
               "capture": str(it.get("capture") or "")[:40],
               "page_url": str(it.get("url") or "")[:300],
               "updated_at": now, "n": len(obs),
               "first": obs[0]["date"], "last": obs[-1]["date"],
               "observations": obs}
        S3.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        idx_add[sid] = {"n": len(obs), "first": obs[0]["date"],
                        "last": obs[-1]["date"],
                        "capture": doc["capture"]}
        out.append({"id": sid, "ok": True, "n": len(obs),
                    "added": len(have) - before,
                    "first": obs[0]["date"], "last": obs[-1]["date"]})
        print("[archived-fred] %s n=%d (+%d) %s -> %s"
              % (sid, len(obs), len(have) - before,
                 obs[0]["date"], obs[-1]["date"]))
    try:
        ik = "data/warm/archived-fred/_index.json"
        try:
            idx = json.loads(S3.get_object(Bucket=BUCKET,
                                           Key=ik)["Body"].read())
        except Exception:
            idx = {"series": {}}
        idx["series"].update(idx_add)
        idx["updated_at"] = now
        idx["n_series"] = len(idx["series"])
        S3.put_object(Bucket=BUCKET, Key=ik,
                      Body=json.dumps(idx, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
    except Exception as e:
        print("[archived-fred] index: %s" % str(e)[:120])
    return {"ok": True, "results": out,
            "banked": sum(1 for x in out if x.get("ok"))}


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

    # ── v-bars (Khalid: "TradingView has all ICE data since inception —
    # I can see it in my account"). TV streams chart bars over a
    # WebSocket, which the fetch/XHR taps could never see; extension
    # v1.9.0 mines them and posts kind="bars". Landing zone is raw and
    # append-only — merging into banked FRED docs happens in a separate
    # audited ops pass, never blind from a browser payload.
    if str(req.get("kind") or "") == "bars":
        return _resp(200, _save_bars(req.get("bars")))

    # kind="series" — Khalid's browser posts archived FRED history
    # straight in. Every automated egress (Lambda) is blocked by
    # archive.org (498), TradingView (TLS fingerprint) and
    # investing.com (403); his residential browser is not. Landing zone
    # is raw + append-only; an ops pass does the checksum + merge, so a
    # browser payload still never rewrites banked history unaudited.
    if str(req.get("kind") or "") == "series":
        return _resp(200, _save_series(req.get("series")))

    wl_saved = 0
    try:
        wl_saved = _save_watchlists(req.get("watchlists"))
    except Exception as _e:
        print(f"[tv-ingest] watchlists save failed: {str(_e)[:120]}")
    src_saved = 0
    try:
        src_saved = _save_sources(req.get("sources"), req.get("harvest_diag"))
        # ops4085: descriptions land in their OWN artifact. TradingView
        # returns source=null for macro symbols but still returns a
        # description, and that description is the join key for mapping
        # ECONOMICS: codes onto real FRED series. Kept separate from
        # tv-sources so a description can never be misread as attribution.
        try:
            _save_descs(req.get("descs"))
        except Exception as e:
            print(f"[ingest] descs save failed: {e}")
    except Exception as _e:
        print(f"[tv-ingest] sources save failed: {str(_e)[:120]}")

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
        if wl_saved:
            return _resp(200, {"ok": True, "brain_upserted": 0,
                               "mirror_added": 0,
                               "watchlists_saved": wl_saved, "sources_saved": src_saved, "sources_saved": src_saved})
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

    # brain upserts — PARALLEL (ops 3161: 1,983-note harvests were timing
    # out on serial writes; 8 workers keep a 200-note request well inside
    # the lambda budget). Brain failure is NEVER fatal: the S3 mirror still
    # captures everything, and the first error is returned verbatim so the
    # extension can show it instead of a blank "upload failed".
    from concurrent.futures import ThreadPoolExecutor
    brain_ok = brain_err = 0
    brain_error_sample = None
    chunks = [[{k: v for k, v in n.items() if not k.startswith("_")}
               for n in notes[i:i + 100]]
              for i in range(0, len(notes), 100)]
    if chunks:
        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(
                lambda c: (len(c), _brain_put({"notes_upsert": c}, uid)),
                chunks))
        for n_c, d in results:
            if d.get("ok"):
                brain_ok += n_c
            else:
                brain_err += n_c
                if brain_error_sample is None:
                    brain_error_sample = json.dumps(d)[:200]
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

    out = {"ok": (brain_err == 0 or added > 0 or brain_ok > 0),
           "brain_error_sample": brain_error_sample,
           "received": len(raw),
           "normalized": len(notes), "rejected": rejected,
           "brain_upserted": brain_ok, "brain_failed": brain_err,
           "watchlists_saved": wl_saved, "sources_saved": src_saved,
           "mirror_added": added, "mirror_total": len(m["notes"])}
    print(json.dumps(out))
    return _resp(200 if out["ok"] else 502, out)
