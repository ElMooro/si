"""justhodl-polygon-eod v1.0.0 -- per-ticker daily histories folded from justhodl-polygon-full's grouped sessions.

Khalid 2026-10-25 ("100% of the data pulled and used"): 131 engines ask Polygon for /v2/aggs/ticker/{T}/range/1/day
per ticker on a rate-limited key, while justhodl-polygon-full already banks EVERY US session's grouped-daily file
verbatim (data/warm/polygon-full/grouped/YYYY/YYYY-MM-DD.json.gz, 2021-08-26 -> today, ~12.5k tickers each, written
02:29 UTC the next day = complete sessions, ops 6654).  This engine transposes those files into the shape the consumers
need - one file per ticker, every session it ever traded - so a per-ticker ask is one S3 read:

    data/warm/polygon-full/eod/<T>.json.gz   {"ticker","adjusted":true,"fields":["t","o","h","l","c","v","vw","n"],
                                              "rows":[[t_ms,o,h,l,c,v,vw,n],...] (sorted by t),"first","last","n",...}
    data/warm/polygon-full/eod/_state/state.json, data/warm/polygon-full/eod/manifest.json

Doctrine: reads only the polygon-full bank (never the provider; no key); folds the sessions not yet folded, oldest
first, SESSIONS_PER_RUN per invoke (memory-bounded: 120 sessions x 12.5k rows); write-if-changed per ticker; lease +
wall-clock budget; no self-chaining (the bounded conductor re-kicks it during the backfill, then one run a day after
polygon-full's 02:29 UTC run).  Holes: a weekday inside the window with no banked session that polygon-full has not
recorded as a holiday is published in the manifest as `gaps` - a consumer (polygon_bankfirst) must never answer a window
that spans one.  Nothing is guessed, nothing is derived: the rows are Polygon's own numbers for that session.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.config import Config

ENGINE = "justhodl-polygon-eod"
VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
IN_ROOT = "data/warm/polygon-full/grouped/"
IN_STATE = "data/warm/polygon-full/_state/state.json"
IN_MANIFEST = "data/warm/polygon-full/manifest.json"
ROOT = "data/warm/polygon-full/eod/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
BUDGET_S = int(os.environ.get("POLYGON_EOD_BUDGET_S", "780") or 780)
SESSIONS_PER_RUN = int(os.environ.get("POLYGON_EOD_SESSIONS_PER_RUN", "120") or 120)
WORKERS = int(os.environ.get("POLYGON_EOD_WORKERS", "16") or 16)
FIELDS = ["t", "o", "h", "l", "c", "v", "vw", "n"]
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=5, read_timeout=60, max_pool_connections=WORKERS + 4)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
_t0 = time.time()


def _now():
    return datetime.now(timezone.utc)


def _left():
    return BUDGET_S - (time.time() - _t0)


def _g(x):
    """A guard scaled to the budget (tests run with a short BUDGET_S)."""
    return min(x, BUDGET_S * x / 780.0)


def _j(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)
    except Exception:  # noqa: BLE001
        return default


def _put_json(key, obj, cache="no-cache"):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, separators=(",", ":"), default=str).encode(), ContentType="application/json", CacheControl=cache)


def safe(t):
    return "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(t))[:80] or "_"


def banked_sessions():
    """ISO dates of every grouped file polygon-full holds (listed, never assumed)."""
    out = set()
    for y in range(2020, _now().year + 1):
        tok = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": IN_ROOT + "%d/" % y, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                k = o["Key"]
                if k.endswith(".json.gz") and len(k) >= 18:
                    out.add(k[-18:-8])
            tok = resp.get("NextContinuationToken")
            if not tok:
                break
    return out


def gaps(available, holidays, window_start, through):
    """Weekdays in [window_start, through] with no banked session that are not recorded holidays."""
    out = []
    try:
        d = date.fromisoformat(window_start)
        end = date.fromisoformat(through)
    except (TypeError, ValueError):
        return out
    while d <= end:
        ds = d.isoformat()
        if d.weekday() < 5 and ds not in available and ds not in holidays:
            out.append(ds)
        d += timedelta(days=1)
    return out


def fold_session(ds, by_ticker, counts):
    """Add one session's rows to by_ticker (ticker -> list of row arrays)."""
    doc = _j(IN_ROOT + "%s/%s.json.gz" % (ds[:4], ds))
    if not isinstance(doc, dict):
        counts["unreadable"] += 1
        return False
    n = 0
    for r in doc.get("results") or []:
        t = r.get("T")
        if not t or r.get("t") is None:
            continue
        by_ticker.setdefault(str(t), []).append([r.get("t"), r.get("o"), r.get("h"), r.get("l"), r.get("c"), r.get("v"), r.get("vw"), r.get("n")])
        n += 1
    counts["rows"] += n
    return True


def write_ticker(ticker, rows, batch_sessions):
    """Merge rows into the ticker's file (same t replaces); returns (new_rows, changed)."""
    key = ROOT + safe(ticker) + ".json.gz"
    old = _j(key) or {}
    have = {r[0]: r for r in (old.get("rows") or []) if isinstance(r, list) and r}
    new = 0
    for r in rows:
        if r[0] not in have:
            new += 1
        have[r[0]] = r
    if not new and old:
        return 0, False
    ordered = [have[t] for t in sorted(have)]
    date_of = lambda ms: datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")  # noqa: E731
    doc = {"ticker": ticker, "adjusted": True, "source": "polygon grouped-daily (justhodl-polygon-full), transposed by %s v%s" % (ENGINE, VERSION),
           "fields": FIELDS, "rows": ordered, "n": len(ordered), "first": date_of(ordered[0][0]), "last": date_of(ordered[-1][0]),
           "updated_at": _now().isoformat(timespec="seconds"), "sessions_folded_last": len(batch_sessions)}
    body = gzip.compress(json.dumps(doc, separators=(",", ":")).encode())
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/gzip", CacheControl="public, max-age=900",
                  Metadata={"engine": ENGINE, "ticker": ticker[:60], "first": doc["first"], "last": doc["last"], "n": str(doc["n"]), "pulled_at": doc["updated_at"]})
    return new, True


def lambda_handler(event, context=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    state = _j(STATE_KEY) or {"version": VERSION, "runs": 0, "folded": [], "tickers": [], "phase": "BACKFILL", "key_status": "ok"}
    if float(state.get("lease_until") or 0) > time.time() and not event.get("force"):
        return {"skipped": "lease_held", "lease_until": state.get("lease_until")}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put_json(STATE_KEY, state)
    run = {"started": _now().isoformat(timespec="seconds"), "sessions": [], "rows": 0, "tickers_written": 0, "tickers_unchanged": 0, "unreadable": 0, "steps": []}
    try:
        available = banked_sessions()
        folded = set(state.get("folded") or [])
        todo = sorted(available - folded)
        if event.get("sessions"):
            todo = [s for s in todo if s in set(event["sessions"])]
        per_run = int(event.get("sessions_per_run") or SESSIONS_PER_RUN)
        batch = todo[:per_run]
        counts = {"rows": 0, "unreadable": 0}
        by_ticker = {}
        t = time.time()
        for ds in batch:
            if _left() < _g(240):
                break
            if fold_session(ds, by_ticker, counts):
                run["sessions"].append(ds)
        run["steps"].append({"step": "fold", "seconds": round(time.time() - t, 1), "sessions": len(run["sessions"]), "rows": counts["rows"]})
        run["rows"], run["unreadable"] = counts["rows"], counts["unreadable"]
        written = unchanged = 0
        if by_ticker and _left() > _g(60):
            t = time.time()
            deadline = _t0 + BUDGET_S - _g(40)
            pool = ThreadPoolExecutor(max_workers=WORKERS)
            futures = {pool.submit(write_ticker, tk, rows, run["sessions"]): tk for tk, rows in by_ticker.items()}
            pending_tickers = set(by_ticker)
            timed_out = False
            try:
                for f in as_completed(futures, timeout=max(1.0, deadline - time.time())):
                    tk = futures[f]
                    try:
                        new, changed = f.result()
                        written += 1 if changed else 0
                        unchanged += 0 if changed else 1
                        pending_tickers.discard(tk)
                    except Exception as e:  # noqa: BLE001
                        run.setdefault("errors", []).append([tk, str(e)[:120]])
                        pending_tickers.discard(tk)
                    if time.time() > deadline:
                        timed_out = True
                        break
            except Exception:  # noqa: BLE001 - as_completed timeout
                timed_out = True
            pool.shutdown(wait=False, cancel_futures=True)   # never wait past the budget: the unfinished tickers are redone next run
            run["steps"].append({"step": "write", "seconds": round(time.time() - t, 1), "tickers": written, "unchanged": unchanged, "left_unwritten": len(pending_tickers), "timed_out": timed_out})
            if pending_tickers:                    # a batch that did not finish writing is not folded: next run redoes it (idempotent merge)
                run["sessions"] = []
        run["tickers_written"], run["tickers_unchanged"] = written, unchanged
        if run["sessions"]:
            folded |= set(run["sessions"])
            tickers = set(state.get("tickers") or []) | set(by_ticker)
            state["tickers"] = sorted(tickers)
        state["folded"] = sorted(folded)
        state["phase"] = "STEADY" if not (available - folded) else "BACKFILL"
        pf_state = _j(IN_STATE) or {}
        holidays = set(pf_state.get("holidays") or [])
        window_start = pf_state.get("window_start") or (min(available) if available else None)
        through = max(available) if available else None
        g = gaps(available, holidays, window_start, through) if available else []
        state["runs"] = int(state.get("runs") or 0) + 1
        state["last_run"] = run
        state["updated_at"] = _now().isoformat(timespec="seconds")
        state["lease_until"] = 0
        _put_json(STATE_KEY, state)
        man = {"engine": ENGINE, "version": VERSION, "updated_at": state["updated_at"], "phase": state["phase"], "runs": state["runs"],
               "sessions_available": len(available), "sessions_folded": len(folded), "sessions_pending": len(available - folded),
               "first_session": min(folded) if folded else None, "last_session": max(folded) if folded else None,
               "tickers": len(state.get("tickers") or []), "objects": len(state.get("tickers") or []), "pages": len(state.get("tickers") or []),
               "window_start": window_start, "gaps": g, "holidays_recorded": sorted(holidays),
               "fields": FIELDS, "layout": ROOT + "<T>.json.gz",
               "label": "Polygon grouped-daily sessions (justhodl-polygon-full) transposed per ticker - never LIVE; daily 03:10 UTC after polygon-full 02:29", "last_run": run}
        _put_json(MANIFEST_KEY, man)
        return {"ok": True, "phase": state["phase"], "folded": len(folded), "pending": len(available - folded), "sessions_this_run": len(run["sessions"]), "rows": run["rows"],
                "tickers_written": written, "tickers_unchanged": unchanged, "gaps": g[:20], "elapsed_s": round(time.time() - _t0, 1), "key_status": "ok"}
    except Exception:
        state["lease_until"] = 0
        state["last_error"] = {"at": _now().isoformat(timespec="seconds")}
        _put_json(STATE_KEY, state)
        raise
