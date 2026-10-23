"""
justhodl-fmp-bank v1.0.0 -- the FMP Ultimate warehouse (bulk + EOD), banked verbatim on S3.

Why (Khalid, 2026-10-23: "I want 100% of the data pulled and used by my system"): the provider audit found FMP
Ultimate ($139/month) consumed live by 233 engines (~7,000 per-symbol calls a day) and banked NOWHERE - the data.html
card held 8 datasets / 10.9 MB.  Ultimate sells "full historical access" and "bulk and batch delivery"; the fleet was
pulling statements 1-6 periods at a time and never touched a bulk endpoint.  This engine is the warehouse-first
doctrine (2026-09-02) applied to FMP: one puller banks the provider; consumers read S3 (aws/shared/fmp_bank.py).

What it banks (all /stable, CSV as served, gzip'd verbatim - "each banked file is OURS forever")

  SNAPSHOT bulk (whole market, no history on the provider side -> we create the history by dating each pull):
      profile-bulk?part=N            rating-bulk        dcf-bulk        scores-bulk
      price-target-summary-bulk      upgrades-downgrades-consensus-bulk
      key-metrics-ttm-bulk           ratios-ttm-bulk    peers-bulk      etf-holder-bulk?part=N
    -> data/warm/fmp-bank/<endpoint>/latest.csv.gz   (the current file, overwritten)
    -> data/warm/fmp-bank/<endpoint>/daily/YYYY-MM-DD.csv.gz  (dated copy; only when the content hash changed)

  DATED bulk (per fiscal year x period; finished years are banked once, the two newest years are re-pulled every run
  because filings keep arriving):
      income-statement-bulk  balance-sheet-statement-bulk  cash-flow-statement-bulk
      income-statement-growth-bulk  balance-sheet-statement-growth-bulk  cash-flow-statement-growth-bulk
      key-metrics-bulk  ratios-bulk  financial-growth-bulk           (periods: annual, Q1, Q2, Q3, Q4)
      earnings-surprises-bulk                                       (year only)
    -> data/warm/fmp-bank/<endpoint>/<year>/<period>.csv.gz

  EOD bulk (every listed symbol's close for one trading day, global):
      eod-bulk?date=YYYY-MM-DD   from FMP_EOD_FLOOR (default 2000-01-01) to the last closed session
    -> data/warm/fmp-bank/eod-bulk/<year>/<date>.csv.gz      (empty/404 days are counted skips, never gaps)

Doctrine carried over from the other warehouse lanes
  * lease + wall-clock budget: one invoke does as much as fits in FMP_BANK_BUDGET_S, writes state, stops.  The daily
    schedule (and ops-driven kicks for the backfill) finish the catalog over days; nothing self-chains (no event loops).
  * an endpoint the plan does not serve is RECORDED (http status in the catalog), never guessed around and never
    fatal: the catalog on data.html says exactly which endpoints answered and which did not.
  * 429 -> exponential backoff (FMP's per-minute limit), 5xx -> retry, 401/403 -> key_status recorded and the run stops
    (never a silent empty warehouse).
  * verbatim bytes, gzip file-to-file through /tmp (bulk CSVs reach tens of MB), content hash kept per file so an
    unchanged snapshot does not create a new S3 version.
  * manifest.json carries objects / bytes (count_from for the catalog card, ops 5040 doctrine) and per-endpoint
    status; never derived by walking the prefix.

Key: managed_secret(("FMP_KEY", "FMP_API_KEY"), ("/justhodl/fmp/api-key",)).  Never printed.
Schedule: EventBridge Scheduler justhodl-fmp-bank-daily cron(13 23 * * ? *) UTC (after FMP's EOD prints ~21:30,
after justhodl-fmp-ratios 22:38; the provider-publication slot cap for FMP is 4/min - minute :13 is free).
Cost: ~10-14 min x 2 GB a day once the backfill is done (~$2.5/month); S3 ~1-3 GB/year of new objects.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3

from managed_secret import managed_secret

ENGINE = "justhodl-fmp-bank"
VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/fmp-bank/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
CATALOG_KEY = ROOT + "catalog.json"
STABLE = "https://financialmodelingprep.com/stable"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com) fmp-bank/" + VERSION}
BUDGET_S = int(os.environ.get("FMP_BANK_BUDGET_S", "780") or 780)
PACE_S = float(os.environ.get("FMP_BANK_PACE_S", "0.25") or 0.25)
EOD_FLOOR = os.environ.get("FMP_EOD_FLOOR", "2000-01-01")
YEAR_FLOOR = int(os.environ.get("FMP_YEAR_FLOOR", "1985") or 1985)
REFRESH_YEARS = 2                      # newest N fiscal years are re-pulled every run
MAX_BYTES = 400_000_000
TMP = "/tmp/fmp-bank.csv"

SNAPSHOT = [
    ("profile-bulk", True), ("rating-bulk", False), ("dcf-bulk", False), ("scores-bulk", False),
    ("price-target-summary-bulk", False), ("upgrades-downgrades-consensus-bulk", False),
    ("key-metrics-ttm-bulk", False), ("ratios-ttm-bulk", False), ("peers-bulk", False), ("etf-holder-bulk", True),
]
DATED = [
    ("income-statement-bulk", True), ("balance-sheet-statement-bulk", True), ("cash-flow-statement-bulk", True),
    ("income-statement-growth-bulk", True), ("balance-sheet-statement-growth-bulk", True),
    ("cash-flow-statement-growth-bulk", True), ("key-metrics-bulk", True), ("ratios-bulk", True),
    ("financial-growth-bulk", True), ("earnings-surprises-bulk", False),
]
PERIODS = ["annual", "Q1", "Q2", "Q3", "Q4"]

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()
_last_call = [0.0]


def _now():
    return datetime.now(timezone.utc)


def _left():
    return BUDGET_S - (time.time() - _t0)


def _j(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return default


def _put_json(key, obj, cache="no-cache"):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl=cache)


def _key():
    return managed_secret(("FMP_KEY", "FMP_API_KEY"), ("/justhodl/fmp/api-key",))


class Fetch:
    """One bulk GET -> ('ok', path, nbytes, nrows, sha) | ('empty',..) | ('http<code>',..) | ('err:..',..).

    The body streams to /tmp (bulk files reach tens of MB) and is hashed on the way; 429/5xx retried with backoff.
    """

    def __init__(self, key):
        self.key = key
        self.calls = 0
        self.retries = 0

    def get(self, endpoint, params):
        q = dict(params or {})
        q["apikey"] = self.key
        url = "%s/%s?%s" % (STABLE, endpoint, urllib.parse.urlencode(q))
        delay = 2.0
        for attempt in range(5):
            gap = PACE_S - (time.time() - _last_call[0])
            if gap > 0:
                time.sleep(gap)
            _last_call[0] = time.time()
            self.calls += 1
            try:
                req = urllib.request.Request(url, headers=UA)
                with urllib.request.urlopen(req, timeout=120) as r, open(TMP, "wb") as f:
                    h, n = hashlib.sha256(), 0
                    while True:
                        chunk = r.read(1 << 20)
                        if not chunk:
                            break
                        n += len(chunk)
                        if n > MAX_BYTES:
                            return ("err:too_large", TMP, n, 0, "")
                        h.update(chunk)
                        f.write(chunk)
                rows = _count_rows(TMP) if n else 0
                if n == 0 or rows == 0:
                    return ("empty", TMP, n, 0, h.hexdigest())
                return ("ok", TMP, n, rows, h.hexdigest())
            except urllib.error.HTTPError as e:
                code = e.code
                if code == 429 or code >= 500:
                    if attempt < 4 and _left() > delay + 30:
                        self.retries += 1
                        time.sleep(delay)
                        delay *= 2
                        continue
                return ("http%d" % code, TMP, 0, 0, "")
            except Exception as e:  # noqa: BLE001
                if attempt < 4 and _left() > delay + 30:
                    self.retries += 1
                    time.sleep(delay)
                    delay *= 2
                    continue
                return ("err:" + type(e).__name__, TMP, 0, 0, "")
        return ("err:retries", TMP, 0, 0, "")


def _count_rows(path):
    """Data rows in a CSV file (header excluded); a JSON error body ("Error Message" / []) counts as 0."""
    with open(path, "rb") as f:
        head = f.read(512)
    if not head or head.lstrip()[:1] in (b"{", b"["):
        return 0
    n = 0
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for _ in csv.reader(f):
            n += 1
    return max(n - 1, 0)


def _bank(key, src_path, meta, state):
    """gzip file-to-file into S3 at <key>; records the object's size in state (objects/bytes are derived from it)."""
    gz_path = src_path + ".gz"
    with open(src_path, "rb") as fi, gzip.open(gz_path, "wb", compresslevel=6) as fo:
        while True:
            chunk = fi.read(1 << 20)
            if not chunk:
                break
            fo.write(chunk)
    size = os.path.getsize(gz_path)
    with open(gz_path, "rb") as f:
        s3.put_object(Bucket=BUCKET, Key=key, Body=f, ContentType="text/csv", ContentEncoding="gzip",
                      Metadata={k: str(v)[:250] for k, v in meta.items()})
    os.remove(gz_path)
    state.setdefault("sizes", {})[key] = size
    return size


def _record(cat, name, verdict, nbytes=0, rows=0, extra=None):
    e = cat.setdefault(name, {"endpoint": name})
    e["last_verdict"] = verdict
    e["last_at"] = _now().isoformat()
    if verdict == "ok":
        e["last_ok_at"] = e["last_at"]
        e["last_bytes"] = nbytes
        e["last_rows"] = rows
        e["ok_runs"] = e.get("ok_runs", 0) + 1
    else:
        e["fail_runs"] = e.get("fail_runs", 0) + 1
    if extra:
        e.update(extra)


def _fresh_state():
    return {"version": VERSION, "started_at": _now().isoformat(), "sizes": {}, "hashes": {}, "dated_done": {}, "eod": {"cursor": EOD_FLOOR, "sessions": 0, "skips": 0, "failures": {}},
            "runs": 0, "key_status": "unknown", "phase": "BACKFILL"}


# ---------------------------------------------------------------- snapshot bulk ----------------------------------
def do_snapshot(fx, state, cat, today):
    for ep, parted in SNAPSHOT:
        if _left() < 60:
            return False
        part = 0
        while True:
            params = {"part": part} if parted else {}
            v, path, n, rows, sha = fx.get(ep, params)
            name = ep if not parted else "%s?part=%d" % (ep, part)
            if v != "ok":
                _record(cat, name, v, n, rows)
                if v.startswith("http4"):
                    state["key_status"] = "unauthorized" if v in ("http401", "http403") else state["key_status"]
                break
            base = ROOT + ep + ("/part-%d" % part if parted else "")
            hkey = base + "/latest.csv.gz"
            if state["hashes"].get(hkey) != sha:
                _bank(hkey, path, {"engine": ENGINE, "endpoint": ep, "part": part, "rows": rows, "sha256": sha}, state)
                dated = base + "/daily/%s.csv.gz" % today
                _bank(dated, path, {"engine": ENGINE, "endpoint": ep, "part": part, "rows": rows, "sha256": sha, "as_of": today}, state)
                state["hashes"][hkey] = sha
                _record(cat, name, "ok", n, rows, {"changed": True, "key": hkey})
            else:
                _record(cat, name, "ok", n, rows, {"changed": False, "key": hkey})
            if not parted:
                break
            part += 1
            if part > 40:
                break
    return True


# ---------------------------------------------------------------- dated bulk -------------------------------------
def _dated_jobs(state, cur_year):
    """(endpoint, year, period) in priority order: newest years first so a fresh warehouse is useful on day one."""
    done = state["dated_done"]
    jobs = []
    for y in range(cur_year, YEAR_FLOOR - 1, -1):
        refresh = y >= cur_year - REFRESH_YEARS + 1
        for ep, has_period in DATED:
            for p in (PERIODS if has_period else ["year"]):
                k = "%s/%d/%s" % (ep, y, p)
                d = done.get(k)
                if d and not refresh:
                    continue
                if d and refresh and d.get("day") == state.get("today"):
                    continue
                if d and d.get("verdict", "").startswith("http4") and d.get("day") == state.get("today"):
                    continue
                jobs.append((ep, y, p, k))
    return jobs


def do_dated(fx, state, cat, today):
    cur_year = int(today[:4])
    state["today"] = today
    jobs = _dated_jobs(state, cur_year)
    state["dated_pending"] = len(jobs)
    for ep, y, p, k in jobs:
        if _left() < 45:
            return False
        params = {"year": y}
        if p != "year":
            params["period"] = p
        v, path, n, rows, sha = fx.get(ep, params)
        rec = {"verdict": v, "day": today, "rows": rows, "bytes": n}
        if v == "ok":
            key = ROOT + "%s/%d/%s.csv.gz" % (ep, y, p)
            if state["hashes"].get(key) != sha:
                _bank(key, path, {"engine": ENGINE, "endpoint": ep, "year": y, "period": p, "rows": rows, "sha256": sha}, state)
                state["hashes"][key] = sha
            rec["key"] = key
            _record(cat, ep, "ok", n, rows, {"last_year": y, "last_period": p})
        elif v == "empty":
            _record(cat, ep, "empty", 0, 0, {"last_year": y, "last_period": p})
        else:
            _record(cat, ep, v, 0, 0, {"last_year": y, "last_period": p})
            if v in ("http401", "http403"):
                state["key_status"] = "unauthorized"
                return False
        state["dated_done"][k] = rec
        state["dated_pending"] -= 1
    return True


# ---------------------------------------------------------------- eod bulk ---------------------------------------
def do_eod(fx, state, cat):
    e = state["eod"]
    edge = (_now() - timedelta(hours=6)).date()          # last closed session (FMP EOD prints ~21:30 UTC)
    cur = date.fromisoformat(e["cursor"])
    while cur <= edge:
        if _left() < 40:
            return False
        if cur.weekday() >= 5:
            cur += timedelta(days=1)
            e["cursor"] = cur.isoformat()
            continue
        ds = cur.isoformat()
        v, path, n, rows, sha = fx.get("eod-bulk", {"date": ds})
        if v == "ok":
            key = ROOT + "eod-bulk/%s/%s.csv.gz" % (ds[:4], ds)
            _bank(key, path, {"engine": ENGINE, "endpoint": "eod-bulk", "date": ds, "rows": rows, "sha256": sha}, state)
            e["sessions"] += 1
            e["last_rows"] = rows
            _record(cat, "eod-bulk", "ok", n, rows, {"last_date": ds})
        elif v in ("empty", "http404"):
            e["skips"] += 1                               # holiday / not yet published / before coverage
        elif v in ("http401", "http403"):
            state["key_status"] = "unauthorized"
            _record(cat, "eod-bulk", v)
            return False
        else:
            e["failures"][ds] = e["failures"].get(ds, 0) + 1
            _record(cat, "eod-bulk", v, 0, 0, {"last_date": ds})
            if e["failures"][ds] < 3:
                return False                              # retry this date next run; do not walk past a real failure
        cur += timedelta(days=1)
        e["cursor"] = cur.isoformat()
    e["edge"] = edge.isoformat()
    return True


# ---------------------------------------------------------------- manifest ---------------------------------------
def write_manifest(state, cat, fx, run):
    sizes = state.get("sizes") or {}
    state["objects"], state["bytes"] = len(sizes), sum(sizes.values())
    cat_doc = {"engine": ENGINE, "version": VERSION, "updated_at": _now().isoformat(), "endpoints": cat,
               "served": sorted(k for k, v in cat.items() if v.get("last_ok_at")),
               "not_served": sorted(k for k, v in cat.items() if not v.get("last_ok_at"))}
    _put_json(CATALOG_KEY, cat_doc)
    man = {"engine": ENGINE, "version": VERSION, "updated_at": _now().isoformat(),
           "objects": state["objects"], "bytes": state["bytes"], "pages": state["objects"], "pages_bytes": state["bytes"],
           "datasets": len([k for k in state["dated_done"] if state["dated_done"][k].get("verdict") == "ok"])
           + len([k for k in sizes if k.endswith("latest.csv.gz")]) + state["eod"]["sessions"],
           "eod": {k: v for k, v in state["eod"].items() if k != "failures"},
           "dated_pending": state.get("dated_pending"), "phase": state["phase"], "key_status": state["key_status"],
           "endpoints_served": cat_doc["served"], "endpoints_not_served": cat_doc["not_served"],
           "dated_keys": sorted(k for k, v in state["dated_done"].items() if v.get("verdict") == "ok"),
           "label": "FMP Ultimate bulk + EOD warehouse (verbatim CSV, gz) - never LIVE; refreshed daily 23:13 UTC",
           "run": run}
    _put_json(MANIFEST_KEY, man)
    return man


def lambda_handler(event, context=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    key = _key()
    if not key:
        return {"error": "no FMP key (env FMP_KEY / ssm /justhodl/fmp/api-key)"}
    state = _j(STATE_KEY) or _fresh_state()
    if event.get("reset_state"):
        state = _fresh_state()
    if float(state.get("lease_until") or 0) > time.time() and not event.get("force"):
        return {"skipped": "lease_held", "lease_until": state["lease_until"]}
    state["lease_until"] = time.time() + BUDGET_S + 120
    state["runs"] = state.get("runs", 0) + 1
    _put_json(STATE_KEY, state)
    cat = (_j(CATALOG_KEY) or {}).get("endpoints") or {}
    fx = Fetch(key)
    today = _now().date().isoformat()
    run = {"started_at": _now().isoformat(), "steps": []}
    only = set(event.get("only") or [])
    try:
        for name, fn in (("snapshot", lambda: do_snapshot(fx, state, cat, today)),
                         ("dated", lambda: do_dated(fx, state, cat, today)),
                         ("eod", lambda: do_eod(fx, state, cat))):
            if only and name not in only:
                continue
            if state["key_status"] == "unauthorized":
                break
            t = time.time()
            complete = fn()
            run["steps"].append({"step": name, "complete": bool(complete), "seconds": round(time.time() - t, 1)})
            if state["key_status"] == "unauthorized" or not complete or _left() < 60:
                break                                   # out of budget: the next run resumes from state
        if state["key_status"] != "unauthorized" and fx.calls and any(v.get("last_ok_at") for v in cat.values()):
            state["key_status"] = "ok"
        eod_edge_reached = state["eod"].get("edge") == (_now() - timedelta(hours=6)).date().isoformat()
        state["phase"] = "STEADY" if (state.get("dated_pending") == 0 and eod_edge_reached) else "BACKFILL"
    finally:
        run["calls"], run["retries"], run["seconds"] = fx.calls, fx.retries, round(time.time() - _t0, 1)
        run["finished_at"] = _now().isoformat()
        state["last_run"] = run
        state["lease_until"] = 0
        state["updated_at"] = _now().isoformat()
        _put_json(STATE_KEY, state)
        man = write_manifest(state, cat, fx, run)
    return {"engine": ENGINE, "version": VERSION, "phase": state["phase"], "key_status": state["key_status"],
            "objects": man["objects"], "bytes": man["bytes"], "dated_pending": state.get("dated_pending"),
            "eod_cursor": state["eod"]["cursor"], "run": run}
