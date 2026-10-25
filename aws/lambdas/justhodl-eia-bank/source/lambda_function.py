"""
justhodl-eia-bank v1.0.0 -- the whole EIA v2 catalog, banked verbatim on S3 under data/warm/eia-bank/.

Why (Khalid, 2026-10-25: "fix it all, I want 100% of the data pulled and used by my system"): the audit found EIA at
17 series in 4 engines (two of them broken by key names until 2026-10-23).  EIA v2 serves petroleum, natural gas,
electricity (incl. RTO hourly), coal, nuclear outages, STEO and AEO projections, international, total energy, CO2,
renewables and prices - thousands of series behind ~150 leaf routes.  This engine walks the route tree and banks every
leaf: its metadata, its full history as immutable offset pages, and a daily newest-rows file.

How
  * tree: GET /v2/ then every child route recursively (parents list routes; leaves carry data fields, facets,
    frequencies, startPeriod/endPeriod) -> data/warm/eia-bank/_catalog/routes.json.gz (+ per-route metadata files).
  * stream (backfill): for each leaf x its default frequency, GET /v2/<route>/data?frequency=F&data[0]=value&
    sort[0][column]=period&sort[0][direction]=asc&offset=N&length=5000 -> <route>/<F>/pages/p000NNN.json.gz, cursor =
    offset; pages are immutable once the next page exists (rows sorted by period ascending never move); the last page
    is re-pulled each run until the leaf reaches its endPeriod total.
  * latest (daily): the newest 5000 rows (sort desc) -> <route>/<F>/latest/<date>.json.gz when changed.
  * one token bucket (EIA_BANK_RPS, 1.5/s; EIA allows 5,000 req/hour), 780 s budget, lease, cursors per leaf; a leaf's
    refusal (403/404/429/5xx) is recorded by code and retried next run; nothing self-chains (daily schedule + the
    bounded conductor fmp-backfill.yml finish the backfill).  Write-if-changed via sha256 in S3 metadata.

Key: managed_secret(("EIA_API_KEY",), ("/justhodl/eia/api-key",)).  Never printed.
Schedule: EventBridge Scheduler justhodl-eia-bank-daily cron(31 21 * * ? *) UTC.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

from managed_secret import managed_secret

ENGINE = "justhodl-eia-bank"
VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/eia-bank/"
BASE = os.environ.get("EIA_BASE", "https://api.eia.gov/v2")
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com) eia-bank/" + VERSION, "Accept": "application/json"}
BUDGET_S = int(os.environ.get("EIA_BANK_BUDGET_S", "780") or 780)
WORKERS = int(os.environ.get("EIA_BANK_WORKERS", "4") or 4)
RPS = float(os.environ.get("EIA_BANK_RPS", "1.5") or 1.5)
PAGE = int(os.environ.get("EIA_BANK_PAGE", "5000") or 5000)
PAGES_PER_LEAF_PER_RUN = int(os.environ.get("EIA_BANK_PAGES_PER_LEAF", "6") or 6)
MAX_DEPTH = 8
SAFE_RE = re.compile(r"[^A-Za-z0-9._=-]")

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()


def _now():
    return datetime.now(timezone.utc)


def _left():
    return BUDGET_S - (time.time() - _t0)


def _g(x):
    return min(float(x), BUDGET_S * float(x) / 780.0)


def _j(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return default


def _put_json(key, obj, cache="no-cache"):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, separators=(",", ":"), default=str).encode(), ContentType="application/json", CacheControl=cache)


def safe(s):
    return SAFE_RE.sub("_", str(s))[:160] or "_"


class Bucket:
    def __init__(self, rps):
        self.rps = max(0.2, rps)
        self.tokens = 1.0
        self.t = time.time()
        self.slow_until = 0.0
        self.lock = threading.Lock()

    def take(self):
        while True:
            with self.lock:
                now = time.time()
                rate = self.rps if now > self.slow_until else self.rps / 4
                self.tokens = min(max(1.0, rate), self.tokens + (now - self.t) * rate)
                self.t = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / rate
            time.sleep(min(wait, 0.5))

    def throttled(self):
        with self.lock:
            self.slow_until = time.time() + 120


class Fetch:
    def __init__(self, key, rps):
        self.key = key
        self.bucket = Bucket(rps)
        self.calls = 0
        self.retries = 0
        self.http = {}
        self.lock = threading.Lock()

    def _count(self, v):
        with self.lock:
            self.http[v] = self.http.get(v, 0) + 1

    def get(self, path, params=None):
        """GET /v2<path> -> (verdict, response_dict_or_None, nbytes).  EIA wraps everything in {"response": ...}; errors come
        as {"error": "...", "code": ...} with a 4xx or sometimes a 200."""
        q = dict(params or {})
        q["api_key"] = self.key
        url = BASE + path + "?" + urllib.parse.urlencode(q, doseq=True)
        delay = 2.0
        for attempt in range(4):
            if _left() < _g(20):
                self._count("err:budget")
                return ("err:budget", None, 0)
            self.bucket.take()
            with self.lock:
                self.calls += 1
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=90) as r:
                    body = r.read()
                try:
                    data = json.loads(body) if body.strip() else {}
                except ValueError:
                    self._count("err:NotJSON")
                    return ("err:NotJSON", None, len(body))
                if isinstance(data, dict) and data.get("error") and "response" not in data:
                    code = str(data.get("code") or "api_error")
                    self._count("api_error:" + code)
                    return ("api_error:" + code, data, len(body))
                resp = data.get("response") if isinstance(data, dict) else None
                if resp is None:
                    self._count("err:NoResponse")
                    return ("err:NoResponse", data, len(body))
                self._count("ok")
                return ("ok", resp, len(body))
            except urllib.error.HTTPError as e:
                code = e.code
                if code == 429:
                    self.bucket.throttled()
                if (code == 429 or code >= 500) and attempt < 3:
                    with self.lock:
                        self.retries += 1
                    time.sleep(delay)
                    delay *= 2
                    continue
                self._count("http%d" % code)
                return ("http%d" % code, None, 0)
            except Exception as e:  # noqa: BLE001
                if attempt < 3:
                    with self.lock:
                        self.retries += 1
                    time.sleep(delay)
                    delay *= 2
                    continue
                self._count("err:" + type(e).__name__)
                return ("err:" + type(e).__name__, None, 0)
        return ("err:retries", None, 0)


class Store:
    def __init__(self):
        self.puts = 0
        self.unchanged = 0
        self.bytes = 0
        self.lock = threading.Lock()

    def put(self, key, obj, meta):
        body = json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")
        sha = hashlib.sha256(body).hexdigest()
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            if (head.get("Metadata") or {}).get("sha256") == sha:
                with self.lock:
                    self.unchanged += 1
                return False, len(body)
        except Exception:  # noqa: BLE001
            pass
        gz = gzip.compress(body, compresslevel=6)
        m = {k: str(v)[:250] for k, v in meta.items()}
        m["sha256"] = sha
        m["engine"] = ENGINE
        s3.put_object(Bucket=BUCKET, Key=key, Body=gz, ContentType="application/json", ContentEncoding="gzip", Metadata=m)
        with self.lock:
            self.puts += 1
            self.bytes += len(gz)
        return True, len(gz)


def walk_tree(fx, store, state, cat):
    """Discover every leaf route (depth-first, bounded), banking each route's metadata; the catalog is refreshed weekly
    (EIA adds routes rarely) and on a first run."""
    today = _now().date().isoformat()
    last = state.get("tree_day")
    leaves = state.get("leaves") or {}
    if leaves and last and (_now().date() - datetime.fromisoformat(last).date()).days < 7:
        return leaves
    found = {}
    todo = [("", 0)]
    seen = set()
    n_meta = 0
    while todo and _left() > _g(90):
        path, depth = todo.pop()
        if path in seen or depth > MAX_DEPTH:
            continue
        seen.add(path)
        v, resp, nb = fx.get(path or "/")
        if v != "ok":
            cat.setdefault("_tree", {}).setdefault("failures", {})[path or "/"] = v
            continue
        routes = resp.get("routes") if isinstance(resp, dict) else None
        if routes:
            for rt in routes:
                rid = rt.get("id") if isinstance(rt, dict) else None
                if rid:
                    todo.append((path + "/" + str(rid), depth + 1))
            store.put(ROOT + "_catalog/routes" + (path.replace("/", "__") or "_root") + ".json.gz", resp, {"route": path or "/", "pulled_at": today})
            n_meta += 1
        else:
            freqs = [f.get("id") for f in (resp.get("frequency") or []) if isinstance(f, dict) and f.get("id")]
            default_f = resp.get("defaultFrequency") or (freqs[0] if freqs else None)
            data_fields = sorted((resp.get("data") or {}).keys()) if isinstance(resp.get("data"), dict) else []
            found[path] = {"frequencies": freqs, "default_frequency": default_f, "data_fields": data_fields, "facets": [f.get("id") for f in (resp.get("facets") or []) if isinstance(f, dict)],
                           "start": resp.get("startPeriod"), "end": resp.get("endPeriod"), "name": resp.get("name"), "description": (resp.get("description") or "")[:300]}
            store.put(ROOT + safe(path.strip("/")) + "/_meta.json.gz", resp, {"route": path, "pulled_at": today})
            n_meta += 1
    if found:
        merged = dict(leaves)
        merged.update(found)
        state["leaves"] = merged
        state["tree_day"] = today
        state["tree_discovered"] = len(found)
        store.put(ROOT + "_catalog/leaves.json.gz", merged, {"pulled_at": today, "leaves": len(merged)})
        return merged
    return leaves


def leaf_jobs(fx, store, state, cat, path, meta, mode):
    """mode 'stream': up to PAGES_PER_LEAF_PER_RUN ascending pages from the leaf's cursor; mode 'latest': newest rows."""
    f = meta.get("default_frequency")
    if not f:
        return "no_frequency"
    fields = meta.get("data_fields") or ["value"]
    params = {"frequency": f, "sort[0][column]": "period", "length": PAGE}
    for i, fld in enumerate(fields[:12]):
        params["data[%d]" % i] = fld
    ls = state.setdefault("leaf_state", {}).setdefault(path, {"offset": 0, "pages": 0, "complete": False, "total": None})
    base = ROOT + safe(path.strip("/")) + "/" + safe(f)
    if mode == "latest":
        p = dict(params)
        p["sort[0][direction]"] = "desc"
        v, resp, nb = fx.get(path + "/data", p)
        if v != "ok":
            return v
        rows = resp.get("data") if isinstance(resp, dict) else None
        if not rows:
            return "empty"
        store.put(base + "/latest/%s.json.gz" % _now().date().isoformat(), resp, {"route": path, "frequency": f, "pulled_at": _now().date().isoformat(), "total": resp.get("total")})
        ls["total"] = resp.get("total")
        return "ok"
    if ls.get("complete"):
        # re-pull only the last page (new rows land there) once per run
        offset = max(0, int(ls.get("offset") or 0) - PAGE)
        pages = 1
    else:
        offset = int(ls.get("offset") or 0)
        pages = PAGES_PER_LEAF_PER_RUN
    for _ in range(pages):
        if _left() < _g(45):
            return "err:budget"
        p = dict(params)
        p["sort[0][direction]"] = "asc"
        p["offset"] = offset
        v, resp, nb = fx.get(path + "/data", p)
        if v != "ok":
            return v
        rows = resp.get("data") if isinstance(resp, dict) else None
        total = resp.get("total")
        try:
            total = int(total) if total is not None else None
        except (TypeError, ValueError):
            total = None
        ls["total"] = total
        if not rows:
            ls["complete"] = True
            break
        store.put(base + "/pages/p%06d.json.gz" % (offset // PAGE), resp, {"route": path, "frequency": f, "offset": offset, "rows": len(rows), "total": total or ""})
        ls["pages"] = max(ls.get("pages", 0), offset // PAGE + 1)
        offset += len(rows)
        ls["offset"] = offset
        if len(rows) < PAGE or (total is not None and offset >= total):
            ls["complete"] = True
            break
    ls["last_run"] = _now().date().isoformat()
    return "ok"


def write_manifest(state, cat, fx, store, run):
    leaves = state.get("leaves") or {}
    ls = state.get("leaf_state") or {}
    complete = sum(1 for v in ls.values() if v.get("complete"))
    objects = int(state.get("objects") or 0)
    nbytes = int(state.get("bytes") or 0)
    refused = {k: v for k, v in (cat.get("leaves") or {}).items() if v and v not in ("ok", "empty")}
    phase = "STEADY" if leaves and complete >= len(leaves) and state.get("runs", 0) > 1 else "BACKFILL"
    state["phase"] = phase
    man = {"engine": ENGINE, "version": VERSION, "updated_at": _now().isoformat(), "objects": objects, "bytes": nbytes, "pages": objects, "pages_bytes": nbytes,
           "datasets": len([k for k, v in ls.items() if v.get("pages")]), "datasets_declared": len(leaves), "leaves": len(leaves), "leaves_complete": complete,
           "phase": phase, "key_status": state.get("key_status"), "tree_day": state.get("tree_day"),
           "endpoints_served": sorted(k for k, v in (cat.get("leaves") or {}).items() if v in ("ok", "empty")), "endpoints_refused": sorted(refused.items()),
           "rows_total_known": sum(int(v.get("total") or 0) for v in ls.values() if v.get("total")), "rows_banked_est": sum(int(v.get("offset") or 0) for v in ls.values()),
           "run": run, "runs": state.get("runs"), "skipped": [{"path": "facet-filtered sub-queries", "why": "every row of every leaf is banked with all its facet columns; filtered views are derivable"}],
           "label": "EIA v2 - every leaf route, full history as offset pages + daily newest rows (verbatim JSON, gz); refreshed daily 21:31 UTC"}
    _put_json(ROOT + "manifest.json", man)
    _put_json(ROOT + "catalog.json", {"engine": ENGINE, "version": VERSION, "updated_at": man["updated_at"], "leaves": cat.get("leaves"), "tree": cat.get("_tree"), "leaf_state": ls})
    return man


def lambda_handler(event, context=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    key = managed_secret(("EIA_API_KEY",), ("/justhodl/eia/api-key",))
    if not key:
        return {"error": "no EIA key (env EIA_API_KEY / ssm /justhodl/eia/api-key)"}
    state_key = ROOT + "_state/state.json"
    fresh = {"version": VERSION, "started_at": _now().isoformat(), "runs": 0, "key_status": "unknown", "phase": "BACKFILL", "objects": 0, "bytes": 0}
    state = fresh if event.get("reset_state") else (_j(state_key) or fresh)
    if float(state.get("lease_until") or 0) > time.time() and not event.get("force"):
        return {"skipped": "lease_held", "lease_until": state["lease_until"]}
    state["lease_until"] = time.time() + BUDGET_S + 120
    state["runs"] = state.get("runs", 0) + 1
    _put_json(state_key, state)
    cat = _j(ROOT + "catalog.json") or {}
    cat.setdefault("leaves", {})
    fx = Fetch(key, float(event.get("rps") or RPS))
    store = Store()
    run = {"started_at": _now().isoformat(), "steps": []}
    pool = ThreadPoolExecutor(max_workers=WORKERS)
    try:
        t = time.time()
        leaves = walk_tree(fx, store, state, cat)
        run["steps"].append({"step": "tree", "seconds": round(time.time() - t, 1), "leaves": len(leaves), "calls": fx.calls})
        only = set(event.get("only") or [])
        order = sorted(leaves)
        if only:
            order = [p for p in order if p in only]
        # latest first for every leaf (cheap: one call each), then stream pages round-robin from the per-leaf cursor
        t = time.time()
        futures = {pool.submit(leaf_jobs, fx, store, state, cat, p, leaves[p], "latest"): p for p in order if _left() > _g(120)}
        for f in as_completed(futures):
            cat["leaves"][futures[f]] = f.result()
        run["steps"].append({"step": "latest", "seconds": round(time.time() - t, 1), "calls": fx.calls})
        t = time.time()
        start = int(state.get("stream_cursor") or 0) % max(1, len(order))
        i = start
        n = 0
        futures = {}
        while n < len(order) and _left() > _g(60):
            p = order[i]
            if not (state.get("leaf_state", {}).get(p) or {}).get("complete") or True:
                futures[pool.submit(leaf_jobs, fx, store, state, cat, p, leaves[p], "stream")] = p
            i = (i + 1) % len(order)
            n += 1
            if len(futures) >= WORKERS:
                for f in as_completed(futures):
                    v = f.result()
                    cat["leaves"][futures[f]] = v if v != "ok" else cat["leaves"].get(futures[f], "ok")
                futures = {}
        for f in as_completed(futures):
            v = f.result()
            cat["leaves"][futures[f]] = v if v != "ok" else cat["leaves"].get(futures[f], "ok")
        state["stream_cursor"] = i
        run["steps"].append({"step": "stream", "seconds": round(time.time() - t, 1), "calls": fx.calls, "leaves_touched": n})
        state["key_status"] = "ok" if fx.http.get("ok") else ("unauthorized" if (fx.http.get("http403", 0) + fx.http.get("http401", 0)) >= 3 and not fx.http.get("ok") else state.get("key_status"))
    finally:
        pool.shutdown(wait=True)
        state["objects"] = int(state.get("objects") or 0) + store.puts
        state["bytes"] = int(state.get("bytes") or 0) + store.bytes
        run["calls"], run["retries"], run["http"] = fx.calls, fx.retries, fx.http
        run["puts"], run["unchanged"], run["put_bytes"] = store.puts, store.unchanged, store.bytes
        run["seconds"], run["finished_at"] = round(time.time() - _t0, 1), _now().isoformat()
        state["last_run"] = run
        state["lease_until"] = 0
        state["updated_at"] = _now().isoformat()
        man = write_manifest(state, cat, fx, store, run)
        _put_json(state_key, state)
    return {"engine": ENGINE, "version": VERSION, "phase": state["phase"], "key_status": state["key_status"], "leaves": man["leaves"], "leaves_complete": man["leaves_complete"],
            "objects": man["objects"], "bytes": man["bytes"], "calls": run["calls"], "puts": run["puts"], "unchanged": run["unchanged"], "seconds": run["seconds"]}
