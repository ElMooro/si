"""
justhodl-provider-bank v1.0.0 -- Trading Economics, CoinMarketCap, NewsAPI, Nasdaq Data Link and CryptoQuant (splits)
banked verbatim on S3 under data/warm/provider-bank/<provider>/<dataset>/...

Why (Khalid, 2026-10-25: "fix it all, I want 100% of the data pulled and used by my system"): the audit found these keys
at 0-30% use.  Trading Economics was a FRED mirror plus a latest-value snapshot (its calendar with consensus, forecasts,
markets, news, comtrade, earnings and ratings never pulled); CoinMarketCap gave snapshots only; NewsAPI page 1 of two
endpoints; Nasdaq Data Link 24 FRED mirrors behind an untriggered handler; CryptoQuant all-exchange daily only.

How (the fmp-walker discipline, one engine for five small providers)
  * provider-bank-spec.json: providers (base URL, auth style - query key / header key / bearer -, per-second rate,
    DAILY CALL CAP, pagination style - offset or page -, where the rows live in the response) and datasets in four kinds:
    list, window (month windows from a floor), per_entity (rotation by cursor; chunked ids for batch endpoints; a
    {chunk} path segment iterates a second universe), latest_pages.
  * every dataset is a PROBE: the plan behind each key is not knowable from the repo, so 401/402/403/426 are recorded
    per dataset by http code, skipped for the day and re-probed tomorrow.  The card says what the key reaches.
  * per-provider daily call caps (CMC credits, NewsAPI requests/day) persist in state and reset at UTC midnight;
    a token bucket per provider; 429 halves the bucket for a minute.
  * write-if-changed (sha256 in S3 metadata), lease + 780 s budget, nothing self-chains; manifest with count_from
    fields, per-dataset cursors/laps/rotation, served/refused per provider, skipped list with reasons.

Keys from managed_secret per provider (env first, then SSM); never printed.
Schedule: EventBridge Scheduler justhodl-provider-bank-daily cron(53 21 * * ? *) UTC.
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
from datetime import date, datetime, timedelta, timezone

import boto3

from managed_secret import managed_secret

ENGINE = "justhodl-provider-bank"
VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com) provider-bank/" + VERSION, "Accept": "application/json"}
BUDGET_S = int(os.environ.get("PROVIDER_BANK_BUDGET_S", "780") or 780)
WORKERS = int(os.environ.get("PROVIDER_BANK_WORKERS", "6") or 6)
MAX_BYTES = int(os.environ.get("PROVIDER_BANK_MAX_BYTES", "60000000") or 60_000_000)
RPS_SCALE = float(os.environ.get("PROVIDER_BANK_RPS_SCALE", "1") or 1)          # tests only: run the fake servers at full speed
SAFE_RE = re.compile(r"[^A-Za-z0-9._=^-]")

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()


def _now():
    return datetime.now(timezone.utc)


def _today():
    return _now().date()


def _left():
    return BUDGET_S - (time.time() - _t0)


def _spec():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "provider-bank-spec.json"), "r", encoding="utf-8") as f:
        return json.load(f)


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
    return SAFE_RE.sub("_", str(s))[:140] or "_"


def _dig(obj, dotted):
    cur = obj
    for part in (dotted or "").split("."):
        if not part:
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


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
                rate = self.rps if now > self.slow_until else self.rps / 2
                self.tokens = min(max(1.0, rate), self.tokens + (now - self.t) * rate)
                self.t = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / rate
            time.sleep(min(wait, 0.5))

    def throttled(self):
        with self.lock:
            self.slow_until = time.time() + 60


class Provider:
    """One provider's auth, rate, daily cap and pagination; counts calls and verdicts."""

    def __init__(self, name, cfg, key, state):
        self.name, self.cfg, self.key = name, cfg, key
        self.bucket = Bucket(float(cfg.get("rps") or 1) * RPS_SCALE)
        self.cap = int(cfg.get("daily_call_cap") or 0)
        self.state = state                                        # {"day": iso, "calls": n}
        self.calls = 0
        self.retries = 0
        self.http = {}
        self.lock = threading.Lock()

    def _count(self, v):
        with self.lock:
            self.http[v] = self.http.get(v, 0) + 1

    def cap_left(self):
        if not self.cap:
            return 10 ** 9
        return self.cap - int(self.state.get("calls") or 0)

    def get(self, path, params, max_bytes=MAX_BYTES):
        if not self.key:
            self._count("no_key")
            return ("no_key", None, 0)
        if self.cap_left() <= 0:
            self._count("daily_cap")
            return ("daily_cap", None, 0)
        q = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        q.update(self.cfg.get("extra_query") or {})
        headers = dict(UA)
        auth = self.cfg.get("auth") or {}
        if auth.get("query"):
            q[auth["query"]] = self.key
        if auth.get("header"):
            headers[auth["header"]] = self.key
        if auth.get("bearer"):
            headers["Authorization"] = "Bearer " + self.key
        url = self.cfg["base"] + path + (("?" + urllib.parse.urlencode(q)) if q else "")
        delay = 2.0
        for attempt in range(4):
            if _left() < 20:
                return ("err:budget", None, 0)
            self.bucket.take()
            with self.lock:
                self.calls += 1
                self.state["calls"] = int(self.state.get("calls") or 0) + 1
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as r:
                    chunks, n = [], 0
                    while True:
                        c = r.read(1 << 20)
                        if not c:
                            break
                        n += len(c)
                        if n > max_bytes:
                            self._count("too_large")
                            return ("too_large", None, n)
                        chunks.append(c)
                body = b"".join(chunks)
                if not body.strip():
                    self._count("empty")
                    return ("empty", None, 0)
                try:
                    data = json.loads(body)
                except ValueError:
                    self._count("err:NotJSON")
                    return ("err:NotJSON", None, n)
                # provider-level error envelopes on a 200
                if isinstance(data, dict):
                    st = data.get("status")
                    if isinstance(st, dict) and st.get("error_code") not in (None, 0, "0"):
                        code = str(st.get("error_code"))
                        v = "api_error:" + code
                        self._count(v)
                        return (v, data, n)
                    if data.get("status") == "error" and data.get("code"):          # NewsAPI
                        v = "api_error:" + str(data.get("code"))
                        self._count(v)
                        return (v, data, n)
                    if "quandl_error" in data:
                        v = "api_error:" + str((data["quandl_error"] or {}).get("code"))
                        self._count(v)
                        return (v, data, n)
                if data in ([], {}):
                    self._count("empty")
                    return ("empty", None, 0)
                self._count("ok")
                return ("ok", data, n)
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
                if code == 404:
                    self._count("empty")
                    return ("empty", None, 0)
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

    def rows_of(self, data, results_key=None):
        rk = results_key or self.cfg.get("results_key")
        if rk:
            r = _dig(data, rk)
            if isinstance(r, list):
                return r
            if isinstance(r, dict):
                return [r]
        if isinstance(data, list):
            return data
        return None

    def paged(self, path, params, max_pages=10, results_key=None, max_bytes=MAX_BYTES):
        pg = self.cfg.get("paging")
        if not pg:
            v, data, nb = self.get(path, params, max_bytes)
            return (v, data, nb, 1 if v == "ok" else 0)
        rows, total, n = [], 0, 0
        cur = int(pg.get("start") or 1)
        size = int(pg.get("size") or 100)
        first = None
        for _ in range(max(1, int(max_pages))):
            p = dict(params or {})
            p[pg["param"]] = cur
            if pg.get("size_param"):
                p[pg["size_param"]] = p.get(pg["size_param"], size)
            v, data, nb = self.get(path, p, max_bytes)
            total += nb
            if v != "ok":
                if n:
                    break
                return (v, data, total, 0)
            first = first or data
            page_rows = self.rows_of(data, results_key)
            if page_rows is None:
                return ("ok", data, total, 1)
            rows.extend(page_rows)
            n += 1
            got = len(page_rows)
            if got < int(p.get(pg.get("size_param") or "", size) or size) * 0.9:
                break
            cur = cur + got if pg.get("mode") == "offset" else cur + 1
        if not rows:
            return ("empty", None, total, n)
        if n == 1:
            return ("ok", first, total, 1)
        return ("ok", rows, total, n)


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


def _field(row, names):
    for n in names:
        if isinstance(row, dict) and row.get(n) not in (None, ""):
            return row.get(n)
    return None


class Walker:
    def __init__(self, spec, keys, state, cat, event):
        self.spec = spec
        self.root = spec.get("root")
        self.state = state
        self.cat = cat
        self.today = _today()
        self.today_s = self.today.isoformat()
        caps = state.setdefault("caps", {})
        self.providers = {}
        for name, cfg in spec["providers"].items():
            c = caps.setdefault(name, {"day": self.today_s, "calls": 0})
            if c.get("day") != self.today_s:
                c["day"], c["calls"] = self.today_s, 0
            self.providers[name] = Provider(name, cfg, keys.get(name), c)
        self.store = Store()
        self.lists = {}
        self.universes = {}
        self.only = set(event.get("only") or [])
        self.only_providers = set(event.get("providers") or [])
        self.ds_state = state.setdefault("datasets", {})
        self.run = {"started_at": _now().isoformat(), "steps": [], "datasets": {}}
        self.pool = ThreadPoolExecutor(max_workers=WORKERS)

    def ds(self, d):
        return self.ds_state.setdefault(d["id"], {"objects": 0, "bytes": 0, "cursor": 0, "lap": 0, "ok": 0, "empty": 0, "fail": 0})

    def record(self, d, verdict, nbytes=0, rows=0, extra=None):
        e = self.cat.setdefault(d["id"], {"dataset": d["id"], "provider": d["provider"], "path": d["path"], "kind": d["kind"]})
        e["last_verdict"] = verdict
        e["last_at"] = _now().isoformat()
        if verdict == "ok":
            e["last_ok_at"] = e["last_at"]
            e["last_rows"] = rows
            e["ok"] = e.get("ok", 0) + 1
        elif verdict == "empty":
            e["empty"] = e.get("empty", 0) + 1
        else:
            e["fail"] = e.get("fail", 0) + 1
            e.setdefault("verdicts", {})[verdict] = e.get("verdicts", {}).get(verdict, 0) + 1
        if extra:
            e.update(extra)

    def bank(self, d, key, payload, meta):
        changed, nb = self.store.put(key, payload, meta)
        st = self.ds(d)
        if changed:
            st["objects"] = st.get("objects", 0) + 1
            st["bytes"] = st.get("bytes", 0) + nb
        st["ok"] = st.get("ok", 0) + 1
        rows = len(payload) if isinstance(payload, list) else 1
        self.record(d, "ok", nb, rows, {"last_key": key})
        return changed

    def params(self, d, extra=None):
        p = dict(d.get("params") or {})
        for k, v in list(p.items()):
            if isinstance(v, str) and v.startswith("@today"):
                off = int(v[6:] or 0) if v[6:] else 0
                p[k] = (self.today + timedelta(days=off)).isoformat()
        if extra:
            p.update(extra)
        return p

    def refused_today(self, d):
        e = self.cat.get(d["id"]) or {}
        v = str(e.get("last_verdict", ""))
        return (v.startswith("http4") and v != "http429" or v.startswith("api_error") or v in ("no_key",)) and str(e.get("last_at", ""))[:10] == self.today_s

    def active(self, d):
        if d.get("disabled"):
            return False
        if self.only and d["id"] not in self.only:
            return False
        if self.only_providers and d["provider"] not in self.only_providers:
            return False
        if self.refused_today(d):
            return False
        return True

    def key_path(self, d):
        return self.root + d["id"]

    # universes --------------------------------------------------------------------------------------------------------
    def build_universes(self):
        out = {}
        for name, u in (self.spec.get("universes") or {}).items():
            if "static" in u:
                out[name] = list(u["static"])
                continue
            srcs = u["from"] if isinstance(u["from"], list) else [u["from"]]
            vals = []
            for src in srcs:
                rows = self.lists.get(src)
                if rows is None:
                    rows = self.rows_from_bank(src)
                for r in rows or []:
                    v = _field(r, [u["field"]] + list(u.get("alt_fields") or []))
                    if v not in (None, ""):
                        vals.append(str(v))
            if u.get("accumulate"):
                acc = set(self.state.setdefault("accumulated", {}).get(name) or []) | set(vals)
                vals = sorted(acc)
                self.state["accumulated"][name] = vals
            vals = sorted(set(vals))
            if u.get("chunk"):
                k = int(u["chunk"])
                vals = [",".join(vals[i:i + k]) for i in range(0, len(vals), k)]
            out[name] = vals
        self.universes = out
        self.state["universe_counts"] = {k: len(v) for k, v in out.items()}

    def rows_from_bank(self, ds_id):
        d = next((x for x in self.spec["datasets"] if x["id"] == ds_id), None)
        if not d:
            return []
        if d["kind"] == "list":
            payload = _j(self.key_path(d) + "/latest.json.gz")
            return self.providers[d["provider"]].rows_of(payload, d.get("results_key")) or (payload if isinstance(payload, list) else [])
        if d["kind"] == "per_entity":                                 # union of the per-entity files banked so far (datasets-by-db)
            out = []
            for ent in self.universes.get(d["universe"]) or []:
                payload = _j(self.key_path(d) + "/" + safe(ent) + ".json.gz")
                rows = self.providers[d["provider"]].rows_of(payload, d.get("results_key")) or (payload if isinstance(payload, list) else [])
                for r in rows:
                    if isinstance(r, dict) and ds_id == "ndl/datasets-by-db" and r.get("database_code") and r.get("dataset_code"):
                        r = dict(r, code_full="%s/%s" % (r["database_code"], r["dataset_code"]))
                    out.append(r)
            return out
        return []

    # kinds ------------------------------------------------------------------------------------------------------------
    def do_list(self, d):
        pr = self.providers[d["provider"]]
        if d.get("paged"):
            v, payload, nb, npages = pr.paged(d["path"], self.params(d), int(d.get("max_pages") or 10), d.get("results_key"), int(d.get("max_bytes") or MAX_BYTES))
        else:
            v, payload, nb = pr.get(d["path"], self.params(d), int(d.get("max_bytes") or MAX_BYTES))
            npages = 1
        if v != "ok":
            self.record(d, v, nb)
            return
        rows = pr.rows_of(payload, d.get("results_key"))
        self.lists[d["id"]] = rows if rows is not None else (payload if isinstance(payload, list) else [payload])
        base = self.key_path(d)
        changed = self.bank(d, base + "/latest.json.gz", payload, {"provider": d["provider"], "path": d["path"], "pulled_at": self.today_s, "pages": npages})
        if changed or d.get("dated_always"):
            self.bank(d, base + "/daily/%s.json.gz" % self.today_s, payload, {"provider": d["provider"], "path": d["path"], "as_of": self.today_s})

    def do_window(self, d):
        pr = self.providers[d["provider"]]
        st = self.ds(d)
        fy, fm = (int(x) for x in (d.get("floor") or "2015-01").split("-"))
        cur = st.get("cursor_month") or "%04d-%02d" % (fy, fm)
        cy, cm = (int(x) for x in cur.split("-"))
        tm = (self.today.year, self.today.month)
        months = [tm]
        if self.today.day <= 5:
            months.append((tm[0] if tm[1] > 1 else tm[0] - 1, tm[1] - 1 if tm[1] > 1 else 12))
        n = 0
        while (cy, cm) <= tm and n < int(d.get("months_per_run") or 24):
            months.append((cy, cm))
            cm += 1
            if cm == 13:
                cy, cm = cy + 1, 1
            n += 1
        p1, p2 = d.get("date_params") or ["from", "to"]
        for (y, m) in sorted(set(months)):
            if _left() < 30 or pr.cap_left() <= 0:
                return
            start = date(y, m, 1)
            end = date(y + (m == 12), 1 if m == 12 else m + 1, 1) - timedelta(days=1)
            v, payload, nb, npages = pr.paged(d["path"], self.params(d, {p1: start.isoformat(), p2: end.isoformat()}), int(d.get("max_pages") or 10), d.get("results_key"))
            key = self.key_path(d) + "/%04d-%02d.json.gz" % (y, m)
            if v == "ok":
                self.bank(d, key, payload, {"provider": d["provider"], "path": d["path"], "from": start.isoformat(), "to": end.isoformat()})
            elif v == "empty":
                self.record(d, "empty")
            else:
                self.record(d, v, nb)
                if v.startswith("http4") or v.startswith("api_error"):
                    return
        st["cursor_month"] = "%04d-%02d" % (cy, cm) if (cy, cm) <= tm else "%04d-%02d" % tm

    def _entity_job(self, d, ent, chunk=None):
        pr = self.providers[d["provider"]]
        path = d["path"]
        params = {}
        if "{entity}" in path:
            path = path.replace("{entity}", urllib.parse.quote(str(ent), safe=",/?=&"))
        elif d.get("entity_param"):
            params[d["entity_param"]] = ent
        if chunk is not None:
            if "{chunk}" in path:
                path = path.replace("{chunk}", urllib.parse.quote(str(chunk), safe=",/?=&"))
                if "?" in path:                                    # a chunk may carry its own query (cq hourly paths)
                    path, _, qs = path.partition("?")
                    params.update(dict(urllib.parse.parse_qsl(qs)))
            else:
                params[d.get("chunk_param") or "chunk"] = chunk
        if "?" in path:
            path, _, qs = path.partition("?")
            params.update(dict(urllib.parse.parse_qsl(qs)))
        maxb = int(d.get("max_bytes") or MAX_BYTES)
        if d.get("paged"):
            v, payload, nb, npages = pr.paged(path, self.params(d, params), int(d.get("max_pages") or 10), d.get("results_key"), maxb)
        else:
            v, payload, nb = pr.get(path, self.params(d, params), maxb)
        key = self.key_path(d) + "/" + safe(ent)
        if chunk is not None:
            key += "/" + safe(chunk)
        if d.get("dated_always"):
            key += "/" + self.today_s
        key += ".json.gz"
        if v == "ok":
            self.bank(d, key, payload, {"provider": d["provider"], "path": d["path"], "entity": ent, "chunk": chunk or "", "pulled_at": self.today_s})
        elif v == "empty":
            st = self.ds(d)
            st["empty"] = st.get("empty", 0) + 1
            self.record(d, "empty")
        else:
            st = self.ds(d)
            st["fail"] = st.get("fail", 0) + 1
            self.record(d, v, nb)
        return v

    def do_per_entity(self, d, share_s):
        pr = self.providers[d["provider"]]
        st = self.ds(d)
        uni = list(self.universes.get(d["universe"]) or [])
        if not uni:
            self.record(d, "no_universe", 0, 0, {"universe": d.get("universe")})
            return
        chunks = list(self.universes.get(d["chunk_universe"]) or [None]) if d.get("chunk_universe") else [None]
        st["universe_size"] = len(uni)
        cursor = int(st.get("cursor") or 0) % len(uni)
        per_run = int(d.get("entities_per_run") or 0)
        t_end = min(time.time() + share_s, _t0 + BUDGET_S - 25)
        futures, n_done, start, stop = [], 0, cursor, False
        while time.time() < t_end and n_done < len(uni) and not stop and (not per_run or n_done < per_run) and pr.cap_left() > 0:
            ent = uni[cursor]
            for ch in chunks:
                futures.append(self.pool.submit(self._entity_job, d, ent, ch))
            cursor = (cursor + 1) % len(uni)
            n_done += 1
            if cursor == 0:
                st["lap"] = st.get("lap", 0) + 1
            if len(futures) >= WORKERS * 2:
                for f in as_completed(futures):
                    v = f.result()
                    if v in ("http401", "http402", "http403", "http426", "no_key", "daily_cap") or str(v).startswith("api_error"):
                        stop = True
                futures = []
        for f in as_completed(futures):
            f.result()
        st["cursor"] = cursor
        st["entities_last_run"] = n_done
        hist = st.setdefault("entities_by_day", {})
        hist[self.today_s] = hist.get(self.today_s, 0) + n_done
        for k in sorted(hist)[:-14]:
            del hist[k]
        self.run["datasets"][d["id"]] = {"entities": n_done, "from": start, "to": cursor}

    def run_all(self):
        spec = self.spec
        ds_all = [d for d in spec["datasets"] if self.active(d)]
        t = time.time()
        for d in [d for d in ds_all if d["kind"] == "list"]:
            if _left() < 60:
                break
            self.do_list(d)
        self.run["steps"].append({"step": "lists", "seconds": round(time.time() - t, 1)})
        # per-entity list sources needed by universes (datasets-by-db) run before the universes are built
        for d in [d for d in ds_all if d["kind"] == "per_entity" and any((u.get("from") == d["id"] or d["id"] in (u.get("from") or [])) for u in spec["universes"].values() if "from" in u)]:
            self.universes = {k: list(u["static"]) for k, u in spec["universes"].items() if "static" in u}
            self.do_per_entity(d, 60)
        self.build_universes()
        t = time.time()
        for d in [d for d in ds_all if d["kind"] == "window"]:
            if _left() < 60:
                break
            self.do_window(d)
        self.run["steps"].append({"step": "windows", "seconds": round(time.time() - t, 1)})
        pe = [d for d in ds_all if d["kind"] == "per_entity" and d["id"] not in self.run["datasets"]]
        pe.sort(key=lambda d: (int(d.get("priority") or 2), d["id"]))
        weights = {d["id"]: {0: 4, 1: 4, 2: 2, 3: 1}.get(int(d.get("priority") or 2), 1) for d in pe}
        total_w = sum(weights.values()) or 1
        remaining = _left() - 30
        t = time.time()
        for d in pe:
            if _left() < 40:
                break
            share = min(max(8.0, remaining * weights[d["id"]] / total_w), _left() - 35)
            if share <= 5:
                break
            t_ds = time.time()
            self.do_per_entity(d, share)
            self.run["datasets"].setdefault(d["id"], {})["seconds"] = round(time.time() - t_ds, 1)
        self.run["steps"].append({"step": "per_entity", "seconds": round(time.time() - t, 1)})
        self.pool.shutdown(wait=True)


def write_manifest(spec, state, cat, run):
    root = spec.get("root")
    dss = state.get("datasets") or {}
    objects = sum(int(v.get("objects") or 0) for v in dss.values())
    nbytes = sum(int(v.get("bytes") or 0) for v in dss.values())
    per, by_provider = {}, {}
    for d in spec["datasets"]:
        st = dss.get(d["id"]) or {}
        c = cat.get(d["id"]) or {}
        hist = st.get("entities_by_day") or {}
        per_day = (sum(hist.values()) / max(1, len(hist))) if hist else None
        per[d["id"]] = {"provider": d["provider"], "kind": d["kind"], "path": d["path"], "universe": d.get("universe"), "universe_size": st.get("universe_size"),
                        "objects": st.get("objects", 0), "bytes": st.get("bytes", 0), "lap": st.get("lap", 0), "cursor": st.get("cursor"), "cursor_month": st.get("cursor_month"),
                        "entities_per_day": round(per_day) if per_day else None,
                        "rotation_days": round(st["universe_size"] / per_day, 1) if per_day and st.get("universe_size") else None,
                        "last_verdict": c.get("last_verdict"), "last_ok_at": c.get("last_ok_at"), "verdicts": c.get("verdicts")}
        bp = by_provider.setdefault(d["provider"], {"served": [], "refused": [], "untested": [], "objects": 0, "bytes": 0})
        bp["objects"] += int(st.get("objects") or 0)
        bp["bytes"] += int(st.get("bytes") or 0)
        if c.get("last_ok_at"):
            bp["served"].append(d["id"])
        elif c.get("last_verdict"):
            bp["refused"].append([d["id"], c.get("last_verdict")])
        else:
            bp["untested"].append(d["id"])
    for name, bp in by_provider.items():
        bp["calls_today"] = (state.get("caps") or {}).get(name, {}).get("calls")
        bp["daily_call_cap"] = spec["providers"][name].get("daily_call_cap")
        bp["key_present"] = bool((state.get("keys_present") or {}).get(name))
    laps = all((dss.get(d["id"]) or {}).get("lap", 0) >= 1 for d in spec["datasets"] if d["kind"] == "per_entity" and (cat.get(d["id"]) or {}).get("last_ok_at"))
    windows = all(not (dss.get(d["id"]) or {}).get("cursor_month") or (dss.get(d["id"]) or {}).get("cursor_month") >= _today().strftime("%Y-%m") for d in spec["datasets"] if d["kind"] == "window" and (cat.get(d["id"]) or {}).get("last_ok_at"))
    phase = "STEADY" if (laps and windows and state.get("runs", 0) > 1) else "BACKFILL"
    state["phase"] = phase
    man = {"engine": ENGINE, "version": VERSION, "spec_version": spec.get("version"), "updated_at": _now().isoformat(),
           "objects": objects, "bytes": nbytes, "pages": objects, "pages_bytes": nbytes,
           "datasets": len([k for k, v in per.items() if v.get("objects")]), "datasets_declared": len(spec["datasets"]),
           "phase": phase, "universe_counts": state.get("universe_counts"), "by_provider": by_provider,
           "endpoints_served": sorted(k for k, v in cat.items() if v.get("last_ok_at")),
           "endpoints_refused": sorted((k, v.get("last_verdict")) for k, v in cat.items() if not v.get("last_ok_at") and v.get("last_verdict")),
           "skipped": spec.get("skipped"), "per_dataset": per, "run": run, "runs": state.get("runs"),
           "label": "Trading Economics, CoinMarketCap, NewsAPI, Nasdaq Data Link, CryptoQuant splits (verbatim JSON, gz) - never LIVE; plan verdicts per dataset; refreshed daily 21:53 UTC"}
    _put_json(root + "manifest.json", man)
    _put_json(root + "catalog.json", {"engine": ENGINE, "version": VERSION, "updated_at": man["updated_at"], "datasets": cat, "skipped": spec.get("skipped")})
    return man


def resolve_keys(spec):
    keys = {}
    for name, cfg in spec["providers"].items():
        try:
            keys[name] = managed_secret(tuple(cfg.get("key_env") or ()), tuple(cfg.get("key_ssm") or ())) or ""
        except Exception:  # noqa: BLE001
            keys[name] = ""
    return keys


def lambda_handler(event, context=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    spec = _spec()
    root = spec.get("root")
    keys = resolve_keys(spec)
    state_key = root + "_state/state.json"
    fresh = {"version": VERSION, "started_at": _now().isoformat(), "runs": 0, "phase": "BACKFILL", "datasets": {}, "caps": {}}
    state = fresh if event.get("reset_state") else (_j(state_key) or fresh)
    if float(state.get("lease_until") or 0) > time.time() and not event.get("force"):
        return {"skipped": "lease_held", "lease_until": state["lease_until"]}
    state["lease_until"] = time.time() + BUDGET_S + 120
    state["runs"] = state.get("runs", 0) + 1
    state["keys_present"] = {k: bool(v) for k, v in keys.items()}
    _put_json(state_key, state)
    cat = (_j(root + "catalog.json") or {}).get("datasets") or {}
    w = Walker(spec, keys, state, cat, event)
    try:
        w.run_all()
    finally:
        run = w.run
        run["providers"] = {n: {"calls": p.calls, "retries": p.retries, "http": p.http, "calls_today": p.state.get("calls"), "cap": p.cap, "key": bool(p.key)} for n, p in w.providers.items()}
        run["puts"], run["unchanged"], run["put_bytes"] = w.store.puts, w.store.unchanged, w.store.bytes
        run["seconds"], run["finished_at"] = round(time.time() - _t0, 1), _now().isoformat()
        state["last_run"] = run
        state["lease_until"] = 0
        state["updated_at"] = _now().isoformat()
        man = write_manifest(spec, state, cat, run)
        _put_json(state_key, state)
    return {"engine": ENGINE, "version": VERSION, "phase": state["phase"], "objects": man["objects"], "bytes": man["bytes"], "puts": run["puts"],
            "unchanged": run["unchanged"], "seconds": run["seconds"], "providers": run["providers"], "universe_counts": state.get("universe_counts")}
