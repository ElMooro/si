"""
justhodl-polygon-bank v1.1.0 -- the Polygon/Massive REST families the fleet never banked, verbatim on S3.

Why (Khalid, 2026-10-25: "fix it all, I want 100% of the data pulled and used by my system"): the provider audit found
the Polygon plans (Stocks delayed, Options, Futures Basic, ETF Global) ~70% used: the grouped-daily tape, options chains
for a few names, ETF Global for 116+300 funds and futures curves are banked; market status/holidays, the full ticker
universe (active + delisted, OTC, crypto, fx, indices), exchanges/conditions/types, the whole-market EOD snapshot and
gainers/losers, dividends and splits as a history, short interest (confirmed entitled by ops 6641), ticker details/
events/related companies, options contracts reference, minute bars and grouped crypto/fx files were never pulled, and
ETF Global flows only for 110-day windows.  This engine banks them from polygon-bank-spec.json.

How (same discipline as justhodl-fmp-walker)
  * kinds: list, daily_dated ({date} in the path), window (<date_field>.gte/.lte by month), per_entity ({entity} in the
    path or an entity query param; rotation by cursor over a universe), minute_bars (per chart symbol per session).
  * next_url pagination followed up to max_pages; results arrays merged; non-list responses stored verbatim.
  * one token bucket (POLYGON_BANK_RPS, 15/s; a 429 halves it for a minute) behind a thread pool; 780 s budget; lease;
    nothing self-chains (the daily schedule + the bounded conductor fmp-backfill.yml finish the backfill).
  * entitlement recorded by http code per dataset (403 NOT_AUTHORIZED is a fact on the card, not a crash); 404 on a
    date/ticker is 'empty'.  Write-if-changed via sha256 in S3 metadata.  Manifest with count_from fields.

Key: managed_secret(("POLYGON_KEY", "POLYGON_API_KEY", "MASSIVE_KEY"), ("/justhodl/polygon/api-key", "/justhodl/massive-api-key")).
Schedule: EventBridge Scheduler justhodl-polygon-bank-daily cron(41 22 * * ? *) UTC (after the US close + the 15-minute
delay; before fmp-bank 23:13 / fmp-walker 23:27).
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

ENGINE = "justhodl-polygon-bank"
VERSION = "1.1.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
HOST = os.environ.get("POLYGON_HOST", "https://api.polygon.io")
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com) polygon-bank/" + VERSION}
BUDGET_S = int(os.environ.get("POLYGON_BANK_BUDGET_S", "780") or 780)
WORKERS = int(os.environ.get("POLYGON_BANK_WORKERS", "4") or 4)
RPS = float(os.environ.get("POLYGON_BANK_RPS", "4") or 4)        # v1.1.0: the first run saw 624 x 429 at 15/s; polygon-full lives at ~3/s
MAX_BYTES = int(os.environ.get("POLYGON_BANK_MAX_BYTES", "40000000") or 40_000_000)
CHART_IDX_KEY = "data/warm/tv-bars/universe/_index.json"
CORE = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK.B", "JPM", "LLY", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ", "ABBV",
        "TSM", "ASML", "NFLX", "CRM", "AMD", "ORCL", "ADBE", "KO", "PEP", "WMT", "MRK", "BAC", "CVX", "LIN", "TMO", "MCD", "ACN", "CSCO", "ABT", "GE",
        "SPY", "QQQ", "IWM", "DIA", "HYG", "TLT", "GLD", "SLV", "USO", "XLF", "XLE", "XLK", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "VIX"]
US_TV_EXCH = {"US", "NASDAQ", "NYSE", "AMEX", "NYSEARCA", "BATS", "CBOE"}
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}([.\-][A-Z]{1,2})?$")
SAFE_RE = re.compile(r"[^A-Za-z0-9._=:^-]")

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()


def _now():
    return datetime.now(timezone.utc)


def _today():
    return _now().date()


def _left():
    return BUDGET_S - (time.time() - _t0)


def _spec():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "polygon-bank-spec.json"), "r", encoding="utf-8") as f:
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
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl=cache)


def _key():
    return managed_secret(("POLYGON_KEY", "POLYGON_API_KEY", "MASSIVE_KEY"), ("/justhodl/polygon/api-key", "/justhodl/massive-api-key"))


def safe(s):
    return SAFE_RE.sub("_", str(s))[:120] or "_"


class Bucket:
    def __init__(self, rps):
        self.rps = max(0.5, rps)
        self.tokens = 1.0
        self.t = time.time()
        self.slow_until = 0.0
        self.lock = threading.Lock()

    def take(self):
        while True:
            with self.lock:
                now = time.time()
                rate = self.rps if now > self.slow_until else self.rps / 4
                self.tokens = min(rate, self.tokens + (now - self.t) * rate)
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
    """GET one Polygon URL -> (verdict, payload, nbytes, next_url).  verdict ok | empty | http<code> | err:<Type> | too_large.

    payload: the parsed response (dict).  404 and a status OK with no rows are 'empty'; {"status":"NOT_AUTHORIZED"} on a
    200 body is reported as http403 (Polygon sometimes answers 200 with an error status).
    """

    def __init__(self, key, bucket):
        self.key = key
        self.bucket = bucket
        self.calls = 0
        self.retries = 0
        self.http = {}
        self.lock = threading.Lock()

    def _count(self, v):
        with self.lock:
            self.http[v] = self.http.get(v, 0) + 1

    def get(self, url, params=None, max_bytes=MAX_BYTES):
        q = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        q["apiKey"] = self.key
        full = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(q)
        delay = 1.5
        for attempt in range(4):
            if _left() < 20:
                return ("err:budget", None, 0, None)
            self.bucket.take()
            with self.lock:
                self.calls += 1
            try:
                with urllib.request.urlopen(urllib.request.Request(full, headers=UA), timeout=60) as r:
                    chunks, n = [], 0
                    while True:
                        c = r.read(1 << 20)
                        if not c:
                            break
                        n += len(c)
                        if n > max_bytes:
                            self._count("too_large")
                            return ("too_large", None, n, None)
                        chunks.append(c)
                body = b"".join(chunks)
                try:
                    data = json.loads(body) if body.strip() else {}
                except ValueError:
                    self._count("err:NotJSON")
                    return ("err:NotJSON", None, n, None)
                if isinstance(data, list):                      # /v1/marketstatus/upcoming answers a bare list
                    if not data:
                        self._count("empty")
                        return ("empty", None, 0, None)
                    self._count("ok")
                    return ("ok", {"status": "OK", "results": data}, n, None)
                status = str((data or {}).get("status") or "")
                if status in ("NOT_AUTHORIZED",):
                    self._count("http403")
                    return ("http403", data, n, None)
                if status in ("ERROR", "NOT_FOUND"):
                    self._count("fmp_error" if False else "api_error")
                    return ("api_error", data, n, None)
                res = data.get("results") if isinstance(data, dict) else None
                rows = res if isinstance(res, list) else (data.get("tickers") if isinstance(data, dict) and isinstance(data.get("tickers"), list) else None)
                if rows is not None and len(rows) == 0:
                    self._count("empty")
                    return ("empty", data, n, None)
                if not data:
                    self._count("empty")
                    return ("empty", None, 0, None)
                self._count("ok")
                return ("ok", data, n, data.get("next_url") if isinstance(data, dict) else None)
            except urllib.error.HTTPError as e:
                code = e.code
                if code == 404:
                    self._count("empty")
                    return ("empty", None, 0, None)
                if code == 429:
                    self.bucket.throttled()
                if (code == 429 or code >= 500) and attempt < 3:
                    with self.lock:
                        self.retries += 1
                    time.sleep(delay)
                    delay *= 2
                    continue
                self._count("http%d" % code)
                return ("http%d" % code, None, 0, None)
            except Exception as e:  # noqa: BLE001
                if attempt < 3:
                    with self.lock:
                        self.retries += 1
                    time.sleep(delay)
                    delay *= 2
                    continue
                self._count("err:" + type(e).__name__)
                return ("err:" + type(e).__name__, None, 0, None)
        return ("err:retries", None, 0, None)

    def paged(self, url, params, max_pages=10, max_bytes=MAX_BYTES):
        """Follow next_url; returns (verdict, payload, nbytes, npages) - payload is the merged results list (or the
        single response object when the endpoint does not return a results list)."""
        rows, total, n, nxt, first = [], 0, 0, None, None
        cur_url, cur_params = url, params
        for _ in range(max(1, int(max_pages))):
            v, data, nb, nxt = self.get(cur_url, cur_params, max_bytes)
            total += nb
            if v == "ok":
                first = first or data
                res = data.get("results")
                if isinstance(res, list):
                    rows.extend(res)
                    n += 1
                else:
                    return ("ok", data, total, 1)
                if not nxt:
                    break
                cur_url, cur_params = nxt, {}
                continue
            if v == "empty" and n:
                break
            if v == "err:budget":
                return ("err:budget", None, total, n)
            if n:
                break
            return (v, data, total, 0)
        return ("ok" if rows else "empty", rows if rows else None, total, n)


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


def chart_universe():
    chart = set(CORE)
    idx = _j(CHART_IDX_KEY) or {}
    entries = idx.get("symbols") or idx.get("entries") or idx if isinstance(idx, dict) else idx
    if isinstance(entries, dict):
        entries = list(entries.keys())
    for s in entries or []:
        s = str(s)
        if ":" in s:
            ex, _, bare = s.partition(":")
        elif "__" in s:
            ex, _, bare = s.partition("__")
        else:
            ex, bare = "US", s
        if ex.upper() in US_TV_EXCH and TICKER_RE.match(bare.upper()):
            chart.add(bare.upper().replace("-", "."))              # Polygon spells share classes BRK.B
    return sorted(x for x in chart if x != "VIX")


def build_universes(spec, lists):
    out = {}
    tick = lists.get("tickers-stocks-active") or []
    out["us_tickers"] = sorted({str(r.get("ticker")) for r in tick if isinstance(r, dict) and r.get("ticker")})
    out["us_etfs"] = sorted({str(r.get("ticker")) for r in tick if isinstance(r, dict) and r.get("ticker") and str(r.get("type") or "").upper() in ("ETF", "ETN", "ETV", "ETS")})
    out["chart"] = chart_universe()
    for name, u in (spec.get("universes") or {}).items():
        if "static" in u:
            out[name] = list(u["static"])
    return out


class Walker:
    def __init__(self, spec, key, state, cat, event):
        self.spec = spec
        self.root = spec.get("root", "data/warm/polygon-bank/")
        self.fx = Fetch(key, Bucket(float(event.get("rps") or RPS)))
        self.store = Store()
        self.state = state
        self.cat = cat
        self.today = _today()
        self.today_s = self.today.isoformat()
        self.lists = {}
        self.universes = {}
        self.only = set(event.get("only") or [])
        self.skip_kinds = set(event.get("skip_kinds") or [])
        self.ds_state = state.setdefault("datasets", {})
        self.run = {"started_at": _now().isoformat(), "steps": [], "datasets": {}}
        self.pool = ThreadPoolExecutor(max_workers=WORKERS)
        self.unauthorized = False

    def ds(self, d):
        return self.ds_state.setdefault(d["id"], {"objects": 0, "bytes": 0, "cursor": 0, "lap": 0, "ok": 0, "empty": 0, "fail": 0})

    def storm(self, d):
        return self.run.setdefault("storm", {}).get(d["id"], 0) >= 15

    def record(self, d, verdict, nbytes=0, rows=0, extra=None):
        e = self.cat.setdefault(d["id"], {"dataset": d["id"], "path": d["path"], "kind": d["kind"]})
        e["last_verdict"] = verdict
        e["last_at"] = _now().isoformat()
        storm = self.run.setdefault("storm", {})
        storm[d["id"]] = storm.get(d["id"], 0) + 1 if verdict == "http429" else 0
        if verdict == "ok":
            e["last_ok_at"] = e["last_at"]
            e["last_rows"] = rows
            e["ok"] = e.get("ok", 0) + 1
        elif verdict == "empty":
            e["empty"] = e.get("empty", 0) + 1
        else:
            e["fail"] = e.get("fail", 0) + 1
            e.setdefault("verdicts", {})[verdict] = e.get("verdicts", {}).get(verdict, 0) + 1
            if verdict == "http401":
                self.unauthorized = True
        if extra:
            e.update(extra)

    def bank(self, d, key, payload, meta):
        changed, nb = self.store.put(key, payload, meta)
        st = self.ds(d)
        if changed:
            st["objects"] = st.get("objects", 0) + 1
            st["bytes"] = st.get("bytes", 0) + nb
        st["ok"] = st.get("ok", 0) + 1
        rows = len(payload) if isinstance(payload, list) else (len(payload.get("tickers") or payload.get("results") or []) if isinstance(payload, dict) and isinstance(payload.get("tickers") or payload.get("results"), list) else 1)
        self.record(d, "ok", nb, rows, {"last_key": key})
        return changed

    def params(self, d, extra=None):
        p = dict(d.get("params") or {})
        for k, v in list(p.items()):
            if isinstance(v, str) and v.startswith("@"):
                p[k] = ",".join(self.universes.get(v[1:]) or [])
        if extra:
            p.update(extra)
        return p

    def url(self, d, entity=None, day=None):
        path = d["path"]
        if entity is not None:
            path = path.replace("{entity}", urllib.parse.quote(str(entity), safe=":."))
        if day is not None:
            path = path.replace("{date}", day)
        return HOST + path

    def active(self, d):
        if d.get("disabled"):
            return False
        if self.only and d["id"] not in self.only:
            return False
        if d["kind"] in self.skip_kinds:
            return False
        e = self.cat.get(d["id"]) or {}
        if str(e.get("last_verdict", "")).startswith("http4") and e.get("last_verdict") != "http429" and str(e.get("last_at", ""))[:10] == self.today_s:
            return False
        return True

    # kinds ------------------------------------------------------------------------------------------------------------
    def do_list(self, d):
        if d.get("paged"):
            v, payload, nb, npages = self.fx.paged(self.url(d), self.params(d), int(d.get("max_pages") or 10), int(d.get("max_bytes") or MAX_BYTES))
        else:
            v, payload, nb, _ = self.fx.get(self.url(d), self.params(d), int(d.get("max_bytes") or MAX_BYTES))
            npages = 1
        if v != "ok":
            self.record(d, v, nb)
            return
        rows = payload if isinstance(payload, list) else (payload.get("results") if isinstance(payload.get("results"), list) else (payload.get("tickers") if isinstance(payload.get("tickers"), list) else [payload]))
        self.lists[d["id"]] = rows
        base = self.root + d["id"]
        changed = self.bank(d, base + "/latest.json.gz", payload, {"path": d["path"], "pulled_at": self.today_s, "pages": npages})
        if changed or d.get("dated_always"):
            self.bank(d, base + "/daily/%s.json.gz" % self.today_s, payload, {"path": d["path"], "as_of": self.today_s, "pages": npages})

    def do_daily_dated(self, d):
        st = self.ds(d)
        floor = date.fromisoformat(d.get("floor") or "2020-01-01")
        cur = date.fromisoformat(st.get("cursor_date") or floor.isoformat())
        edge = self.today - timedelta(days=1)
        days = [edge - timedelta(days=i) for i in range(2)]
        n_back = 0
        while cur <= edge and n_back < int(d.get("days_per_run") or 60):
            days.append(cur)
            cur += timedelta(days=1)
            n_back += 1
        futures = []
        for day in sorted(set(days)):
            if _left() < 30 or self.storm(d):
                break
            if day.weekday() >= 5 and not d.get("weekends"):
                continue
            futures.append(self.pool.submit(self._dated_job, d, day.isoformat()))
        verdicts = {}
        for f in as_completed(futures):
            ds_s, v = f.result()
            verdicts[ds_s] = v
        bad = sorted(k for k, v in verdicts.items() if v not in ("ok", "empty"))
        new_cursor = cur
        if bad:
            first_bad = date.fromisoformat(bad[0])
            if first_bad < new_cursor:
                new_cursor = first_bad                                  # resume at the first unanswered day
        if verdicts:
            st["cursor_date"] = new_cursor.isoformat()

    def _dated_job(self, d, ds_s):
        if self.storm(d):
            return ds_s, "storm"
        v, payload, nb, _ = self.fx.get(self.url(d, day=ds_s), self.params(d))
        if v == "ok":
            self.bank(d, self.root + d["id"] + "/%s/%s.json.gz" % (ds_s[:4], ds_s), payload, {"path": d["path"], "date": ds_s})
        elif v == "empty":
            st = self.ds(d)
            st["empty"] = st.get("empty", 0) + 1
            self.record(d, "empty")
        else:
            self.record(d, v, nb)
        return ds_s, v

    def do_window(self, d):
        st = self.ds(d)
        fy, fm = (int(x) for x in (d.get("floor") or "2020-01").split("-"))
        cur = st.get("cursor_month") or "%04d-%02d" % (fy, fm)
        cy, cm = (int(x) for x in cur.split("-"))
        tm = (self.today.year, self.today.month)
        months = [tm]
        if self.today.day <= 5:
            months.append((tm[0] if tm[1] > 1 else tm[0] - 1, tm[1] - 1 if tm[1] > 1 else 12))
        n = 0
        while (cy, cm) <= tm and n < int(d.get("months_per_run") or 60):
            months.append((cy, cm))
            cm += 1
            if cm == 13:
                cy, cm = cy + 1, 1
            n += 1
        field = d.get("date_field") or "date"
        for (y, m) in sorted(set(months)):
            if _left() < 30:
                return
            start = date(y, m, 1)
            end = date(y + (m == 12), 1 if m == 12 else m + 1, 1) - timedelta(days=1)
            extra = {field + ".gte": start.isoformat(), field + ".lte": end.isoformat()}
            v, payload, nb, npages = self.fx.paged(self.url(d), self.params(d, extra), int(d.get("max_pages") or 20))
            key = self.root + d["id"] + "/%04d-%02d.json.gz" % (y, m)
            if v == "ok":
                self.bank(d, key, payload, {"path": d["path"], "from": extra[field + ".gte"], "to": extra[field + ".lte"], "pages": npages})
            elif v == "empty":
                self.record(d, "empty")
            else:
                self.record(d, v, nb)
                if v.startswith("http4") or v == "err:budget" or self.storm(d):
                    st["cursor_month"] = "%04d-%02d" % min((y, m), (cy, cm))
                    return
        st["cursor_month"] = "%04d-%02d" % (cy, cm) if (cy, cm) <= tm else "%04d-%02d" % tm

    def _entity_job(self, d, ent):
        ep = d.get("entity_param")
        if ep == "@path":
            url, params = self.url(d, entity=ent), self.params(d)
        else:
            url, params = self.url(d), self.params(d, {ep: ent})
        maxb = int(d.get("max_bytes") or MAX_BYTES)
        if d.get("paged"):
            v, payload, nb, npages = self.fx.paged(url, params, int(d.get("max_pages") or 10), maxb)
        else:
            v, payload, nb, _ = self.fx.get(url, params, maxb)
        key = self.root + d["id"] + "/" + safe(ent) + ("/" + self.today_s if d.get("dated_always") else "") + ".json.gz"
        if v == "ok":
            self.bank(d, key, payload, {"path": d["path"], "entity": ent, "pulled_at": self.today_s})
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
        st = self.ds(d)
        uni = list(self.universes.get(d["universe"]) or [])
        if not uni:
            self.record(d, "no_universe", 0, 0, {"universe": d.get("universe")})
            return
        st["universe_size"] = len(uni)
        cursor = int(st.get("cursor") or 0) % len(uni)
        t_end = min(time.time() + share_s, _t0 + BUDGET_S - 25)
        futures, n_done, start = [], 0, cursor
        stop = False
        while time.time() < t_end and n_done < len(uni) and not stop and not self.storm(d):
            futures.append(self.pool.submit(self._entity_job, d, uni[cursor]))
            cursor = (cursor + 1) % len(uni)
            n_done += 1
            if cursor == 0:
                st["lap"] = st.get("lap", 0) + 1
            if len(futures) >= WORKERS * 3:
                for f in as_completed(futures):
                    if f.result() in ("http401", "http403", "http402"):
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

    def do_minute_bars(self, d, share_s):
        st = self.ds(d)
        uni = list(self.universes.get(d["universe"]) or [])
        if not uni:
            self.record(d, "no_universe")
            return
        st["universe_size"] = len(uni)
        per = st.setdefault("per_symbol", {})
        back_days = int(d.get("backfill_days") or 730)
        edge = self.today - timedelta(days=1)
        t_end = min(time.time() + share_s, _t0 + BUDGET_S - 25)
        futures = []

        def job(sym, day):
            ds_s = day.isoformat()
            v, payload, nb, _ = self.fx.get(self.url(d, entity=sym, day=ds_s), self.params(d))
            if v == "ok":
                self.bank(d, self.root + d["id"] + "/%s/%s/%s.json.gz" % (safe(sym), ds_s[:7], ds_s), payload.get("results") if isinstance(payload, dict) and isinstance(payload.get("results"), list) else payload, {"path": d["path"], "symbol": sym, "date": ds_s})
            elif v == "empty":
                self.record(d, "empty")
            else:
                self.record(d, v, nb)
            return sym, ds_s, v

        def settle(fs):
            stop = False
            for f in as_completed(fs):
                s_, d_, v = f.result()
                r2 = per.setdefault(s_, {"newest": None, "oldest": None, "days": 0})
                if v == "ok":
                    r2["days"] = r2.get("days", 0) + 1
                if v in ("ok", "empty"):
                    if not r2.get("newest") or d_ > r2["newest"]:
                        r2["newest"] = d_
                    if not r2.get("oldest") or d_ < r2["oldest"]:
                        r2["oldest"] = d_
                if v in ("http401", "http403", "http402"):
                    stop = True
            return stop

        for sym in uni:                                             # newest side first
            if time.time() > t_end:
                break
            rec = per.setdefault(sym, {"newest": None, "oldest": None, "days": 0})
            day = date.fromisoformat(rec["newest"]) + timedelta(days=1) if rec.get("newest") else edge
            while day <= edge and time.time() < t_end:
                if day.weekday() < 5:
                    futures.append(self.pool.submit(job, sym, day))
                day += timedelta(days=1)
        if settle(futures):
            return
        futures = []
        while time.time() < t_end and not self.storm(d):            # then older days, round-robin
            progressed = False
            for sym in uni:
                if time.time() > t_end or self.storm(d):
                    break
                rec = per[sym]
                oldest = date.fromisoformat(rec["oldest"]) if rec.get("oldest") else edge
                if (edge - oldest).days >= back_days:
                    continue
                day = oldest - timedelta(days=1)
                while day.weekday() >= 5:
                    day -= timedelta(days=1)
                futures.append(self.pool.submit(job, sym, day))
                rec["oldest"] = day.isoformat()
                progressed = True
                if len(futures) >= WORKERS * 4:
                    if settle(futures):
                        return
                    futures = []
            if not progressed:
                break
        settle(futures)
        st["symbols_complete"] = sum(1 for s_ in uni if per.get(s_, {}).get("oldest") and (edge - date.fromisoformat(per[s_]["oldest"])).days >= back_days)

    def run_all(self):
        spec = self.spec
        ds_all = [d for d in spec["datasets"] if self.active(d)]
        t = time.time()
        for d in [d for d in ds_all if d["kind"] == "list"]:
            if _left() < 60:
                break
            self.do_list(d)
        self.run["steps"].append({"step": "lists", "seconds": round(time.time() - t, 1), "calls": self.fx.calls})
        h = self.fx.http
        if not h.get("ok") and (h.get("http401", 0) + h.get("http403", 0)) >= 3:
            self.unauthorized = True
        if self.unauthorized:
            return
        for d in spec["datasets"]:
            if d["kind"] == "list" and d["id"] not in self.lists:
                payload = _j(self.root + d["id"] + "/latest.json.gz")
                if isinstance(payload, list):
                    self.lists[d["id"]] = payload
                elif isinstance(payload, dict) and isinstance(payload.get("results"), list):
                    self.lists[d["id"]] = payload["results"]
        self.universes = build_universes(spec, self.lists)
        self.state["universe_counts"] = {k: len(v) for k, v in self.universes.items()}
        t = time.time()
        for d in [d for d in ds_all if d["kind"] in ("daily_dated", "window")]:
            if _left() < 60:
                break
            (self.do_daily_dated if d["kind"] == "daily_dated" else self.do_window)(d)
        self.run["steps"].append({"step": "dated+window", "seconds": round(time.time() - t, 1), "calls": self.fx.calls})
        pe = [d for d in ds_all if d["kind"] in ("per_entity", "minute_bars")]
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
            (self.do_minute_bars if d["kind"] == "minute_bars" else self.do_per_entity)(d, share)
            self.run["datasets"].setdefault(d["id"], {})["seconds"] = round(time.time() - t_ds, 1)
            if self.unauthorized:
                break
        self.run["steps"].append({"step": "per_entity", "seconds": round(time.time() - t, 1), "calls": self.fx.calls})
        self.pool.shutdown(wait=True)


def write_manifest(spec, state, cat, run):
    root = spec.get("root")
    dss = state.get("datasets") or {}
    objects = sum(int(v.get("objects") or 0) for v in dss.values())
    nbytes = sum(int(v.get("bytes") or 0) for v in dss.values())
    per = {}
    for d in spec["datasets"]:
        st = dss.get(d["id"]) or {}
        c = cat.get(d["id"]) or {}
        hist = st.get("entities_by_day") or {}
        per_day = (sum(hist.values()) / max(1, len(hist))) if hist else None
        per[d["id"]] = {"kind": d["kind"], "path": d["path"], "universe": d.get("universe"), "universe_size": st.get("universe_size"),
                        "objects": st.get("objects", 0), "bytes": st.get("bytes", 0), "lap": st.get("lap", 0), "cursor": st.get("cursor"),
                        "cursor_month": st.get("cursor_month"), "cursor_date": st.get("cursor_date"),
                        "entities_per_day": round(per_day) if per_day else None,
                        "rotation_days": round(st["universe_size"] / per_day, 1) if per_day and st.get("universe_size") else None,
                        "symbols_complete": st.get("symbols_complete"), "last_verdict": c.get("last_verdict"), "last_ok_at": c.get("last_ok_at"), "verdicts": c.get("verdicts")}
    served = sorted(k for k, v in cat.items() if v.get("last_ok_at"))
    refused = sorted((k, v.get("last_verdict")) for k, v in cat.items() if not v.get("last_ok_at") and v.get("last_verdict"))
    laps = all((dss.get(d["id"]) or {}).get("lap", 0) >= 1 for d in spec["datasets"] if d["kind"] == "per_entity" and (cat.get(d["id"]) or {}).get("last_ok_at"))
    bars = all((dss.get(d["id"]) or {}).get("symbols_complete", 0) >= (dss.get(d["id"]) or {}).get("universe_size", 1) for d in spec["datasets"] if d["kind"] == "minute_bars" and (cat.get(d["id"]) or {}).get("last_ok_at"))
    windows = all(not (dss.get(d["id"]) or {}).get("cursor_month") or (dss.get(d["id"]) or {}).get("cursor_month") >= _today().strftime("%Y-%m") for d in spec["datasets"] if d["kind"] == "window")
    dated = all(not (dss.get(d["id"]) or {}).get("cursor_date") or (dss.get(d["id"]) or {}).get("cursor_date") >= (_today() - timedelta(days=1)).isoformat() for d in spec["datasets"] if d["kind"] == "daily_dated")
    phase = "STEADY" if (laps and bars and windows and dated and state.get("runs", 0) > 1) else "BACKFILL"
    state["phase"] = phase
    man = {"engine": ENGINE, "version": VERSION, "spec_version": spec.get("version"), "updated_at": _now().isoformat(),
           "objects": objects, "bytes": nbytes, "pages": objects, "pages_bytes": nbytes,
           "datasets": len([k for k, v in per.items() if v.get("objects")]), "datasets_declared": len(spec["datasets"]),
           "phase": phase, "key_status": state.get("key_status"), "universe_counts": state.get("universe_counts"),
           "endpoints_served": served, "endpoints_refused": refused, "skipped": spec.get("skipped"), "per_dataset": per, "run": run, "runs": state.get("runs"),
           "label": "Polygon/Massive REST families beyond the tape (verbatim JSON, gz) - delayed plan, never LIVE; refreshed daily 22:41 UTC"}
    _put_json(root + "manifest.json", man)
    _put_json(root + "catalog.json", {"engine": ENGINE, "version": VERSION, "updated_at": man["updated_at"], "datasets": cat, "skipped": spec.get("skipped")})
    return man


def lambda_handler(event, context=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    spec = _spec()
    root = spec.get("root")
    key = _key()
    if not key:
        return {"error": "no Polygon key (env POLYGON_KEY / ssm /justhodl/polygon/api-key)"}
    state_key = root + "_state/state.json"
    fresh = {"version": VERSION, "started_at": _now().isoformat(), "runs": 0, "key_status": "unknown", "phase": "BACKFILL", "datasets": {}}
    state = fresh if event.get("reset_state") else (_j(state_key) or fresh)
    if float(state.get("lease_until") or 0) > time.time() and not event.get("force"):
        return {"skipped": "lease_held", "lease_until": state["lease_until"]}
    state["lease_until"] = time.time() + BUDGET_S + 120
    state["runs"] = state.get("runs", 0) + 1
    _put_json(state_key, state)
    cat = (_j(root + "catalog.json") or {}).get("datasets") or {}
    w = Walker(spec, key, state, cat, event)
    try:
        w.run_all()
        if w.unauthorized:
            state["key_status"] = "unauthorized"
        elif w.fx.http.get("ok"):
            state["key_status"] = "ok"
    finally:
        run = w.run
        run["calls"], run["retries"], run["http"] = w.fx.calls, w.fx.retries, w.fx.http
        run["puts"], run["unchanged"], run["put_bytes"] = w.store.puts, w.store.unchanged, w.store.bytes
        run["seconds"], run["finished_at"] = round(time.time() - _t0, 1), _now().isoformat()
        state["last_run"] = run
        state["lease_until"] = 0
        state["updated_at"] = _now().isoformat()
        man = write_manifest(spec, state, cat, run)
        _put_json(state_key, state)
    return {"engine": ENGINE, "version": VERSION, "phase": state["phase"], "key_status": state["key_status"], "objects": man["objects"], "bytes": man["bytes"],
            "calls": run["calls"], "puts": run["puts"], "unchanged": run["unchanged"], "seconds": run["seconds"], "universe_counts": state.get("universe_counts")}
