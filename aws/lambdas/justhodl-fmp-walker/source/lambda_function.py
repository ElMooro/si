"""
justhodl-fmp-walker v1.1.1 -- every non-bulk FMP Ultimate family, banked verbatim on S3 (data/warm/fmp/).

Why (Khalid, 2026-10-25: "fix it all, I want 100% of the data pulled and used by my system"): justhodl-fmp-bank banks the
bulk files (statements/ratios/metrics/profiles/EOD for the whole market).  Everything else FMP sells - per-symbol
histories (prices back to the 1960s, market cap, dividends, splits, earnings, estimates, targets, grades, ratings,
as-reported statements, segmentation, executives, employees, float), 13F (filers, dates, holder performance, symbol
positions, full extracts), insiders, Senate/House trades, transcripts, ETF holdings/weightings/dates, mutual-fund
disclosures, economics (indicators, COT, calendar, treasury), news/press releases, calendars (earnings/dividends/
splits/IPO), SEC filing feeds, market lists/hours/holidays, sector/industry history and 1-minute bars for the chart
universe - is described in fmp-walker-spec.json and walked by this one engine.

How
  * The spec (bundled) lists ~160 datasets in six kinds; endpoint names + query names come from the fmp_data 2.8.0 SDK
    catalog (262 /stable endpoints).  An endpoint the plan refuses is RECORDED by http code in the catalog and skipped
    for the day - never guessed around, never fatal.
  * One invoke = one wall-clock budget (FMP_WALKER_BUDGET_S, default 780 s): lists first (they define the universes),
    then the latest/daily/window feeds (the day's increment), then per-entity datasets by priority, each advancing its own
    cursor through its universe (rotation).  State is written at the end; the next run resumes.  Nothing self-chains:
    the daily Scheduler schedule + the bounded conductor (.github/workflows/fmp-backfill.yml, until STEADY) finish the
    backfill over days.
  * Throughput: a thread pool (FMP_WALKER_WORKERS, 12) behind one token bucket (FMP_WALKER_RPS, 18/s = 1,080/min of the
    plan's 3,000/min; a 429 halves the rate for a minute).  Each HTTP call -> one S3 object (pages merged).
  * Write-if-changed: the object's sha256 travels in S3 metadata; an unchanged body is never re-put (no new version,
    LastModified stays a true change signal).  Verbatim rows as served (JSON, gz); pages are concatenated into one array.
  * Universes: tierA = US-listed (NASDAQ/NYSE/AMEX...) actively trading equities + US ETFs; all = every listed symbol;
    chart = banked chart names + core 40; ciks of 13F filers accumulate from the latest-filings feed; senators from the
    profile feed; sectors/industries/exchanges/COT symbols from their lists.
  * Manifest (count_from for the data.html card, never a prefix walk) carries objects/bytes/datasets, per-dataset laps,
    cursor, entities/day and the measured rotation in days, served/not-served endpoints with http verdicts, skipped
    endpoints with reasons.

Key: managed_secret(("FMP_KEY", "FMP_API_KEY"), ("/justhodl/fmp/api-key",)).  Never printed.
Schedule: EventBridge Scheduler justhodl-fmp-walker-daily cron(27 23 * * ? *) UTC (after fmp-bank 23:13).
Cost: ~13 min x 1 GB a day (~$0.35/month of Lambda) + S3 PUTs for changed objects; backfill days add the conductor's
rounds (bounded; <= $1.5/day; under the governor's $50/engine CAP and anomaly floors by design).
"""
from __future__ import annotations

import gc
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
from botocore.config import Config

from managed_secret import managed_secret

ENGINE = "justhodl-fmp-walker"
VERSION = "1.1.1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
STABLE = "https://financialmodelingprep.com/stable"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com) fmp-walker/" + VERSION}
BUDGET_S = int(os.environ.get("FMP_WALKER_BUDGET_S", "780") or 780)
WORKERS = int(os.environ.get("FMP_WALKER_WORKERS", "12") or 12)
RPS = float(os.environ.get("FMP_WALKER_RPS", "18") or 18)
PER_ENTITY_MAX_BYTES = int(os.environ.get("FMP_WALKER_MAX_BYTES", "25000000") or 25_000_000)
CHART_IDX_KEY = "data/warm/tv-bars/universe/_index.json"
CORE = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK-B", "JPM", "LLY", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ", "ABBV",
        "TSM", "ASML", "NFLX", "CRM", "AMD", "ORCL", "ADBE", "KO", "PEP", "WMT", "MRK", "BAC", "CVX", "LIN", "TMO", "MCD", "ACN", "CSCO", "ABT", "GE"]
US_EXCH = {"NASDAQ", "NYSE", "AMEX", "NYSEARCA", "NYSE ARCA", "BATS", "CBOE", "NASDAQ GLOBAL SELECT", "NASDAQ GLOBAL MARKET", "NASDAQ CAPITAL MARKET",
           "NEW YORK STOCK EXCHANGE", "NEW YORK STOCK EXCHANGE ARCA", "AMERICAN STOCK EXCHANGE", "NASDAQ STOCK EXCHANGE", "CBOE US", "BATS EXCHANGE", "US"}
US_TV_EXCH = {"US", "NASDAQ", "NYSE", "AMEX", "NYSEARCA", "BATS", "CBOE"}
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}([.\-][A-Z]{1,2})?$")
SAFE_RE = re.compile(r"[^A-Za-z0-9._=^-]")

s3 = boto3.client("s3", region_name="us-east-1", config=Config(max_pool_connections=40, retries={"max_attempts": 6, "mode": "standard"}))
_t0 = time.time()
# lists the universes read; everything else is written and dropped (v1.1.0: retaining every paged feed - cik-list,
# shares-float-all, industry classification, delisted - held ~1 GB of rows and the first live run died without a state write)
KEEP_LISTS = {"stock-list", "etf-list", "actively-trading-list", "earnings-transcript-list", "institutional-ownership/latest", "senate-profile",
              "available-exchanges", "available-sectors", "available-industries", "commitment-of-traders-list", "commodities-list", "forex-list",
              "cryptocurrency-list", "quotes/crypto", "index-list"}


def _now():
    return datetime.now(timezone.utc)


def _today():
    return _now().date()


def _left():
    return BUDGET_S - (time.time() - _t0)


def _g(seconds):
    """Guard margins scale with the budget (780 s in production; small in tests) so every loop stays bounded."""
    return min(float(seconds), BUDGET_S * float(seconds) / 780.0)


def _spec():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "fmp-walker-spec.json"), "r", encoding="utf-8") as f:
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
    return managed_secret(("FMP_KEY", "FMP_API_KEY"), ("/justhodl/fmp/api-key",))


def safe(s):
    return SAFE_RE.sub("_", str(s))[:120] or "_"


# ---------------------------------------------------------------- rate limit + fetch ---------------------------------
class Bucket:
    """Token bucket shared by the worker threads; a 429 halves the rate for 60 s (FMP's limit is per minute)."""

    def __init__(self, rps):
        self.rps = self.base = max(0.5, rps)
        self.tokens = 1.0
        self.t = time.time()
        self.slow_until = 0.0
        self.lock = threading.Lock()

    def take(self):
        while True:
            with self.lock:
                now = time.time()
                rate = self.rps if now > self.slow_until else self.rps / 2
                self.tokens = min(rate, self.tokens + (now - self.t) * rate)
                self.t = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / rate
            time.sleep(min(wait, 0.5))

    def throttled(self):
        with self.lock:
            self.slow_until = time.time() + 60


class Fetch:
    """GET one /stable URL -> (verdict, rows_or_None, nbytes).  verdict: ok | empty | http<code> | err:<Type> | too_large.

    rows is the parsed JSON (list, or dict for the few object responses).  An FMP error body ({"Error Message":...})
    is reported as http4xx-like 'fmp_error', never as data.
    """

    def __init__(self, key, bucket):
        self.key = key
        self.bucket = bucket
        self.calls = 0
        self.retries = 0
        self.http = {}
        self.lock = threading.Lock()

    def _count(self, verdict):
        with self.lock:
            self.http[verdict] = self.http.get(verdict, 0) + 1

    def get(self, endpoint, params, max_bytes=PER_ENTITY_MAX_BYTES):
        q = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        q["apikey"] = self.key
        url = "%s/%s?%s" % (STABLE, endpoint, urllib.parse.urlencode(q))
        delay = 1.5
        for attempt in range(4):
            if _left() < _g(20):
                return ("err:budget", None, 0)
            self.bucket.take()
            with self.lock:
                self.calls += 1
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
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
                if isinstance(data, dict) and ("Error Message" in data or "error" in data and len(data) <= 2):
                    self._count("fmp_error")
                    return ("fmp_error", data, n)
                if data == [] or data == {}:
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

    def pages(self, endpoint, params, limit=None, max_pages=10, page_required=False, max_bytes=PER_ENTITY_MAX_BYTES):
        """Follow page=0.. until a short page / empty / max_pages.  Returns (verdict, rows, nbytes, npages)."""
        rows, total, n = [], 0, 0
        for page in range(max_pages):
            p = dict(params or {})
            if page or page_required or limit:
                p["page"] = page
            if limit:
                p["limit"] = limit
            v, data, nb = self.get(endpoint, p, max_bytes)
            total += nb
            if v == "ok" and isinstance(data, list):
                rows.extend(data)
                n += 1
                if limit and len(data) < int(limit) * 0.9:
                    break
                if not limit and len(data) < 10:
                    break
                continue
            if v == "empty" and n:
                break
            if v == "ok" and not isinstance(data, list):           # object response: one page
                return ("ok", data, total, 1)
            if v == "err:budget":                                 # out of time mid-way: never bank a partial answer
                return ("err:budget", None, total, n)
            if n:                                                 # provider error after some pages: keep what we have
                break
            return (v, data if v == "fmp_error" else None, total, 0)
        return ("ok" if rows else "empty", rows if rows else None, total, n)


# ---------------------------------------------------------------- S3 write-if-changed --------------------------------
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

    def exists(self, key):
        try:
            s3.head_object(Bucket=BUCKET, Key=key)
            return True
        except Exception:  # noqa: BLE001
            return False


# ---------------------------------------------------------------- universes -----------------------------------------
def _field(row, names):
    for n in names:
        if isinstance(row, dict) and row.get(n) not in (None, ""):
            return row.get(n)
    return None


def _is_us(row):
    ex = str(_field(row, ["exchangeShortName", "exchange", "exchangeName"]) or "").upper().strip()
    return ex in US_EXCH or ex.startswith("NASDAQ") or ex.startswith("NYSE") or ex in ("AMEX",)


def _profiles_us(fx):
    """US-listed, actively trading symbols and US ETFs from the bulk bank's profile-bulk (exchange / isActivelyTrading /
    isEtf live there, not on /stable/stock-list - v1.1.1: the first live run saw tierA == all == 94,118).  Falls back to
    one company-screener call per US exchange when the bulk bank is not readable."""
    tier, etfs = set(), set()
    try:
        import fmp_bank
        rows = fmp_bank.snapshot("profile") or []
    except Exception:  # noqa: BLE001
        rows = []
    for r in rows:
        if not isinstance(r, dict) or not r.get("symbol"):
            continue
        active = str(r.get("isActivelyTrading", r.get("actively_trading", "true"))).lower() in ("true", "1", "yes", "")
        if _is_us(r) and active:
            tier.add(str(r["symbol"]))
            if str(r.get("isEtf", r.get("is_etf", ""))).lower() in ("true", "1", "yes"):
                etfs.add(str(r["symbol"]))
    if not tier and fx is not None:
        for ex in ("NASDAQ", "NYSE", "AMEX"):
            v, data, nb = fx.get("company-screener", {"exchange": ex, "isActivelyTrading": "true", "limit": 10000})
            for r in data if (v == "ok" and isinstance(data, list)) else []:
                if isinstance(r, dict) and r.get("symbol"):
                    tier.add(str(r["symbol"]))
                    if str(r.get("isEtf", "")).lower() in ("true", "1", "yes"):
                        etfs.add(str(r["symbol"]))
    return tier, etfs


def build_universes(spec, root, state, lists, fx=None):
    """lists: {dataset_id: rows} for the list datasets pulled this run (or banked latest)."""
    out = {}
    stock = lists.get("stock-list") or []
    etfs = lists.get("etf-list") or []
    active = {str(r.get("symbol")) for r in (lists.get("actively-trading-list") or []) if isinstance(r, dict) and r.get("symbol")}
    all_syms = sorted({str(r.get("symbol")) for r in stock + etfs if isinstance(r, dict) and r.get("symbol")})
    tier = set()
    for r in stock:
        if isinstance(r, dict) and r.get("symbol") and _is_us(r) and (not active or str(r["symbol"]) in active):
            tier.add(str(r["symbol"]))
    etf_us = {str(r["symbol"]) for r in etfs if isinstance(r, dict) and r.get("symbol") and _is_us(r)}
    if not tier:                                                 # /stable/stock-list carries no exchange: use profile-bulk
        tier, etf_us2 = _profiles_us(fx)
        etf_us = etf_us | etf_us2
        if active:
            tier = {t for t in tier if t in active} or tier
    tier = sorted(set(tier) | set(etf_us))
    state["universe_source"] = "stock-list" if any(_is_us(r) for r in stock[:200] if isinstance(r, dict)) else ("profile-bulk" if tier and len(tier) < len(all_syms) else "all-fallback")
    if not tier and all_syms:                                    # nothing knows the exchanges: the whole list, declared as such
        tier = all_syms
    out["all"], out["tierA"], out["etfs_us"] = all_syms, tier, sorted(etf_us) or tier
    tset = set(tier)
    out["transcript_symbols"] = sorted({str(r.get("symbol")) for r in (lists.get("earnings-transcript-list") or []) if isinstance(r, dict) and r.get("symbol")} & tset) or tier
    # chart universe: banked chart names (US) + core 40
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
            chart.add(bare.upper())
    out["chart"] = sorted(chart)
    # accumulated 13F filer ciks
    ciks = set(state.get("holder_ciks") or [])
    for r in lists.get("institutional-ownership/latest") or []:
        c = _field(r, ["cik"])
        if c:
            ciks.add(str(c))
    state["holder_ciks"] = sorted(ciks)
    out["holder_ciks"] = sorted(ciks)
    out["senate_ids"] = sorted({str(_field(r, ["senateId", "bioguideId", "id"])) for r in (lists.get("senate-profile") or []) if _field(r, ["senateId", "bioguideId", "id"])})
    out["exchanges"] = sorted({str(_field(r, ["exchange", "name"])) for r in (lists.get("available-exchanges") or []) if _field(r, ["exchange", "name"])})
    out["sectors"] = sorted({str(_field(r, ["sector"])) for r in (lists.get("available-sectors") or []) if _field(r, ["sector"])})
    out["industries"] = sorted({str(_field(r, ["industry"])) for r in (lists.get("available-industries") or []) if _field(r, ["industry"])})
    out["cot_symbols"] = sorted({str(_field(r, ["symbol", "tradingSymbol"])) for r in (lists.get("commitment-of-traders-list") or []) if _field(r, ["symbol", "tradingSymbol"])})
    out["commodities"] = sorted({str(r.get("symbol")) for r in (lists.get("commodities-list") or []) if isinstance(r, dict) and r.get("symbol")})
    out["forex"] = sorted({str(r.get("symbol")) for r in (lists.get("forex-list") or []) if isinstance(r, dict) and r.get("symbol")})
    cryptos = [r for r in (lists.get("cryptocurrency-list") or []) if isinstance(r, dict) and r.get("symbol")]
    out["cryptos"] = sorted({str(r["symbol"]) for r in cryptos})
    q = [r for r in (lists.get("quotes/crypto") or []) if isinstance(r, dict) and r.get("symbol")]
    q.sort(key=lambda r: -(float(r.get("marketCap") or 0) if str(r.get("marketCap") or "").replace(".", "", 1).replace("-", "", 1).isdigit() else 0))
    out["cryptos_top"] = [str(r["symbol"]) for r in q[:200]] or out["cryptos"][:200]
    out["indexes"] = sorted({str(r.get("symbol")) for r in (lists.get("index-list") or []) if isinstance(r, dict) and r.get("symbol")})
    for name, u in (spec.get("universes") or {}).items():
        if "static" in u:
            out[name] = list(u["static"])
    y0 = _today().year
    out["years_2010"] = [str(y) for y in range(2010, y0 + 1)]
    out["quarters_2013"] = ["%d-Q%d" % (y, qq) for y in range(2013, y0 + 1) for qq in (1, 2, 3, 4) if (y, qq) <= (y0, (_today().month - 1) // 3 + 1)]
    return out


def trailing_quarters(n):
    t = _today()
    y, q = t.year, (t.month - 1) // 3 + 1
    q -= 1                                                        # the last COMPLETED quarter first
    if q == 0:
        y, q = y - 1, 4
    out = []
    for _ in range(n):
        out.append((y, q))
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    return out


# ---------------------------------------------------------------- the walk ------------------------------------------
class Walker:
    def __init__(self, spec, key, state, cat, event):
        self.spec = spec
        self.root = spec.get("root", "data/warm/fmp/")
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

    # -- bookkeeping ---------------------------------------------------------------------------------------------------
    def ds(self, d):
        return self.ds_state.setdefault(d["id"], {"objects": 0, "bytes": 0, "cursor": 0, "lap": 0, "ok": 0, "empty": 0, "fail": 0})

    def record(self, d, verdict, nbytes=0, rows=0, extra=None):
        e = self.cat.setdefault(d["id"], {"dataset": d["id"], "endpoint": d["endpoint"], "kind": d["kind"]})
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
            if verdict in ("http401", "http403"):
                self.unauthorized = self.unauthorized or verdict == "http401"
        if extra:
            e.update(extra)

    def bank(self, d, key, rows, meta, verdict_rows=None):
        changed, nb = self.store.put(key, rows, meta)
        st = self.ds(d)
        if changed:
            st["objects"] = st.get("objects", 0) + 1
            st["bytes"] = st.get("bytes", 0) + nb
        st["ok"] = st.get("ok", 0) + 1
        self.record(d, "ok", nb, len(rows) if isinstance(rows, list) else 1, {"last_key": key})
        return changed

    def params(self, d, extra=None):
        p = dict(d.get("params") or {})
        for k, v in list(p.items()):
            if v == "@today":
                p[k] = self.today_s
        if extra:
            p.update(extra)
        return p

    def active(self, d):
        if d.get("disabled"):
            return False
        if self.only and d["id"] not in self.only:
            return False
        if d["kind"] in self.skip_kinds:
            return False
        e = self.cat.get(d["id"]) or {}
        if e.get("last_verdict", "").startswith("http4") and str(e.get("last_at", ""))[:10] == self.today_s and e.get("last_verdict") != "http429":
            return False                                          # refused today: recorded, try again tomorrow
        return True

    # -- kinds ---------------------------------------------------------------------------------------------------------
    def do_list(self, d):
        v, data, nb = self.fx.get(d["endpoint"], self.params(d))
        if v != "ok":
            self.record(d, v, nb)
            return
        if d["endpoint"] in KEEP_LISTS or d["id"] in KEEP_LISTS:
            self.lists[d["endpoint"]] = data if isinstance(data, list) else [data]
            self.lists[d["id"]] = self.lists[d["endpoint"]]
        base = self.root + d["id"]
        changed = self.bank(d, base + "/latest.json.gz", data, {"endpoint": d["endpoint"], "pulled_at": self.today_s})
        if changed or d.get("dated_always"):
            self.bank(d, base + "/daily/%s.json.gz" % self.today_s, data, {"endpoint": d["endpoint"], "as_of": self.today_s})
        del data
        gc.collect()

    def do_latest_pages(self, d):
        st = self.ds(d)
        if d.get("per_page_files"):
            return self.do_paged_reference(d)
        prev = _j(self.root + d["id"] + "/%s.json.gz" % st.get("last_day", "")) if st.get("last_day") else None
        seen = {hashlib.sha1(json.dumps(r, sort_keys=True, default=str).encode()).hexdigest() for r in (prev or [])} if isinstance(prev, list) else set()
        rows, total, npages = [], 0, 0
        for page in range(int(d.get("max_pages") or 10)):
            p = self.params(d, {"page": page})
            if d.get("limit"):
                p["limit"] = d["limit"]
            v, data, nb = self.fx.get(d["endpoint"], p)
            total += nb
            if v != "ok" or not isinstance(data, list):
                if not npages:
                    self.record(d, v, nb)
                    return
                break
            npages += 1
            fresh = [r for r in data if hashlib.sha1(json.dumps(r, sort_keys=True, default=str).encode()).hexdigest() not in seen]
            rows.extend(data)
            if seen and len(fresh) < max(1, len(data) // 10):      # the page is almost entirely yesterday's rows
                break
            if d.get("limit") and len(data) < int(d["limit"]) * 0.9:
                break
            if not d.get("limit") and len(data) < 10:
                break
        if not rows:
            self.record(d, "empty")
            return
        key = self.root + d["id"] + "/%s.json.gz" % self.today_s
        self.bank(d, key, rows, {"endpoint": d["endpoint"], "as_of": self.today_s, "pages": npages})
        st["last_day"] = self.today_s
        st["last_pages"] = npages
        if d["endpoint"] in KEEP_LISTS or d["id"] in KEEP_LISTS:
            self.lists[d["endpoint"]] = rows
            self.lists[d["id"]] = rows
        del rows, seen
        gc.collect()

    def do_paged_reference(self, d):
        """A big paged reference feed (cik-list, shares-float-all, ...): one object per page under <id>/<date>/pNNN,
        nothing merged or retained - memory stays at one page."""
        st = self.ds(d)
        npages = 0
        for page in range(int(d.get("max_pages") or 10)):
            if _left() < _g(30):
                break
            p = self.params(d, {"page": page})
            if d.get("limit"):
                p["limit"] = d["limit"]
            v, data, nb = self.fx.get(d["endpoint"], p)
            if v != "ok" or not isinstance(data, list):
                if not npages:
                    self.record(d, v, nb)
                break
            npages += 1
            self.bank(d, self.root + d["id"] + "/%s/p%03d.json.gz" % (self.today_s, page), data, {"endpoint": d["endpoint"], "as_of": self.today_s, "page": page})
            short = (d.get("limit") and len(data) < int(d["limit"]) * 0.9) or (not d.get("limit") and len(data) < 10)
            del data
            if short:
                break
        if npages:
            st["last_day"] = self.today_s
            st["last_pages"] = npages
        gc.collect()

    def do_daily_dated(self, d):
        st = self.ds(d)
        floor = date.fromisoformat(d.get("floor") or "2026-10-01")
        cur = date.fromisoformat(st.get("cursor_date") or floor.isoformat())
        edge = self.today - timedelta(days=1) if _now().hour < 23 else self.today
        # always re-pull the last 3 days (late rows), then advance the backfill cursor
        days = [edge - timedelta(days=i) for i in range(3)]
        n_back = 0
        while cur <= edge and n_back < int(d.get("days_per_run") or 120):
            days.append(cur)
            cur += timedelta(days=1)
            n_back += 1
        def job(day):
            ds_s = day.isoformat()
            if d.get("paged"):
                v, data, nb, npages = self.fx.pages(d["endpoint"], self.params(d, {"date": ds_s}), limit=d.get("limit"), max_pages=int(d.get("max_pages") or 20))
            else:
                v, data, nb = self.fx.get(d["endpoint"], self.params(d, {"date": ds_s}))
            if v == "ok":
                self.bank(d, self.root + d["id"] + "/%s/%s.json.gz" % (ds_s[:4], ds_s), data, {"endpoint": d["endpoint"], "date": ds_s})
            elif v == "empty":
                st["empty"] = st.get("empty", 0) + 1
                self.record(d, "empty")
            else:
                self.record(d, v, nb)
            return ds_s, v

        futures = []
        for day in sorted(set(days)):
            if _left() < _g(30):
                break
            if day.weekday() >= 5 and d["endpoint"] != "insider-trading/latest":
                continue
            futures.append(self.pool.submit(job, day))
        verdicts = {}
        for f in as_completed(futures):
            ds_s, v = f.result()
            verdicts[ds_s] = v
        # the cursor advances only past days that were answered (ok/empty); a budget stop or refusal is retried next run
        ok_days = sorted(k for k, v in verdicts.items() if v in ("ok", "empty"))
        bad_days = sorted(k for k, v in verdicts.items() if v not in ("ok", "empty"))
        new_cursor = cur
        if bad_days:
            first_bad = date.fromisoformat(bad_days[0])
            if first_bad < new_cursor:
                new_cursor = first_bad
        if ok_days or not st.get("cursor_date"):
            st["cursor_date"] = new_cursor.isoformat()

    def do_window(self, d):
        st = self.ds(d)
        floor_y, floor_m = (int(x) for x in (d.get("floor") or "2020-01").split("-"))
        cur = st.get("cursor_month") or "%04d-%02d" % (floor_y, floor_m)
        cy, cm = (int(x) for x in cur.split("-"))
        tm = (self.today.year, self.today.month)
        months = []
        # re-pull current month (+ previous month during the first 5 days), then advance the backfill cursor
        months.append(tm)
        if self.today.day <= 5:
            months.append((tm[0] if tm[1] > 1 else tm[0] - 1, tm[1] - 1 if tm[1] > 1 else 12))
        n = 0
        while (cy, cm) <= tm and n < int(d.get("months_per_run") or 24):
            months.append((cy, cm))
            cm += 1
            if cm == 13:
                cy, cm = cy + 1, 1
            n += 1
        entities = self.universes.get(d["universe"]) if d.get("universe") else [None]
        for (y, m) in sorted(set(months)):
            if _left() < _g(30):
                return
            start = date(y, m, 1)
            end = date(y + (m == 12), 1 if m == 12 else m + 1, 1) - timedelta(days=1)
            for ent in entities or [None]:
                extra = {"from": start.isoformat(), "to": end.isoformat()}
                if ent is not None:
                    extra[d["entity_param"]] = ent
                if d.get("paged"):
                    v, data, nb, npages = self.fx.pages(d["endpoint"], self.params(d, extra), limit=d.get("limit"), max_pages=int(d.get("max_pages") or 40))
                else:
                    v, data, nb = self.fx.get(d["endpoint"], self.params(d, extra))
                key = self.root + d["id"] + ("/%s" % safe(ent) if ent is not None else "") + "/%04d-%02d.json.gz" % (y, m)
                if v == "ok":
                    self.bank(d, key, data, {"endpoint": d["endpoint"], "from": extra["from"], "to": extra["to"], "entity": ent or ""})
                elif v == "empty":
                    self.record(d, "empty")
                else:
                    self.record(d, v, nb)
                    if v.startswith("http4") or v == "err:budget":
                        st["cursor_month"] = "%04d-%02d" % (min((y, m), (cy, cm)))     # resume from the month that failed
                        return
        st["cursor_month"] = "%04d-%02d" % (cy, cm) if (cy, cm) <= tm else "%04d-%02d" % tm

    def _entity_job(self, d, ent, extra_params=None, period=None):
        """One entity (and optional period) -> fetch + bank.  Returns verdict."""
        pe = {d["entity_param"]: ent} if d.get("entity_param") and not str(d["entity_param"]).startswith("@") else {}
        if d.get("entity_param") == "@yq":
            y, q = ent.split("-Q")
            pe = {"year": int(y), "quarter": int(q)}
        if period:
            pe["year"], pe["quarter"] = period
        if extra_params:
            pe.update(extra_params)
        maxb = int(d.get("max_bytes") or PER_ENTITY_MAX_BYTES)
        if d.get("paged"):
            v, data, nb, npages = self.fx.pages(d["endpoint"], self.params(d, pe), limit=d.get("limit"), max_pages=int(d.get("max_pages") or 10), max_bytes=maxb)
        else:
            v, data, nb = self.fx.get(d["endpoint"], self.params(d, pe), maxb)
        key = self.root + d["id"] + "/" + safe(ent)
        if extra_params and extra_params.get("exchange"):
            key += "/" + safe(extra_params["exchange"])
        if period:
            key += "/%dQ%d" % period
        if d.get("dated_always"):
            key += "/" + self.today_s
        key += ".json.gz"
        if v == "ok":
            self.bank(d, key, data, {"endpoint": d["endpoint"], "entity": ent, "pulled_at": self.today_s, **{k: str(v2) for k, v2 in (pe.items()) if k not in (d.get("entity_param"),)}})
        elif v == "empty":
            st = self.ds(d)
            st["empty"] = st.get("empty", 0) + 1
            self.record(d, "empty")
        elif v == "too_large":
            st = self.ds(d)
            st["too_large"] = st.get("too_large", 0) + 1
            self.record(d, v, nb)
        else:
            st = self.ds(d)
            st["fail"] = st.get("fail", 0) + 1
            self.record(d, v, nb)
        return v

    def do_per_entity(self, d, share_s):
        """Advance the dataset's cursor through its universe for at most share_s seconds of wall clock."""
        st = self.ds(d)
        uni = list(self.universes.get(d["universe"]) or [])
        if not uni:
            self.record(d, "no_universe", 0, 0, {"universe": d.get("universe")})
            return
        st["universe_size"] = len(uni)
        if d.get("intraday"):
            return self.do_intraday(d, uni, share_s)
        exchanges = self.universes.get("us_exchanges") if d.get("per_exchange") else [None]
        periods = None
        if d["kind"] == "per_entity_period":
            spec_p = str(d.get("periods") or "trailing_quarters:8")
            if spec_p.startswith("trailing_quarters:"):
                periods = trailing_quarters(int(spec_p.split(":")[1]))
        cursor = int(st.get("cursor") or 0) % len(uni)
        t_end = min(time.time() + share_s, _t0 + BUDGET_S - _g(25))
        futures = []
        n_done = 0
        start_cursor = cursor
        while time.time() < t_end and n_done < len(uni):
            ent = uni[cursor]
            jobs = []
            if periods is not None:
                for per in periods:
                    key = self.root + d["id"] + "/" + safe(ent) + "/%dQ%d.json.gz" % per
                    if per in periods[:2] or not self.store.exists(key):       # newest two quarters refresh; older ones once
                        jobs.append((None, per))
            elif d["kind"] == "per_entity_period":                             # periods from a dates dataset
                dates_ds = str(d.get("periods")).split(":", 1)[1]
                dates = _j(self.root + dates_ds + "/" + safe(ent) + ".json.gz") or []
                for r in dates if isinstance(dates, list) else []:
                    y, q = r.get("fiscalYear") or r.get("year"), r.get("quarter") or r.get("period")
                    try:
                        y, q = int(y), int(str(q).replace("Q", ""))
                    except (TypeError, ValueError):
                        continue
                    key = self.root + d["id"] + "/" + safe(ent) + "/%dQ%d.json.gz" % (y, q)
                    if not self.store.exists(key):
                        jobs.append((None, (y, q)))
            else:
                for ex in exchanges:
                    jobs.append(({"exchange": ex} if ex else None, None))
            for extra, per in jobs:
                futures.append(self.pool.submit(self._entity_job, d, ent, extra, per))
            cursor = (cursor + 1) % len(uni)
            n_done += 1
            if cursor == 0:
                st["lap"] = st.get("lap", 0) + 1
            if len(futures) >= WORKERS * 3:
                for f in as_completed(futures):
                    v = f.result()
                    if v in ("http401", "http403", "http402"):
                        t_end = 0
                futures = []
        for f in as_completed(futures):
            f.result()
        st["cursor"] = cursor
        st["entities_last_run"] = n_done
        st["last_run_day"] = self.today_s
        hist = st.setdefault("entities_by_day", {})
        hist[self.today_s] = hist.get(self.today_s, 0) + n_done
        for k in sorted(hist)[:-14]:
            del hist[k]
        self.run["datasets"][d["id"]] = {"entities": n_done, "from": start_cursor, "to": cursor}

    def do_intraday(self, d, uni, share_s):
        """1-minute bars per trading day per chart symbol: catch up to yesterday for every name, then backfill older days."""
        st = self.ds(d)
        per = st.setdefault("per_symbol", {})
        back_days = int(d.get("backfill_days") or 365)
        edge = self.today - timedelta(days=1)
        t_end = min(time.time() + share_s, _t0 + BUDGET_S - _g(25))
        futures = []

        def job(sym, day):
            ds_s = day.isoformat()
            v, data, nb = self.fx.get(d["endpoint"], {"symbol": sym, "from": ds_s, "to": ds_s})
            key = self.root + d["id"] + "/%s/%s/%s.json.gz" % (safe(sym), ds_s[:7], ds_s)
            if v == "ok":
                self.bank(d, key, data, {"endpoint": d["endpoint"], "symbol": sym, "date": ds_s})
            elif v == "empty":
                self.record(d, "empty")
            else:
                self.record(d, v, nb)
            return sym, ds_s, v

        # phase 1: newest side for every symbol (yesterday back to the last banked day)
        for sym in uni:
            if time.time() > t_end:
                break
            rec = per.setdefault(sym, {"newest": None, "oldest": None, "days": 0})
            newest = date.fromisoformat(rec["newest"]) if rec.get("newest") else edge - timedelta(days=1)
            day = newest + timedelta(days=1) if rec.get("newest") else edge
            while day <= edge and time.time() < t_end:
                if day.weekday() < 5:
                    futures.append(self.pool.submit(job, sym, day))
                day += timedelta(days=1)
        pending = {}                                                # sym -> oldest day submitted and not yet answered
        stopped = set()

        def settle(fs):
            nonlocal t_end
            for f in as_completed(fs):
                s_, d_, v = f.result()
                r2 = per.setdefault(s_, {"newest": None, "oldest": None, "days": 0})
                if v == "ok":
                    r2["days"] = r2.get("days", 0) + 1
                if v in ("ok", "empty"):                              # an answered day (empty = holiday) moves both edges
                    if not r2.get("newest") or d_ > r2["newest"]:
                        r2["newest"] = d_
                    if not r2.get("oldest") or d_ < r2["oldest"]:
                        r2["oldest"] = d_
                else:
                    stopped.add(s_)                                   # retried next run; never walks past a failure
                if v in ("http401", "http403", "http402"):
                    t_end = 0
                pending.pop(s_, None)

        settle(futures)
        futures = []
        # phase 2: backfill older days round-robin
        while time.time() < t_end:
            progressed = False
            for sym in uni:
                if time.time() > t_end:
                    break
                if sym in stopped or sym in pending:
                    continue
                rec = per.setdefault(sym, {"newest": None, "oldest": None, "days": 0})
                oldest = date.fromisoformat(rec["oldest"]) if rec.get("oldest") else edge
                if (edge - oldest).days >= back_days:
                    continue
                day = oldest - timedelta(days=1)
                while day.weekday() >= 5:
                    day -= timedelta(days=1)
                futures.append(self.pool.submit(job, sym, day))
                pending[sym] = day.isoformat()
                progressed = True
                if len(futures) >= WORKERS * 4:
                    settle(futures)
                    futures = []
            if not progressed:
                if futures:
                    settle(futures)
                    futures = []
                    continue
                break
        settle(futures)
        done = sum(1 for s_ in uni if per.get(s_, {}).get("oldest") and (edge - date.fromisoformat(per[s_]["oldest"])).days >= back_days)
        st["symbols_complete"] = done
        st["universe_size"] = len(uni)
        st["last_run_day"] = self.today_s

    # -- run -----------------------------------------------------------------------------------------------------------
    def run_all(self):
        spec = self.spec
        ds_all = [d for d in spec["datasets"] if self.active(d)]
        t = time.time()
        for d in [d for d in ds_all if d["kind"] == "list"]:
            if _left() < _g(60):
                break
            self.do_list(d)
        self.run["steps"].append({"step": "lists", "seconds": round(time.time() - t, 1), "calls": self.fx.calls})
        h = self.fx.http
        if not h.get("ok") and (h.get("http401", 0) + h.get("http403", 0) + h.get("http402", 0)) >= 3:
            self.unauthorized = True                              # every list refused: the key, not one endpoint's plan
        if self.unauthorized:
            return
        # banked lists for universes not pulled this run
        for d in spec["datasets"]:
            if d["kind"] == "list" and d["endpoint"] not in self.lists:
                rows = _j(self.root + d["id"] + "/latest.json.gz")
                if isinstance(rows, list):
                    self.lists[d["endpoint"]] = rows
                    self.lists[d["id"]] = rows
        t = time.time()
        for d in [d for d in ds_all if d["kind"] == "latest_pages"]:
            if _left() < _g(60):
                break
            self.do_latest_pages(d)
        self.run["steps"].append({"step": "latest", "seconds": round(time.time() - t, 1), "calls": self.fx.calls})
        if "institutional-ownership/latest" not in self.lists:
            prev = self.ds_state.get("institutional-ownership-latest", {}).get("last_day")
            if prev:
                self.lists["institutional-ownership/latest"] = _j(self.root + "institutional-ownership-latest/%s.json.gz" % prev) or []
        if "senate-profile" not in self.lists:
            prev = self.ds_state.get("senate-profile", {}).get("last_day")
            if prev:
                self.lists["senate-profile"] = _j(self.root + "senate-profile/%s.json.gz" % prev) or []
        needs_universes = any(d["kind"] in ("per_entity", "per_entity_period") or (d["kind"] == "window" and d.get("universe")) for d in ds_all)
        self.universes = build_universes(spec, self.root, self.state, self.lists, self.fx if needs_universes else None)
        self.state["universe_counts"] = {k: len(v) for k, v in self.universes.items()}
        t = time.time()
        for d in [d for d in ds_all if d["kind"] in ("daily_dated", "window")]:
            if _left() < _g(60):
                break
            (self.do_daily_dated if d["kind"] == "daily_dated" else self.do_window)(d)
        self.run["steps"].append({"step": "dated+window", "seconds": round(time.time() - t, 1), "calls": self.fx.calls})
        # per-entity: share the remaining budget by priority (1 = 4 units, 2 = 2, 3 = 1), smallest universes finish fast
        pe = [d for d in ds_all if d["kind"] in ("per_entity", "per_entity_period")]
        pe.sort(key=lambda d: (int(d.get("priority", 2)), 1 if str(d.get("periods", "")).startswith("from_dates:") else 0, d["id"]))   # a dates dataset before its consumer
        weights = {d["id"]: {0: 4, 1: 4, 2: 2, 3: 1}.get(int(d.get("priority", 2)), 1) for d in pe}
        total_w = sum(weights.values()) or 1
        t = time.time()
        remaining = _left() - _g(30)
        for d in pe:
            if _left() < _g(40):
                break
            share = max(_g(8.0), remaining * weights[d["id"]] / total_w)
            share = min(share, _left() - _g(35))
            if share <= _g(5):
                break
            t_ds = time.time()
            self.do_per_entity(d, share)
            self.run["datasets"].setdefault(d["id"], {})["seconds"] = round(time.time() - t_ds, 1)
            if self.unauthorized:
                break
        self.run["steps"].append({"step": "per_entity", "seconds": round(time.time() - t, 1), "calls": self.fx.calls})
        self.pool.shutdown(wait=True)


def write_manifest(spec, state, cat, run):
    root = spec.get("root", "data/warm/fmp/")
    dss = state.get("datasets") or {}
    objects = sum(int(v.get("objects") or 0) for v in dss.values())
    nbytes = sum(int(v.get("bytes") or 0) for v in dss.values())
    per = {}
    for d in spec["datasets"]:
        st = dss.get(d["id"]) or {}
        c = cat.get(d["id"]) or {}
        hist = st.get("entities_by_day") or {}
        per_day = (sum(hist.values()) / max(1, len(hist))) if hist else None
        rot = round(st["universe_size"] / per_day, 1) if per_day and st.get("universe_size") else None
        per[d["id"]] = {"kind": d["kind"], "endpoint": d["endpoint"], "universe": d.get("universe"), "universe_size": st.get("universe_size"),
                        "objects": st.get("objects", 0), "bytes": st.get("bytes", 0), "lap": st.get("lap", 0), "cursor": st.get("cursor"),
                        "cursor_month": st.get("cursor_month"), "cursor_date": st.get("cursor_date"), "entities_per_day": round(per_day) if per_day else None,
                        "rotation_days": rot, "last_verdict": c.get("last_verdict"), "last_ok_at": c.get("last_ok_at"), "verdicts": c.get("verdicts"),
                        "symbols_complete": st.get("symbols_complete"), "disabled": bool(d.get("disabled"))}
    served = sorted(k for k, v in cat.items() if v.get("last_ok_at"))
    refused = sorted((k, v.get("last_verdict")) for k, v in cat.items() if not v.get("last_ok_at") and v.get("last_verdict"))
    laps_done = all((dss.get(d["id"]) or {}).get("lap", 0) >= 1 for d in spec["datasets"] if d["kind"] in ("per_entity", "per_entity_period") and not d.get("disabled") and not d.get("intraday") and (cat.get(d["id"]) or {}).get("last_ok_at"))
    intraday_done = all((dss.get(d["id"]) or {}).get("symbols_complete", 0) >= (dss.get(d["id"]) or {}).get("universe_size", 1) for d in spec["datasets"] if d.get("intraday") and (cat.get(d["id"]) or {}).get("last_ok_at"))
    windows_done = all(not (dss.get(d["id"]) or {}).get("cursor_month") or (dss.get(d["id"]) or {}).get("cursor_month") >= _today().strftime("%Y-%m") for d in spec["datasets"] if d["kind"] == "window" and not d.get("disabled"))
    phase = "STEADY" if (laps_done and intraday_done and windows_done and state.get("runs", 0) > 1) else "BACKFILL"
    state["phase"] = phase
    man = {"engine": ENGINE, "version": VERSION, "spec_version": spec.get("version"), "updated_at": _now().isoformat(),
           "objects": objects, "bytes": nbytes, "pages": objects, "pages_bytes": nbytes,
           "datasets": len([k for k, v in per.items() if v.get("objects")]), "datasets_declared": len(spec["datasets"]),
           "phase": phase, "key_status": state.get("key_status"), "universe_counts": state.get("universe_counts"),
           "endpoints_served": served, "endpoints_refused": refused, "skipped": spec.get("skipped"),
           "per_dataset": per, "run": run, "runs": state.get("runs"),
           "label": "FMP Ultimate non-bulk families (verbatim JSON, gz) - never LIVE; refreshed daily 23:27 UTC; rotation per dataset published"}
    _put_json(root + "manifest.json", man)
    _put_json(root + "catalog.json", {"engine": ENGINE, "version": VERSION, "updated_at": man["updated_at"], "datasets": cat, "skipped": spec.get("skipped")})
    return man


def lambda_handler(event, context=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    spec = _spec()
    root = spec.get("root", "data/warm/fmp/")
    key = _key()
    if not key:
        return {"error": "no FMP key (env FMP_KEY / ssm /justhodl/fmp/api-key)"}
    state_key = root + "_state/state.json"
    state = _j(state_key) or {"version": VERSION, "started_at": _now().isoformat(), "runs": 0, "key_status": "unknown", "phase": "BACKFILL", "datasets": {}}
    if event.get("reset_state"):
        state = {"version": VERSION, "started_at": _now().isoformat(), "runs": 0, "key_status": "unknown", "phase": "BACKFILL", "datasets": {}}
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
    return {"engine": ENGINE, "version": VERSION, "phase": state["phase"], "key_status": state["key_status"], "objects": man["objects"],
            "bytes": man["bytes"], "calls": run["calls"], "puts": run["puts"], "unchanged": run["unchanged"], "seconds": run["seconds"],
            "universe_counts": state.get("universe_counts")}
