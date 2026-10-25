"""polygon_bankfirst -- answer Polygon daily-aggregate requests from the warehouse before the provider, for engines that
build their own urllib requests (the Polygon lane, Khalid 2026-10-25: "100% pulled AND USED").

    from polygon_bankfirst import install
    install()                      # once, at import time; idempotent; a no-op inside the Polygon bank engines

After install(), urllib.request.urlopen(url_or_Request) for api.polygon.io
    /v2/aggs/ticker/{T}/range/1/day/{from}/{to}     (adjusted true, sort asc|desc, limit <= 50000)
    /v2/aggs/ticker/{T}/prev                        (adjusted true)
is answered from justhodl-polygon-eod's per-ticker files (data/warm/polygon-full/eod/<T>.json.gz: Polygon's own
grouped-daily rows, folded per ticker) in Polygon's own envelope, when - and only when - the bank reproduces the live
answer exactly:
  * the window lies inside the bank's history (from >= the ticker's first session; the bank starts 2021-08-26);
  * no published gap (a weekday with no banked session that is not a recorded holiday) falls inside the window;
  * a window reaching today, and /prev, need the bank's newest session to BE the last completed US session, and are
    never answered during the 13:30-21:30 UTC session (the partial bar is the provider's); a window ending in the past
    needs the bank to reach its end;
  * the live answer would not have paginated (rows <= limit);
  * adjusted=false, any multiplier/timespan other than 1/day, snapshots, grouped, trades, quotes, news, reference ->
    live, untouched.
Cost guards mirror fmp_bankfirst: 25 consecutive misses on a ticker family -> live for the rest of the 15-minute window;
90 s of bank time -> live.  Any failure inside the shim falls through to the network.  Counters: STATS.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.client import HTTPMessage

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/polygon-full/eod/"
MANIFEST = ROOT + "manifest.json"
BANK_FIRST_SESSION = "2021-08-26"
MISS_STORM = 25
BANK_TIME_CAP_S = float(os.environ.get("POLYGON_BANKFIRST_TIME_CAP_S") or 90)
CTL_WINDOW_S = 900.0
_TTL = 900
_MAX_CACHED = 64
NEVER_INSTALL_IN = {"justhodl-polygon-full", "justhodl-polygon-eod", "justhodl-polygon-bank", "justhodl-tv-bars", "justhodl-provider-catalog", "justhodl-provider-coverage"}
STATS = {"hits": 0, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
_installed = {"done": False, "real": None}
_ctl = {"bank_time": 0.0, "attempts": 0, "off": None, "miss_run": 0, "since": 0.0}
_cache: dict = {}
_S3 = None
RANGE_RE = re.compile(r"^/v2/aggs/ticker/([^/]+)/range/(\d+)/([a-z]+)/([^/]+)/([^/]+)/?$")
PREV_RE = re.compile(r"^/v2/aggs/ticker/([^/]+)/prev/?$")
FIELDS = ["t", "o", "h", "l", "c", "v", "vw", "n"]


def _s3():
    global _S3
    if _S3 is None:
        import boto3
        _S3 = boto3.client("s3", region_name="us-east-1")
    return _S3


def _get(key):
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < _TTL:
        return hit[1]
    val = None
    try:
        raw = _s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
        val = json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)
    except Exception:  # noqa: BLE001
        val = None
    if len(_cache) >= _MAX_CACHED:
        _cache.pop(next(iter(_cache)))
    _cache[key] = (now, val)
    return val


def _reset_ctl(now=None):
    _ctl.update(bank_time=0.0, attempts=0, off=None, miss_run=0, since=(now if now is not None else time.time()))


def _bypass(reason):
    if _ctl["off"] is None:
        _ctl["off"] = reason
        print("[polygon-bankfirst] bank stepped aside for the rest of this invocation: %s (attempts=%d bank_time=%.1fs)" % (reason, _ctl["attempts"], _ctl["bank_time"]))


def _last_completed_session(now=None):
    now = now or datetime.now(timezone.utc)
    d = now.date()
    if now.hour * 60 + now.minute < 21 * 60 + 30:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _in_session_window(now=None):
    now = now or datetime.now(timezone.utc)
    m = now.hour * 60 + now.minute
    return now.weekday() < 5 and 13 * 60 + 30 <= m < 21 * 60 + 30


def _date_arg(v):
    """Polygon accepts YYYY-MM-DD or a millisecond timestamp."""
    s = str(v)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.strptime(s, "%Y-%m-%d").date()
    if s.isdigit():
        n = int(s)
        return datetime.fromtimestamp(n / (1000.0 if n > 10 ** 11 else 1.0), tz=timezone.utc).date()
    raise ValueError(s)


def _row_date(t_ms):
    return datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc).date()


def _envelope(ticker, rows, adjusted=True):
    out = []
    for r in rows:
        d = {k: v for k, v in zip(FIELDS, r) if v is not None}
        out.append({k: d[k] for k in ("v", "vw", "o", "c", "h", "l", "t", "n") if k in d})
    body = {"ticker": ticker, "queryCount": len(out), "resultsCount": len(out), "adjusted": adjusted, "results": out, "status": "OK",
            "request_id": "bank-" + hashlib.sha256(("%s%d%s" % (ticker, len(out), out[0]["t"] if out else "")).encode()).hexdigest()[:16], "count": len(out)}
    return json.dumps(body, separators=(",", ":"), default=str).encode("utf-8")


def answer(url, now=None):
    """Bank answer (bytes) for a Polygon daily-aggregate URL, or None."""
    if time.time() - _ctl["since"] > CTL_WINDOW_S:
        _reset_ctl()
    if _ctl["off"]:
        STATS["bypassed"] += 1
        return None
    u = urllib.parse.urlparse(url)
    if u.netloc not in ("api.polygon.io", "api.massive.com"):
        return None
    q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
    q.pop("apiKey", None)
    q.pop("apikey", None)
    m = RANGE_RE.match(u.path)
    prev = PREV_RE.match(u.path) if not m else None
    if not m and not prev:
        return None
    if str(q.get("adjusted", "true")).lower() not in ("true", "1"):
        STATS["unshaped"] += 1
        return None
    if set(q) - {"adjusted", "sort", "limit"}:
        STATS["unshaped"] += 1
        return None
    now = now or datetime.now(timezone.utc)
    if m:
        ticker, mult, span, a, b = m.groups()
        if mult != "1" or span != "day":
            STATS["unshaped"] += 1
            return None
        try:
            d_from, d_to = _date_arg(a), _date_arg(b)
            limit = int(q.get("limit") or 5000)
        except ValueError:
            STATS["unshaped"] += 1
            return None
        if limit < 1 or limit > 50000 or d_from > d_to:
            STATS["unshaped"] += 1
            return None
        sort = str(q.get("sort", "asc")).lower()
        if sort not in ("asc", "desc"):
            STATS["unshaped"] += 1
            return None
    else:
        ticker = prev.group(1)
        d_from = d_to = None
        limit, sort = 1, "asc"
    ticker = urllib.parse.unquote(ticker)
    t0 = time.time()
    _ctl["attempts"] += 1
    try:
        doc = _get(ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in ticker)[:80] + ".json.gz")
        man = _get(MANIFEST) or {}
    finally:
        _ctl["bank_time"] += time.time() - t0
        if _ctl["bank_time"] > BANK_TIME_CAP_S:
            _bypass("bank time cap %.0fs" % BANK_TIME_CAP_S)
    if not isinstance(doc, dict) or not doc.get("rows"):
        STATS["misses"] += 1
        _ctl["miss_run"] += 1
        if _ctl["miss_run"] >= MISS_STORM:
            _bypass("%d consecutive misses" % _ctl["miss_run"])
        return None
    _ctl["miss_run"] = 0
    rows = [r for r in doc["rows"] if isinstance(r, list) and len(r) >= 5 and r[0] is not None]
    if not rows:
        STATS["misses"] += 1
        return None
    first, last = _row_date(rows[0][0]), _row_date(rows[-1][0])
    today = now.date()
    if prev or d_to >= today:
        if _in_session_window(now):
            STATS["stale"] += 1
            return None
        if last != _last_completed_session(now):
            STATS["stale"] += 1
            return None
    elif last < d_to:
        STATS["stale"] += 1
        return None
    if prev:
        STATS["hits"] += 1
        return _envelope(ticker, rows[-1:])
    if d_from < first and d_from >= datetime.strptime(BANK_FIRST_SESSION, "%Y-%m-%d").date():
        pass                                   # the ticker did not trade before `first`: the live answer starts there too
    elif d_from < first:
        STATS["unshaped"] += 1                 # the window reaches before the bank's history: the provider has more
        return None
    gaps = man.get("gaps") or []
    lo, hi = d_from.isoformat(), min(d_to, last).isoformat()
    if any(lo <= g <= hi for g in gaps):
        STATS["gap"] += 1
        return None
    sel = [r for r in rows if d_from <= _row_date(r[0]) <= d_to]
    if len(sel) > limit:
        STATS["unshaped"] += 1                 # the live answer would paginate (next_url); not reproduced
        return None
    if sort == "desc":
        sel = list(reversed(sel))
    STATS["hits"] += 1
    return _envelope(ticker, sel)


def _headers(n):
    h = HTTPMessage()
    h["Content-Type"] = "application/json; charset=utf-8"
    h["Content-Length"] = str(n)
    h["X-JH-Source"] = "polygon-eod-bank"
    return h


class _Resp(io.BytesIO):
    status = 200
    code = 200
    reason = "OK"
    version = 11
    chunked = False
    debuglevel = 0

    def __init__(self, body, url):
        super().__init__(body)
        self.url = url
        self.length = len(body)
        self.headers = _headers(len(body))
        self.msg = self.headers
        self.fp = self

    def getcode(self):
        return 200

    def getheader(self, name, default=None):
        return self.headers.get(name, default)

    def getheaders(self):
        return list(self.headers.items())

    def geturl(self):
        return self.url

    def info(self):
        return self.headers

    def isclosed(self):
        return self.closed

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


def _urlopen(url, data=None, timeout=None, *args, **kwargs):
    real = _installed["real"]
    try:
        target = url.full_url if isinstance(url, urllib.request.Request) else str(url)
        if data is None and "/v2/aggs/ticker/" in target and ("api.polygon.io" in target or "api.massive.com" in target):
            body = answer(target)
            if body is not None:
                return _Resp(body, target)
        else:
            STATS["passthrough"] += 1
    except Exception:  # noqa: BLE001
        STATS["errors"] += 1
    if timeout is None:
        return real(url, data, *args, **kwargs)
    return real(url, data, timeout, *args, **kwargs)


def install():
    if _installed["done"]:
        return
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "") in NEVER_INSTALL_IN or os.environ.get("POLYGON_BANKFIRST_DISABLE") == "1":
        return
    _installed["real"] = urllib.request.urlopen
    urllib.request.urlopen = _urlopen
    _installed["done"] = True


def uninstall():
    if _installed["done"] and _installed["real"]:
        urllib.request.urlopen = _installed["real"]
    _installed["done"] = False


def print_stats(prefix="[polygon-bankfirst]"):
    print("%s hits=%d stale=%d gap=%d misses=%d unshaped=%d passthrough=%d bypassed=%d errors=%d bank_time=%.1fs%s" % (
        prefix, STATS["hits"], STATS["stale"], STATS["gap"], STATS["misses"], STATS["unshaped"], STATS["passthrough"], STATS["bypassed"], STATS["errors"],
        _ctl["bank_time"], (" off=%s" % _ctl["off"]) if _ctl["off"] else ""))
