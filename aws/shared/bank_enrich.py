"""bank_enrich -- one call to the whole-market factor book (justhodl-bank-factors) from any engine.

The factor book (data/warm/factor-book/, written daily 02:50 UTC) joins the provider banks once: for every US-listed
symbol, FMP's scores (Altman Z, Piotroski), rating, DCF, analyst price targets and consensus, TTM key metrics and
ratios, latest fiscal-year growth, earnings surprises, float, Polygon short interest and short volume, ETF holders,
peers, returns from the Polygon tape, cross-sectional percentile ranks (universe and sector) and transparent
composites (value / quality / growth / momentum / squeeze / street).  Engines read it instead of parsing the banks:

    from bank_enrich import factor, factors, matrix, screen, attach
    f = factor("AAPL")                       # one symbol's document (dict) or None; cached per container
    fs = factors(["AAPL", "MSFT"])           # {symbol: doc}; threaded reads; missing symbols absent
    m = matrix()                             # {"symbols": [...], "cols": {field: [...]}, "as_of": {...}} -- whole market
    hits = screen(lambda r: r["altman_z"] is not None and r["altman_z"] < 1.8 and (r["mcap"] or 0) > 2e8)
    out = attach(out, symbols=[...])         # adds out["bank_enrichment"] = {as_of, source, fields, rows{sym:{...}}, missing}

Rules (the warehouse doctrine): never raises (S3 trouble -> None / empty, the engine keeps working on what it has);
never fabricates (a symbol the book lacks is in `missing`, not filled); every answer carries `as_of` so the consumer
can label it; values are the banks' own numbers -- the `derived` block is arithmetic on them and named as such.
Bundled into every Lambda zip via aws/shared.  No key, no provider, no network except S3.
"""
from __future__ import annotations

import gzip
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/factor-book/"
SOURCE = "justhodl-bank-factors"
_CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=3, read_timeout=30, max_pool_connections=32)
_S3 = None
_tls = threading.local()
_LOCK = threading.Lock()
_DOCS: dict = {}            # symbol -> (fetched_at, doc|None)
_DOC_TTL = 1800
_DOC_MAX = int(os.environ.get("BANK_ENRICH_DOC_CACHE", "3000"))
_MATRIX: dict = {"at": 0.0, "doc": None}
_MANIFEST: dict = {"at": 0.0, "doc": None}
_MISS = object()

# what attach() puts on an engine's output by default: the fields a desk reads first (all numeric, all from the matrix
# names, so a consumer can look any of them up in manifest.schema_found)
DEFAULT_FIELDS = ("mcap", "sector", "industry", "altman_z", "piotroski", "rating_score", "dcf_upside_pct", "pt_upside_pct", "pt_n_1m", "consensus_score",
                  "pe", "ev_ebitda", "fcf_yield", "roic", "gross_margin", "net_debt_ebitda", "revenue_growth", "eps_growth", "beat_rate_4q",
                  "float_shares", "short_pct_float", "days_to_cover", "short_volume_ratio", "etf_holder_count", "ret_21d", "ret_126d", "from_52w_high_pct",
                  "dollar_vol_20d", "composite_value", "composite_quality", "composite_growth", "composite_momentum", "composite_squeeze", "composite_street")


def _client():
    c = getattr(_tls, "c", None)
    if c is None:
        c = _tls.c = boto3.client("s3", region_name="us-east-1", config=_CFG)
    return c


def _get(key):
    try:
        raw = _client().get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception:  # noqa: BLE001
        return None
    try:
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8", "replace"))
    except Exception:  # noqa: BLE001
        return None


def _safe_sym(sym):
    import re
    return re.sub(r"[^A-Za-z0-9._^=-]", "_", str(sym).upper().strip())[:60]


def manifest(max_age_s=900):
    """The factor book's manifest (as_of per source, coverage, schema_found); {} when the book is not there."""
    with _LOCK:
        if _MANIFEST["doc"] is not None and time.time() - _MANIFEST["at"] < max_age_s:
            return _MANIFEST["doc"]
    doc = _get(ROOT + "manifest.json") or {}
    with _LOCK:
        _MANIFEST.update(at=time.time(), doc=doc)
    return doc


def as_of():
    """{'fmp-bank': ..., 'fmp-walker': ..., 'polygon-bank': ..., 'polygon-tape': ...} or {}."""
    return (manifest() or {}).get("as_of") or {}


def available():
    return bool((manifest() or {}).get("symbols"))


def factor(symbol):
    """One symbol's factor-book document, or None when the book lacks it.  Dash/dot spellings both tried."""
    if not symbol:
        return None
    sym = str(symbol).upper().strip()
    now = time.time()
    with _LOCK:
        hit = _DOCS.get(sym)
        if hit and now - hit[0] < _DOC_TTL:
            return None if hit[1] is _MISS else hit[1]
    doc = _get(ROOT + "symbols/%s.json.gz" % _safe_sym(sym))
    if doc is None and "." in sym:
        doc = _get(ROOT + "symbols/%s.json.gz" % _safe_sym(sym.replace(".", "-")))
    if doc is None and "-" in sym:
        doc = _get(ROOT + "symbols/%s.json.gz" % _safe_sym(sym.replace("-", ".")))
    with _LOCK:
        if len(_DOCS) >= _DOC_MAX:
            for k in sorted(_DOCS, key=lambda k: _DOCS[k][0])[: _DOC_MAX // 4]:
                _DOCS.pop(k, None)
        _DOCS[sym] = (now, doc if doc is not None else _MISS)
    return doc


def factors(symbols, workers=12):
    """{symbol: doc} for the symbols the book holds (threaded); never raises."""
    syms = []
    seen = set()
    for s in symbols or []:
        s = str(s or "").upper().strip()
        if s and s not in seen:
            seen.add(s)
            syms.append(s)
    out = {}
    if not syms:
        return out
    if len(syms) == 1:
        d = factor(syms[0])
        return {syms[0]: d} if d else {}
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(syms)))) as pool:
        for sym, doc in zip(syms, pool.map(factor, syms)):
            if doc:
                out[sym] = doc
    return out


def matrix(max_age_s=1800):
    """The whole-market columnar matrix ({symbols, cols, fields, as_of, n}); None when not there.  One read per
    container (then cached); ~25k symbols x ~150 columns -- use screen()/rows() rather than copying it."""
    with _LOCK:
        if _MATRIX["doc"] is not None and time.time() - _MATRIX["at"] < max_age_s:
            return _MATRIX["doc"]
    doc = _get(ROOT + "matrix/core.json.gz")
    if isinstance(doc, dict) and doc.get("cols") and doc.get("symbols"):
        with _LOCK:
            _MATRIX.update(at=time.time(), doc=doc)
        return doc
    return None


def rows(fields=None, where=None, limit=None):
    """Iterate the matrix as dict rows (identity + the requested fields, default all); `where(row) -> bool` filters."""
    m = matrix()
    if not m:
        return []
    cols = m["cols"]
    names = list(cols) if fields is None else [f for f in fields if f in cols]
    for ident in ("symbol", "sector", "industry", "exchange", "is_etf", "is_otc"):
        if ident in cols and ident not in names:
            names.insert(0, ident)
    out = []
    for i in range(m["n"]):
        r = {k: cols[k][i] for k in names}
        if where is None or where(r):
            out.append(r)
            if limit and len(out) >= limit:
                break
    return out


def screen(where, fields=None, limit=None, common_only=True):
    """Whole-market screen over the matrix: rows where `where(row)` is true (US common stocks by default)."""
    def pred(r):
        if common_only and (r.get("is_etf") or r.get("is_otc")):
            return False
        try:
            return bool(where(r))
        except Exception:  # noqa: BLE001
            return False
    return rows(fields, pred, limit)


def lookup(symbol, fields=None):
    """Matrix-row view of one symbol (fast when many symbols are needed and the matrix is already loaded)."""
    m = matrix()
    if not m:
        return None
    sym = str(symbol or "").upper().strip()
    idx = m.get("_index")
    if idx is None:
        idx = {s: i for i, s in enumerate(m["symbols"])}
        m["_index"] = idx
    i = idx.get(sym, idx.get(sym.replace(".", "-"), idx.get(sym.replace("-", "."))))
    if i is None:
        return None
    cols = m["cols"]
    names = list(cols) if fields is None else [f for f in fields if f in cols]
    return {k: cols[k][i] for k in names}


def prescreen(where, order_by=None, descending=True, limit=1500, fields=None, common_only=True):
    """Whole-market candidate list for an engine whose universe used to be capped by per-symbol API cost: the symbols
    whose matrix row satisfies `where`, ordered by `order_by` (a field name or a callable row -> number), capped.
    Returns [] when the book is not there, so a caller keeps its old universe as the fallback."""
    hits = screen(where, fields=fields, common_only=common_only)
    if order_by is not None:
        keyf = order_by if callable(order_by) else (lambda r, f=order_by: r.get(f))
        hits = [r for r in hits if keyf(r) is not None]
        hits.sort(key=keyf, reverse=descending)
    return [r["symbol"] for r in hits[:limit]]


def quote(symbol):
    """A quote-shaped read of the factor book for an engine whose live FMP /quote call failed or is not needed at EOD:
    price is the bank's last price (FMP profile-bulk as of the bank's pull, after the close), 52-week high/low from
    the Polygon tape.  Labelled `as_of` + `source`; it is the close, never a live tick.  None when the book lacks it."""
    d = factor(symbol)
    if not d:
        return None
    prof, ident, ret = d.get("profile") or {}, d.get("identity") or {}, d.get("returns") or {}
    lk = {str(k).lower(): k for k in prof}

    def p(*names):
        for n in names:
            k = lk.get(n.lower())
            if k is not None and prof.get(k) not in (None, ""):
                return prof[k]
        return None
    price = p("price") if p("price") is not None else ret.get("close")
    return {"symbol": ident.get("symbol") or str(symbol).upper(), "name": ident.get("name") or p("companyname"), "price": price,
            "marketCap": ident.get("mcap") if ident.get("mcap") is not None else p("marketcap", "mktcap"),
            "yearHigh": ret.get("high_52w"), "yearLow": ret.get("low_52w"), "previousClose": ret.get("close"), "avgVolume": p("averagevolume", "volavg"),
            "exchange": ident.get("exchange"), "sector": ident.get("sector"), "industry": ident.get("industry"), "beta": p("beta"),
            "sharesOutstanding": (d.get("float") or {}).get("outstandingShares"), "as_of": (d.get("as_of") or {}), "source": "factor-book (bank close, not a live tick)"}


def _discover(doc, cap=2000):
    """Symbols mentioned in an engine's output: values under keys named symbol/ticker (bounded walk)."""
    found, seen = [], set()
    stack = [doc]
    steps = 0
    while stack and steps < 200000 and len(found) < cap:
        x = stack.pop()
        steps += 1
        if isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, str) and str(k).lower() in ("symbol", "ticker", "sym", "tkr") and 1 <= len(v) <= 12 and v.upper() == v.replace(" ", "").upper():
                    s = v.upper().strip()
                    if s not in seen:
                        seen.add(s)
                        found.append(s)
                elif isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(x, list):
            stack.extend(x)
    return found


def attach(doc, symbols=None, fields=DEFAULT_FIELDS, key="bank_enrichment", max_symbols=400):
    """Add a provenance-carrying enrichment block to an engine's output dict (additive; the engine's own keys are
    untouched).  symbols=None discovers them from the document.  Returns the same dict; never raises."""
    if not isinstance(doc, dict):
        return doc
    try:
        syms = [str(s).upper().strip() for s in (symbols if symbols is not None else _discover(doc)) if s]
        syms = list(dict.fromkeys(syms))[:max_symbols]
        block = {"source": SOURCE, "as_of": as_of(), "fields": list(fields), "rows": {}, "missing": []}
        if not syms:
            block["note"] = "no symbols to enrich"
            doc[key] = block
            return doc
        m = matrix() if len(syms) > 25 else None
        if m:
            for s in syms:
                r = lookup(s, fields)
                if r is None:
                    block["missing"].append(s)
                else:
                    block["rows"][s] = r
        else:
            got = factors(syms)
            for s in syms:
                d = got.get(s)
                if not d:
                    block["missing"].append(s)
                    continue
                flat = _flatten(d)
                block["rows"][s] = {f: flat.get(f) for f in fields}
        block["n"] = len(block["rows"])
        doc[key] = block
    except Exception as e:  # noqa: BLE001
        doc[key] = {"source": SOURCE, "error": type(e).__name__, "rows": {}, "missing": []}
    return doc


def _flatten(d):
    """A per-symbol document flattened to the matrix's field names (identity, curated fields, ranks, composites)."""
    out = {}
    ident = d.get("identity") or {}
    out.update({k: ident.get(k) for k in ("symbol", "name", "exchange", "sector", "industry", "country", "is_etf", "is_otc", "is_adr", "ipo_date", "mcap")})
    for k, v in (d.get("derived") or {}).items():
        out[k] = v
    for k, v in (d.get("returns") or {}).items():
        out[k] = v
    for k, v in (d.get("ranks") or {}).items():
        out[k] = v
    for k, v in (d.get("composites") or {}).items():
        out["composite_" + k] = v
    # curated names from the source blocks (same candidates as the factor book's FIELDS)
    def pick(block, *names):
        b = d.get(block) or {}
        lk = {str(k).lower().replace("_", ""): k for k in b}
        for n in names:
            k = lk.get(n.lower().replace("_", ""))
            if k is not None and b.get(k) not in (None, ""):
                return b[k]
        return None
    out.setdefault("price", pick("profile", "price"))
    out["beta"] = pick("profile", "beta")
    out["altman_z"], out["piotroski"] = pick("scores", "altmanZScore"), pick("scores", "piotroskiScore")
    out["rating_score"] = pick("rating", "overallScore", "ratingScore")
    out["dcf"] = pick("dcf", "dcf")
    out["pt_avg_1m"], out["pt_n_1m"] = pick("price_target", "lastMonthAvgPriceTarget"), pick("price_target", "lastMonthCount")
    out["pe"], out["pb"], out["ps"] = pick("ratios_ttm", "priceToEarningsRatioTTM", "peRatioTTM"), pick("ratios_ttm", "priceToBookRatioTTM"), pick("ratios_ttm", "priceToSalesRatioTTM")
    out["p_fcf"] = pick("ratios_ttm", "priceToFreeCashFlowRatioTTM", "priceToFreeCashFlowsRatioTTM")
    out["gross_margin"], out["net_margin"] = pick("ratios_ttm", "grossProfitMarginTTM"), pick("ratios_ttm", "netProfitMarginTTM")
    out["dividend_yield"], out["payout_ratio"] = pick("ratios_ttm", "dividendYieldTTM"), pick("ratios_ttm", "dividendPayoutRatioTTM", "payoutRatioTTM")
    out["ev_ebitda"] = pick("key_metrics_ttm", "evToEBITDATTM", "enterpriseValueOverEBITDATTM")
    out["fcf_yield"], out["earnings_yield"] = pick("key_metrics_ttm", "freeCashFlowYieldTTM"), pick("key_metrics_ttm", "earningsYieldTTM")
    out["roic"], out["roe"] = pick("key_metrics_ttm", "returnOnInvestedCapitalTTM", "roicTTM"), pick("key_metrics_ttm", "returnOnEquityTTM", "roeTTM")
    out["net_debt_ebitda"] = pick("key_metrics_ttm", "netDebtToEBITDATTM")
    out["revenue_growth"], out["eps_growth"] = pick("growth", "growthRevenue"), pick("growth", "growthEPS", "growthEPSDiluted")
    out["float_shares"], out["free_float_pct"] = pick("float", "floatShares"), pick("float", "freeFloat")
    sh = d.get("short") or {}
    out["short_interest"], out["days_to_cover"] = sh.get("short_interest"), sh.get("days_to_cover")
    return out
