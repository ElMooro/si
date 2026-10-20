"""aws/shared/blackswan_resolver.py - the resolver ladder shared by the seven blackswan-family engines.

Khalid, 2026-10-20 ("merge the blackswan seven"): blackswan-watch, top-signals, bottom-signals, dxy-predict,
macro-predict, liq-indicators and liquidity-reversal each carried a byte-identical copy of these 23 functions and
constants (forked from blackswan-watch v1.8.1).  One copy lives here now; the engines import it.  Behaviour is
unchanged: the functions are the root engine's verbatim, the warm cache (data/warm/blackswan/, 20 h TTL) is the
same, and each engine still sets its own FRED_KEY (`blackswan_resolver.FRED_KEY = ...`) and its own budgets.
"""
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

# The engine sets this after import (six resolve it through managed_secret; liquidity-reversal keeps env-only).
FRED_KEY = os.environ.get("FRED_API_KEY", "")

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
WARM = "data/warm/blackswan/"
TE_KEY = os.environ.get("TE_API_KEY", "")
TE_DIRECT = {
    "ECONOMICS:DEIFOCC": ("germany", "Ifo Current Conditions"),
    "ECONOMICS:DEZCC": ("germany", "ZEW Current Conditions"),
    "ECONOMICS:DEZEWS": ("germany", "ZEW Economic Sentiment Index"),
    "ECONOMICS:DENO": ("germany", "Factory Orders"),
    "ECONOMICS:JPMTO": ("japan", "Machine Tool Orders"),
    "ECONOMICS:JPSBSI": ("japan", "Small Business Sentiment"),
    "ECONOMICS:CNNO": ("china", "New Orders"),
}
MULTPL_SLUGS = {
    "SHILLER_PE_RATIO": "shiller-pe",
    "SP500_PE_RATIO": "s-p-500-pe-ratio",
    "SP500_EARNINGS": "s-p-500-earnings",
    "SP500_EARNINGS_YIELD": "s-p-500-earnings-yield",
    "SP500_DIV_YIELD": "s-p-500-dividend-yield",
    "SP500_SALES": "s-p-500-sales",
    "SP500_BOOK_VALUE": "s-p-500-book-value",
}
s3 = boto3.client("s3")
_FLEET_CACHE = {}
TVC_MAP = {"US01MY": "DGS1MO", "US03MY": "DGS3MO",
           "US06MY": "DGS6MO", "US01Y": "DGS1", "US02Y": "DGS2",
           "US03Y": "DGS3", "US05Y": "DGS5", "US07Y": "DGS7",
           "US10Y": "DGS10", "US20Y": "DGS20", "US30Y": "DGS30"}
_VIDX = {}

def cadence_label(se):
    ds = []
    for i in range(max(1, len(se) - 12), len(se)):
        try:
            ds.append((datetime.fromisoformat(se[i]["date"])
                       - datetime.fromisoformat(
                           se[i - 1]["date"])).days)
        except Exception:
            continue
    if not ds:
        return "chg"
    ds.sort()
    med = ds[len(ds) // 2]
    if med <= 3:
        return "DoD"
    if med <= 10:
        return "WoW"
    if med <= 45:
        return "MoM"
    return "QoQ"

def cboe_fallback(idx, budget):
    """CBOE:* — the vix-curve engine's own CDN CSV route."""
    if budget["n"] <= 0:
        return None
    wkey = WARM + "cb_" + re.sub(r"[^A-Z0-9]", "_", idx) + ".json"
    cached = s3_json(wkey)
    if cached and cached.get("series"):
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 72000:
                return cached
        except Exception:
            pass
    budget["n"] -= 1
    try:
        req = urllib.request.Request(
            "https://cdn.cboe.com/api/global/us_indices/"
            "daily_prices/%s_History.csv" % idx,
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as h:
            txt = h.read().decode("utf-8", "replace")
        se = []
        for line in txt.splitlines()[1:]:
            parts = line.split(",")
            if len(parts) < 5:
                continue
            try:
                dt = datetime.strptime(parts[0].strip(),
                                       "%m/%d/%Y").date()
            except Exception:
                try:
                    dt = datetime.fromisoformat(
                        parts[0].strip()[:10]).date()
                except Exception:
                    continue
            try:
                se.append({"date": dt.isoformat(),
                           "value": float(parts[4])})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 15:
            env = {"series": se[-400:],
                   "fetched_at": datetime.now(
                       timezone.utc).isoformat(timespec="seconds"),
                   "via": "CBOE CDN " + idx}
            s3.put_object(Bucket=BUCKET, Key=wkey,
                          Body=json.dumps(env).encode(),
                          ContentType="application/json")
            return env
    except Exception:
        pass
    return None

def columnar_zip(doc, dates_key, field):
    """4627 shape lesson: fleet stores are columnar — parallel
    'dates' + per-name value arrays."""
    dates = doc.get(dates_key)
    vals = doc.get(field)
    if not (isinstance(dates, list) and isinstance(vals, list)):
        return None
    n = min(len(dates), len(vals))
    se = []
    for i in range(max(0, n - 400), n):
        try:
            se.append({"date": str(dates[i])[:10],
                       "value": float(vals[i])})
        except Exception:
            continue
    return se if len(se) >= 15 else None

def dict_aliases(dic, sym):
    """Parse '(FRED: SID)' and '(MARKET: Y)' out of the
    symbol-dictionary name — the estate's own rosetta stone."""
    nm = dict_name(dic, sym) or ""
    fm = re.search(r"\(FRED:\s*([A-Z0-9]+)\)", nm)
    mm = re.search(r"\(MARKET:\s*([^)]+)\)", nm)
    return (fm.group(1) if fm else None,
            mm.group(1).strip() if mm else None)

def dict_name(dic, sym):
    if isinstance(dic, dict):
        for k in ("symbols", "dictionary", "data"):
            m = dic.get(k)
            if isinstance(m, dict) and sym in m:
                e = m[sym]
                if isinstance(e, dict):
                    return (e.get("name") or e.get("title")
                            or e.get("full_name") or "")[:70]
                if isinstance(e, str):
                    return e[:70]
        if sym in dic and isinstance(dic[sym], (str, dict)):
            e = dic[sym]
            return (e if isinstance(e, str)
                    else (e.get("name") or e.get("title") or ""))[:70]
    return ""

def extract_latest(node, depth=0):
    if depth > 4 or node is None:
        return None, None
    if isinstance(node, dict):
        lv = None
        for k in ("last", "value", "latest", "close", "price", "v"):
            if isinstance(node.get(k), (int, float)):
                lv = float(node[k])
                break
        ld = None
        for k in ("date", "last_date", "as_of", "updated", "t"):
            if node.get(k):
                ld = str(node[k])[:10]
                break
        if lv is not None:
            return lv, ld
        for v in node.values():
            r = extract_latest(v, depth + 1)
            if r[0] is not None:
                return r
    return None, None

def extract_series(node, depth=0):
    """Find a [{date-ish, value-ish}] series anywhere under node."""
    if depth > 4 or node is None:
        return None
    if isinstance(node, list) and node and isinstance(
            node[0], (list, tuple)) and len(node[0]) == 2:
        out = []
        for pr in node[-400:]:
            try:
                d0 = pr[0]
                if isinstance(d0, (int, float)):
                    d0 = datetime.fromtimestamp(
                        d0 / (1000 if d0 > 1e11 else 1),
                        tz=timezone.utc).date().isoformat()
                out.append({"date": str(d0)[:10],
                            "value": float(pr[1])})
            except Exception:
                continue
        return out or None
    if isinstance(node, list) and node and isinstance(node[0], dict):
        dk = next((k for k in node[0]
                   if k in ("date", "d", "t", "time", "period")), None)
        vk = next((k for k in node[0]
                   if k in ("value", "v", "close", "c", "y")), None)
        if dk and vk:
            out = []
            for o in node[-400:]:
                try:
                    out.append({"date": str(o[dk])[:10],
                                "value": float(o[vk])})
                except Exception:
                    continue
            return out or None
    if isinstance(node, dict):
        for k in ("series", "history", "points", "values", "obs",
                  "data"):
            r = extract_series(node.get(k), depth + 1)
            if r:
                return r
        for v in node.values():
            r = extract_series(v, depth + 1)
            if r:
                return r
    return None

def find_row_table(node, depth=0):
    """First list of dicts carrying a date-ish key, anywhere."""
    if depth > 3 or node is None:
        return None
    if isinstance(node, list) and node and isinstance(node[0], dict):
        if any(k in node[0] for k in ("date", "d", "t", "time",
                                      "period")):
            return node
    if isinstance(node, dict):
        for v in node.values():
            r = find_row_table(v, depth + 1)
            if r:
                return r
    return None

def fleet_column_series(key, field):
    doc = fleet_doc(key)
    if doc is None:
        return None
    if field is None:
        se = extract_series(doc)
        return {"series": se} if se else None
    se = columnar_zip(doc, "dates", field)
    if se:
        return {"series": se}
    for ck in ("series", "indices", "columns", "data"):
        sub = doc.get(ck)
        if isinstance(sub, dict) and field in sub:
            merged = {"dates": doc.get("dates"), field: sub[field]}
            se = columnar_zip(merged, "dates", field)
            if se:
                return {"series": se}
    rows = find_row_table(doc)
    if not rows:
        return None
    dk = next((k for k in rows[0]
               if k in ("date", "d", "t", "time", "period")), None)
    if not dk:
        return None
    se = []
    for o in rows[-400:]:
        v = o.get(field)
        if v is None:
            continue
        try:
            d0 = o[dk]
            if isinstance(d0, (int, float)):
                d0 = datetime.fromtimestamp(
                    d0 / (1000 if d0 > 1e11 else 1),
                    tz=timezone.utc).date().isoformat()
            se.append({"date": str(d0)[:10], "value": float(v)})
        except Exception:
            continue
    return {"series": se} if len(se) >= 15 else None

def fleet_doc(key):
    if key not in _FLEET_CACHE:
        _FLEET_CACHE[key] = s3_json(key)
    return _FLEET_CACHE[key]

def fred_fallback(sym, budget):
    """v1.1.0: FRED: members absent from the vault get real series
    (z + range basis) via a bounded, cached direct fetch."""
    if not sym.startswith("FRED:") or budget["n"] <= 0:
        return None
    sid = sym.split(":", 1)[1]
    wkey = WARM + sid + ".json"
    cached = s3_json(wkey)
    if cached and cached.get("series"):
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 72000:
                return cached
        except Exception:
            pass
    budget["n"] -= 1
    try:
        qs = urllib.parse.urlencode({
            "series_id": sid, "api_key": FRED_KEY,
            "file_type": "json", "sort_order": "desc",
            "limit": 300})
        req = urllib.request.Request(
            "https://api.stlouisfed.org/fred/series/observations?"
            + qs, headers={"User-Agent": "justhodl-blackswan"})
        with urllib.request.urlopen(req, timeout=12) as h:
            d = json.loads(h.read())
        se = [{"date": o["date"], "value": float(o["value"])}
              for o in d.get("observations", [])
              if o.get("value") not in (None, ".", "")]
        se.reverse()
        if not se:
            return None
        env = {"series": se,
               "fetched_at": datetime.now(
                   timezone.utc).isoformat(timespec="seconds")}
        s3.put_object(Bucket=BUCKET, Key=wkey,
                      Body=json.dumps(env).encode(),
                      ContentType="application/json")
        return env
    except Exception:
        return None

def leg_to_fred(leg):
    if leg.startswith("FRED:"):
        return leg.split(":", 1)[1]
    if leg.startswith("TVC:") and leg.split(":", 1)[1] in TVC_MAP:
        return TVC_MAP[leg.split(":", 1)[1]]
    return None

def ma200_series(sym):
    """Plain exchange-prefixed tickers from the ma200 closes buffer
    (daily closes for ~700 names)."""
    if ":" not in sym or any(c in sym for c in "!/-+"):
        return None
    tk = sym.split(":", 1)[1]
    doc = fleet_doc("data/_ma200/closes.json")
    if doc is None:
        return None
    ser = doc.get("series") if isinstance(doc, dict) else None
    if isinstance(ser, dict) and tk in ser:
        se = columnar_zip(doc, "dates", None) if False else None
        vals = ser[tk]
        dates = doc.get("dates")
        if isinstance(dates, list) and isinstance(vals, list):
            n = min(len(dates), len(vals))
            se = []
            for i in range(max(0, n - 400), n):
                try:
                    se.append({"date": str(dates[i])[:10],
                               "value": float(vals[i])})
                except Exception:
                    continue
            if len(se) >= 15:
                return {"series": se}
    return None

def multpl_fallback(dataset, budget):
    """MULTPL:* — monthly tables scraped from multpl.com (the
    canonical free mirror of the Shiller series)."""
    if budget["n"] <= 0:
        return None
    base = dataset.upper()
    for suf in ("_MONTH", "_YEAR", "_QUARTER"):
        if base.endswith(suf):
            base = base[:-len(suf)]
    slug = MULTPL_SLUGS.get(base)
    if not slug:
        slug = base.lower().replace("sp500", "s-p-500")\
                   .replace("_", "-")
    wkey = WARM + "mp_" + re.sub(r"[^a-z0-9-]", "_", slug) + ".json"
    cached = s3_json(wkey)
    if cached and cached.get("series"):
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 72000:
                return cached
        except Exception:
            pass
    budget["n"] -= 1
    try:
        req = urllib.request.Request(
            "https://www.multpl.com/%s/table/by-month" % slug,
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as h:
            html = h.read().decode("utf-8", "replace")
        rows = re.findall(
            r"([A-Z][a-z]{2} \d{1,2}, \d{4})</td>\s*<td[^>]*>"
            r"[^0-9-]*([\d,]+\.?\d*)", html)
        se = []
        for d0, v in rows[:420]:
            try:
                dt = datetime.strptime(d0, "%b %d, %Y").date()
                se.append({"date": dt.isoformat(),
                           "value": float(v.replace(",", ""))})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 15:
            env = {"series": se,
                   "fetched_at": datetime.now(
                       timezone.utc).isoformat(timespec="seconds"),
                   "via": "multpl " + slug}
            s3.put_object(Bucket=BUCKET, Key=wkey,
                          Body=json.dumps(env).encode(),
                          ContentType="application/json")
            return env
    except Exception:
        pass
    return None

def nasdaq_fallback(idx, budget):
    """NASDAQ:NQ* Global Index customs — api.nasdaq.com public
    historical JSON."""
    if budget["n"] <= 0:
        return None
    wkey = WARM + "nq_" + re.sub(r"[^A-Z0-9]", "_", idx) + ".json"
    cached = s3_json(wkey)
    if cached:
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            age_s = (datetime.now(timezone.utc) - ft
                     ).total_seconds()
            if cached.get("series") and age_s < 72000:
                return cached
            if cached.get("miss") and age_s < 86400:
                return None
        except Exception:
            pass
    budget["n"] -= 1
    try:
        frm = (datetime.now(timezone.utc)
               - timedelta_days(400)).strftime("%Y-%m-%d")
    except Exception:
        frm = "2025-06-01"
    try:
        qs = ("/api/quote/%s/historical?assetclass=index&limit=300"
              "&fromdate=%s&todate=%s"
              % (idx, frm,
                 datetime.now(timezone.utc).strftime("%Y-%m-%d")))
        d = None
        try:
            req = urllib.request.Request(
                "https://api.nasdaq.com" + qs, headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json, text/plain, */*",
                    "Origin": "https://www.nasdaq.com"})
            time.sleep(0.25)
            with urllib.request.urlopen(req, timeout=12) as h:
                d = json.loads(h.read())
        except Exception:
            d = None
        purl = os.environ.get("NQ_PROXY_URL")
        pkey = os.environ.get("NQ_PROXY_KEY")
        if d is None and purl and pkey:
            req = urllib.request.Request(
                "%s?k=%s&path=%s" % (purl, pkey,
                                     urllib.parse.quote(qs,
                                                        safe="")),
                headers={"User-Agent": "justhodl-fleet"})
            time.sleep(0.25)
            with urllib.request.urlopen(req, timeout=20) as h:
                d = json.loads(h.read())
        if d is None:
            return None
        rows = (((d.get("data") or {}).get("tradesTable")
                 or {}).get("rows")) or []
        se = []
        for o in rows:
            try:
                dt = datetime.strptime(o["date"],
                                       "%m/%d/%Y").date()
                se.append({"date": dt.isoformat(),
                           "value": float(str(o["close"])
                                          .replace(",", "")
                                          .replace("$", ""))})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 15:
            env = {"series": se,
                   "fetched_at": datetime.now(
                       timezone.utc).isoformat(timespec="seconds"),
                   "via": "api.nasdaq.com " + idx}
            s3.put_object(Bucket=BUCKET, Key=wkey,
                          Body=json.dumps(env).encode(),
                          ContentType="application/json")
            return env
    except Exception:
        return None  # transient/blocked: no negative cache
    try:
        s3.put_object(Bucket=BUCKET, Key=wkey,
                      Body=json.dumps({"miss": True,
                                       "fetched_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(
                                           timespec="seconds")}
                                      ).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    return None

def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None

def stoxx_fallback(budget):
    """EUREX:FVS* -> V2TX (VSTOXX index proxy) from STOXX's free
    historical txt (Date;Symbol;Value, DD.MM.YYYY)."""
    wkey = WARM + "stoxx_V2TX.json"
    cached = s3_json(wkey)
    if cached and cached.get("series"):
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 72000:
                return cached
        except Exception:
            pass
    if budget["n"] <= 0:
        return None
    budget["n"] -= 1
    try:
        req = urllib.request.Request(
            "https://www.stoxx.com/document/Indices/Current/"
            "HistoricalData/h_v2tx.txt",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as h:
            txt = h.read().decode("utf-8", "replace")
        se = []
        for line in txt.splitlines():
            parts = line.strip().split(";")
            if len(parts) < 3:
                continue
            try:
                dt = datetime.strptime(parts[0].strip(),
                                       "%d.%m.%Y").date()
                se.append({"date": dt.isoformat(),
                           "value": float(parts[2])})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 15:
            env = {"series": se[-400:],
                   "fetched_at": datetime.now(
                       timezone.utc).isoformat(timespec="seconds"),
                   "via": "STOXX V2TX · VSTOXX index proxy"}
            s3.put_object(Bucket=BUCKET, Key=wkey,
                          Body=json.dumps(env).encode(),
                          ContentType="application/json")
            return env
    except Exception:
        pass
    return None

def tcbs_fallback(budget):
    """HOSE:VNINDEX via the public TCBS bars endpoint."""
    wkey = WARM + "vn_VNINDEX.json"
    cached = s3_json(wkey)
    if cached and cached.get("series"):
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 72000:
                return cached
        except Exception:
            pass
    if budget["n"] <= 0:
        return None
    budget["n"] -= 1
    try:
        to = int(datetime.now(timezone.utc).timestamp())
        url = ("https://apipubaws.tcbs.com.vn/stock-insight/v1/"
               "stock/bars-long-term?ticker=VNINDEX&type=index"
               "&resolution=D&to=%d&countBack=300" % to)
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as h:
            d = json.loads(h.read())
        rows = d.get("data") or []
        se = []
        for o in rows:
            try:
                d0 = str(o.get("tradingDate", ""))[:10]
                se.append({"date": d0,
                           "value": float(o["close"])})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 15:
            env = {"series": se,
                   "fetched_at": datetime.now(
                       timezone.utc).isoformat(timespec="seconds"),
                   "via": "TCBS VNINDEX"}
            s3.put_object(Bucket=BUCKET, Key=wkey,
                          Body=json.dumps(env).encode(),
                          ContentType="application/json")
            return env
    except Exception:
        pass
    return None

def te_hist_fallback(sym, budget):
    """ECONOMICS residue via TE /historical per indicator — the
    4636 category-evidence dump made these names exact."""
    if not TE_KEY or sym not in TE_DIRECT or budget["n"] <= 0:
        return None
    cty, cat = TE_DIRECT[sym]
    wkey = WARM + "te_" + re.sub(r"[^A-Za-z0-9]", "_",
                                 sym.split(":", 1)[1]) + ".json"
    cached = s3_json(wkey)
    if cached and cached.get("series"):
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 72000:
                return cached
        except Exception:
            pass
    budget["n"] -= 1
    try:
        url = ("https://api.tradingeconomics.com/historical/"
               "country/%s/indicator/%s?c=%s&f=json"
               % (urllib.parse.quote(cty),
                  urllib.parse.quote(cat), TE_KEY))
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        time.sleep(0.3)
        with urllib.request.urlopen(req, timeout=20) as h:
            d = json.loads(h.read())
        se = []
        for o in d if isinstance(d, list) else []:
            v = o.get("Value")
            d0 = str(o.get("DateTime", ""))[:10]
            if v is None or len(d0) != 10:
                continue
            try:
                se.append({"date": d0, "value": float(v)})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 15:
            env = {"series": se[-400:],
                   "fetched_at": datetime.now(
                       timezone.utc).isoformat(timespec="seconds"),
                   "via": "TE hist %s/%s" % (cty, cat[:22])}
            s3.put_object(Bucket=BUCKET, Key=wkey,
                          Body=json.dumps(env).encode(),
                          ContentType="application/json")
            return env
    except Exception:
        pass
    return None

def timedelta_days(n):
    from datetime import timedelta as _td
    return _td(days=n)

def vault_index(vault):
    """4623 lesson: vault 'symbols' is a LIST of row dicts, not a
    map. Index once: exact symbol + base name (prefix stripped)."""
    if _VIDX.get("done"):
        return _VIDX
    rows = (vault or {}).get("symbols")
    if isinstance(rows, list):
        for rr in rows:
            if not isinstance(rr, dict):
                continue
            sy = rr.get("symbol")
            if not isinstance(sy, str):
                continue
            _VIDX[sy] = rr
            if ":" in sy and "/" not in sy and "-" not in sy:
                _VIDX.setdefault(sy.split(":", 1)[1], rr)
    _VIDX["done"] = True
    return _VIDX

def vault_lookup(vault, wb, list_name, sym):
    idx = vault_index(vault)
    hit = idx.get(sym)
    if hit is None and ":" in sym and "/" not in sym \
            and "-" not in sym:
        hit = idx.get(sym.split(":", 1)[1])
    if hit is not None:
        return hit
    if isinstance(wb, dict):
        wls = wb.get("watchlists") or wb.get("lists") or {}
        it = None
        if isinstance(wls, dict):
            it = wls.get(list_name)
        elif isinstance(wls, list):
            it = next((x for x in wls if isinstance(x, dict)
                       and (x.get("name") == list_name)), None)
        if isinstance(it, dict):
            for k in ("symbols", "members", "rows", "items"):
                rows = it.get(k)
                if isinstance(rows, list):
                    for rr in rows:
                        if isinstance(rr, dict) and rr.get(
                                "symbol") == sym:
                            return rr
                if isinstance(rows, dict) and sym in rows:
                    return rows[sym]
    return None

def yahoo_fallback(alias, budget, note=None, invert=False):
    """v1.5.0: the symbol-dictionary embeds '(MARKET: X)' aliases —
    fetch via Yahoo v8 chart (the fleet's own symbol-feed/fedwatch
    pattern), bounded + cumulatively cached."""
    if not alias:
        return None
    safe = re.sub(r"[^A-Za-z0-9._^=-]", "_", alias)
    wkey = WARM + "yh_" + safe + ".json"
    cached = s3_json(wkey)
    if cached:  # cache-first: NEVER gated by budget (4633 lesson)
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            age_s = (datetime.now(timezone.utc) - ft
                     ).total_seconds()
            if cached.get("series") and age_s < 72000:
                return cached
            if cached.get("miss") and age_s < 86400:
                return None
        except Exception:
            pass
    if budget["n"] <= 0:
        return None
    budget["n"] -= 1
    rate_limited = False
    for cand in (alias, "^" + alias
                 if not alias.startswith("^")
                 and "=" not in alias else None):
        if not cand:
            continue
        try:
            url = ("https://query1.finance.yahoo.com/v8/finance/"
                   "chart/%s?range=1y&interval=1d"
                   % urllib.parse.quote(cand))
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0"})
            time.sleep(0.25)
            with urllib.request.urlopen(req, timeout=12) as h:
                d = json.loads(h.read())
            res = ((d.get("chart") or {}).get("result")
                   or [None])[0]
            if not res:
                continue
            ts = res.get("timestamp") or []
            cl = (((res.get("indicators") or {}).get("quote")
                   or [{}])[0].get("close")) or []
            se = []
            for i in range(min(len(ts), len(cl))):
                if cl[i] is None:
                    continue
                se.append({"date": datetime.fromtimestamp(
                    ts[i], tz=timezone.utc).date().isoformat(),
                    "value": float(cl[i])})
            if len(se) >= 15:
                if invert:
                    se = [{"date": o["date"],
                           "value": round(1.0 / o["value"], 8)}
                          for o in se if o["value"]]
                env = {"series": se[-400:],
                       "fetched_at": datetime.now(
                           timezone.utc).isoformat(
                           timespec="seconds"),
                       "via": ("Yahoo " + cand
                               + (" inv" if invert else "")
                               + ((" · " + note)
                                  if note else ""))}
                s3.put_object(Bucket=BUCKET, Key=wkey,
                              Body=json.dumps(env).encode(),
                              ContentType="application/json")
                return env
        except urllib.error.HTTPError as e9:
            if e9.code in (429, 999):
                rate_limited = True
            continue
        except Exception:
            continue
    if rate_limited:
        return None  # transient: no negative cache
    try:
        s3.put_object(Bucket=BUCKET, Key=wkey,
                      Body=json.dumps({"miss": True,
                                       "fetched_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(
                                           timespec="seconds")}
                                      ).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    return None
