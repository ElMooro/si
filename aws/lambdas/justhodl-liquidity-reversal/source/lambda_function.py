"""justhodl-liquidity-reversal v1.1.0 (ops 4623)

Khalid's TradingView "blackswan" watchlist, run as an institutional
TAIL-RISK CANARY STRIP — the correct mapping for a list with that
name: these are alarms, not expansion legs.

Sources (all already in the fleet, zero new fetchers):
  data/tv-watchlists.json      — list membership (extension-synced;
                                 name match prefers 'swan' per the
                                 ops-4062 audit-gate lesson)
  data/tradingview.json        — Metric Vault live values/series
  data/tv-workbench.json       — fallback per-watchlist vault mirror
  data/symbol-dictionary.json  — human names

Per symbol: latest value, day-over-day move, |z| of that move vs the
symbol's own recent history, and position in its 1y range. Alarm
states carry NO direction semantics — a black-swan strip cares about
ANY violent move; Khalid reads direction from the named row. Output:
data/blackswan-watch.json + a composite alarm the physical-economy
canary board consumes (observed tier, never load-bearing).
"""
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/liquidity-reversal.json"
WARM = "data/warm/blackswan/"
FRED_KEY = os.environ.get("FRED_API_KEY",
                          "2f057499936072679d8843d7fce99989")
FRED_BUDGET = 110  # bounded per run; cumulative via warm cache
YH_BUDGET = {"n": 70}
CUR_BUDGET = {"n": 25}   # curated symbols + curated composite legs
CB_BUDGET = {"n": 12}    # CBOE CDN route
MP_BUDGET = {"n": 12}    # multpl route
NQ_BUDGET = {"n": 18}    # api.nasdaq.com index historicals
ST_BUDGET = {"n": 2}     # STOXX txt (single cached doc)
VN_BUDGET = {"n": 2}     # TCBS (single cached doc)
TE_BUDGET = {"n": 8}     # TE historical per-indicator
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
CURATED_FRED = {"FX:SONIA3M": ("IUDSOIA",
                               "O/N SONIA proxy for 3M")}  # Yahoo-chart alias fallback, same pattern
s3 = boto3.client("s3")


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


def timedelta_days(n):
    from datetime import timedelta as _td
    return _td(days=n)


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


TENOR_FRED = {"TVC:DE10Y": "IRLTLT01DEM156N",
              "TVC:IT10Y": "IRLTLT01ITM156N",
              "TVC:FR10Y": "IRLTLT01FRM156N",
              "TVC:ES10Y": "IRLTLT01ESM156N",
              "TVC:JP10Y": "IRLTLT01JPM156N"}


def leg_series(leg, budget):
    """v1.7.0 universal composite leg resolver: FRED (incl
    ECONOMICS twins + TVC:US*Y), curated Yahoo, MULTPL."""
    if re.fullmatch(r"\d+(\.\d+)?", leg):
        return {"series": [], "const": float(leg)}
    sid = leg_to_fred(leg)
    if sid is None and leg in ECON_FRED:
        sid = ECON_FRED[leg]
    if sid is None and leg in TENOR_FRED:
        sid = TENOR_FRED[leg]
    if sid:
        return fred_fallback("FRED:" + sid, budget)
    if leg in CURATED_LEG_YH:
        return yahoo_fallback(CURATED_LEG_YH[leg], CUR_BUDGET)
    if leg.startswith("MULTPL:"):
        return multpl_fallback(leg.split(":", 1)[1], MP_BUDGET)
    # plain exchange-ticker legs: ma200 buffer (free) then Yahoo
    if re.fullmatch(r"[A-Z]+:[A-Z0-9]{1,8}", leg):
        fb = ma200_series(leg)
        if fb:
            return fb
        return yahoo_fallback(leg.split(":", 1)[1], YH_BUDGET,
                              "leg heuristic")
    return None


def dict_aliases(dic, sym):
    """Parse '(FRED: SID)' and '(MARKET: Y)' out of the
    symbol-dictionary name — the estate's own rosetta stone."""
    nm = dict_name(dic, sym) or ""
    fm = re.search(r"\(FRED:\s*([A-Z0-9]+)\)", nm)
    mm = re.search(r"\(MARKET:\s*([^)]+)\)", nm)
    return (fm.group(1) if fm else None,
            mm.group(1).strip() if mm else None)


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


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def find_swan_list(wl):
    """Walk any plausible watchlist container shape; prefer names
    containing 'swan' (the 4062 lesson), else 'black'."""
    cands = []

    def add(name, symbols):
        if name and symbols:
            cands.append((str(name), [str(x) for x in symbols
                                      if isinstance(x, str)]))

    def syms_of(v):
        if isinstance(v, list):
            out = []
            for x in v:
                if isinstance(x, str):
                    out.append(x)
                elif isinstance(x, dict):
                    for k in ("symbol", "ticker", "s", "name"):
                        if isinstance(x.get(k), str):
                            out.append(x[k])
                            break
            return out
        if isinstance(v, dict):
            for k in ("symbols", "members", "tickers", "items"):
                if isinstance(v.get(k), list):
                    return syms_of(v[k])
        return []

    if isinstance(wl, dict):
        for k in ("watchlists", "lists", "data"):
            if isinstance(wl.get(k), dict):
                for name, v in wl[k].items():
                    add(name, syms_of(v))
            if isinstance(wl.get(k), list):
                for it in wl[k]:
                    if isinstance(it, dict):
                        add(it.get("name") or it.get("title")
                            or it.get("list"), syms_of(it))
        for name, v in wl.items():
            if isinstance(v, (list, dict)) and name not in (
                    "watchlists", "lists", "data"):
                add(name, syms_of(v))
    for pref in ("liquidity", "liquid"):
        for name, syms in cands:
            if pref in name.lower():
                return name, syms, len(cands)
    return None, [], len(cands)


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


# ── fleet-join resolution (Khalid: "find the missing data from
# other engines") — column-aware history extraction over the
# fleet's own published stores, loaded once per run. ─────────────
FLEET_MAP = {
    "TVC:MOVE": ("data/move-index.json", None),
    "TVC:VIX": ("data/vix-curve-history.json", "vix"),
    "CBOE:VIX3M": ("data/vix-curve-history.json", "vix3m"),
    "CBOE:VXN": ("data/vix-curve-history.json", "vxn"),
}  # DXY dropped: dollar-radar-history holds derived scores only
ECON_FRED = {
    "ECONOMICS:USWEI": "WEI", "ECONOMICS:USJC": "ICSA",
    "ECONOMICS:USIJC": "ICSA", "ECONOMICS:USJC4W": "IC4WSA",
    "ECONOMICS:USCJC": "CCSA", "ECONOMICS:USCU": "TCU",
    "ECONOMICS:USBP": "PERMIT",
    "ECONOMICS:USGDPQQ": "A191RL1Q225SBEA",
    "ECONOMICS:USINBR": "DFF",
    "ECONOMICS:USJO": "JTSJOL", "ECONOMICS:USNO": "AMTMNO",
    "ECONOMICS:USTVS": "TOTALSA", "ECONOMICS:USMEMP": "MANEMP",
}
CURATED_LEG_YH = {"ICEUS:DX1!": "DX=F", "TVC:DXY": "DX-Y.NYB"}
CURATED_YH = {
    "ICEUS:DX1!": ("DX=F", None),
    "TVC:DXY": ("DX-Y.NYB", None),
    "NSE:CNXSMALLCAP": ("^CNXSC", None),
    "KRX:KOSPI200": ("^KS200", None),
    "EURONEXT:N100": ("^N100", None),
    "EURONEXT:PX1": ("^FCHI", None),
    "TVC:CAC40": ("^FCHI", None),
    "OMXSTO:OMXS30": ("^OMX", None),
    "INDEX:FTSEMIB": ("FTSEMIB.MI", None),
    "PSE:PSEI": ("PSEI.PS", None),
    "SPCFD:S5INDU": ("^SP500-20", None),
    "TSE:TOPIX": ("1475.T", "TOPIX ETF proxy"),
    "EUREX:FESX2!": ("^STOXX50E", "index proxy for futures"),
    "EUREX:FMEA2!": ("EEMA", "EM-Asia ETF proxy"),
    "EUREX:FMAA1!": ("AAXJ", "AC-Asia ETF proxy"),
    "SGX:SGP2!": ("^STI", "STI index proxy"),
    "OANDA:SG30SGD": ("^STI", "STI index proxy"),
    "EUREX:FMXU2!": ("ACWX", "world-ex-US ETF proxy"),
    "ASX24:AP1!": ("^AXJO", "index proxy for SPI futures"),
    "CBOT:ZB2!": ("ZB=F", "front-month proxy"),
    "CBOT:UB2!": ("UB=F", "front-month proxy"),
    "CME:SR32!": ("SR3=F", "front-month proxy"),
}
_FLEET_CACHE = {}


def fleet_doc(key):
    if key not in _FLEET_CACHE:
        _FLEET_CACHE[key] = s3_json(key)
    return _FLEET_CACHE[key]


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


TVC_MAP = {"US01MY": "DGS1MO", "US03MY": "DGS3MO",
           "US06MY": "DGS6MO", "US01Y": "DGS1", "US02Y": "DGS2",
           "US03Y": "DGS3", "US05Y": "DGS5", "US07Y": "DGS7",
           "US10Y": "DGS10", "US20Y": "DGS20", "US30Y": "DGS30"}


def leg_to_fred(leg):
    if leg.startswith("FRED:"):
        return leg.split(":", 1)[1]
    if leg.startswith("TVC:") and leg.split(":", 1)[1] in TVC_MAP:
        return TVC_MAP[leg.split(":", 1)[1]]
    return None


COMP_BUDGET = {"n": 40}  # reserved: composite legs (FEDFUNDS-class
# is never a standalone member, so the shared budget starved them)


def _tok(sym):
    out, cur = [], ""
    for ch in sym:
        if ch in "()+-/":
            if ch == "-" and (not out or out[-1] in "()+-/") \
                    and not cur:
                cur += ch
                continue
            if cur:
                out.append(cur)
                cur = ""
            out.append(ch)
        else:
            cur += ch
    if cur:
        out.append(cur)
    return out


def _parse(tokens, pos, budget, depth=0):
    """Recursive-descent over + - / with parentheses and numeric
    constants. Returns (value_map_or_const, pos) or (None, why)."""
    if depth > 4:
        return None, "depth"

    def prim(p):
        if p < len(tokens) and tokens[p] == "(":
            v, p2 = _parse(tokens, p + 1, budget, depth + 1)
            if v is None:
                return None, p2
            if p2 >= len(tokens) or tokens[p2] != ")":
                return None, "unclosed paren"
            return v, p2 + 1
        if p >= len(tokens):
            return None, "eof"
        leg = tokens[p]
        env = leg_series(leg, budget)
        if env is None:
            return None, "leg %s unresolvable" % leg[:22]
        if env.get("const") is not None:
            return env["const"], p + 1
        return {o["date"]: o["value"]
                for o in env["series"]}, p + 1
    v, p = prim(pos)
    if v is None:
        return None, p
    while isinstance(p, int) and p < len(tokens) \
            and tokens[p] in "+-/":
        op = tokens[p]
        rhs, p2 = prim(p + 1)
        if rhs is None:
            return None, p2
        v = _combine(v, rhs, op)
        if v is None:
            return None, "combine fail"
        p = p2
    return v, p


def _combine(a, b, op):
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if op == "+":
            return a + b
        if op == "-":
            return a - b
        return a / b if b else None
    if isinstance(a, (int, float)):
        a = {d: a for d in b}
    if isinstance(b, (int, float)):
        b = {d: b for d in a}
    cal = sorted(set(a) | set(b))[-300:]
    fa, fb = {}, {}
    la = lb = None
    ka, kb = sorted(a), sorted(b)
    ia = ib = 0
    for d in cal:
        while ia < len(ka) and ka[ia] <= d:
            la = a[ka[ia]]
            ia += 1
        while ib < len(kb) and kb[ib] <= d:
            lb = b[kb[ib]]
            ib += 1
        if la is not None:
            fa[d] = la
        if lb is not None:
            fb[d] = lb
    out = {}
    for d in cal:
        if d not in fa or d not in fb:
            continue
        x, y = fa[d], fb[d]
        if op == "+":
            out[d] = x + y
        elif op == "-":
            out[d] = x - y
        else:
            if not y:
                continue
            out[d] = x / y
    return out or None


def eval_composite(sym, budget):
    budget = COMP_BUDGET
    why = []
    if "(" in sym or re.match(r"^\d", sym):
        toks = _tok(sym)
        v, p = _parse(toks, 0, budget)
        if isinstance(v, dict) and len(v) >= 15:
            se = [{"date": d, "value": round(v[d], 6)}
                  for d in sorted(v)[-300:]]
            return {"series": se}
        return {"comp_why": "grammar: %s"
                % (p if isinstance(p, str) else "short")}
    """v1.2.0: evaluate A-B / A/B / A+B formulas whose every leg is
    FRED-mappable — unlocks the plumbing spreads (SOFR-FF, 30s10s,
    IG-vs-10Y...) with full z+range basis via the warm cache."""
    for op in ("-", "/", "+"):
        parts = sym.split(op)
        if len(parts) < 2 or any(not p for p in parts):
            continue
        legs = []
        bad = None
        for p2 in parts:
            env = leg_series(p2, budget)
            if not env or not env.get("series"):
                bad = "%s:leg %s unresolvable (budget %d)" % (
                    op, p2[:22], budget["n"])
                break
            legs.append({o["date"]: o["value"]
                         for o in env["series"]})
        if bad:
            why.append(bad)
            return {"comp_why": " | ".join(why)[:160]}
        # union calendar (4630 r4 confession: len-tie picked the
        # sparse monthly leg — 13 dates). Union guarantees daily
        # granularity whenever any leg is daily.
        cal = set()
        for m in legs:
            cal |= set(m.keys())
        common = sorted(cal)[-300:]
        filled = []
        for m in legs:
            keys = sorted(m.keys())
            f, j, lastv = {}, 0, None
            for d0 in common:
                while j < len(keys) and keys[j] <= d0:
                    lastv = m[keys[j]]
                    j += 1
                if lastv is not None:
                    f[d0] = lastv
            filled.append(f)
        legs = filled
        common = [d0 for d0 in common
                  if all(d0 in m for m in legs)]
        if len(common) < 15:
            return {"comp_why": "%s:only %d ffilled common dates"
                    % (op, len(common))}
        se = []
        for d0 in common:
            v = legs[0][d0]
            for m in legs[1:]:
                if op == "-":
                    v = v - m[d0]
                elif op == "+":
                    v = v + m[d0]
                else:
                    if not m[d0]:
                        v = None
                        break
                    v = v / m[d0]
            if v is not None:
                se.append({"date": d0, "value": round(v, 6)})
        if len(se) >= 15:
            return {"series": se}
        return {"comp_why": "%s:series shrank to %d" % (op, len(se))}
    return {"comp_why": (" | ".join(why) or "no op matched")[:160]}


_VIDX = {}


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


def trend_metrics(vals, diff_basis):
    """Reversal core: 1M vs 3M momentum, statistical force of the
    1M move, sign-flip age, MA50 cross recency. No direction
    semantics fabricated — the named rows carry meaning."""
    out = {}
    if len(vals) < 70:
        return out

    def chg(a, b):
        if diff_basis:
            return a - b
        return ((a / b - 1) * 100) if b else None
    r21 = chg(vals[-1], vals[-22])
    r63 = chg(vals[-1], vals[-64])
    hist21 = []
    for i in range(64, len(vals)):
        c = chg(vals[i], vals[i - 21])
        if c is not None:
            hist21.append(c)
    z21 = None
    if len(hist21) >= 30 and r21 is not None:
        mu = sum(hist21) / len(hist21)
        sd = math.sqrt(sum((x - mu) ** 2 for x in hist21)
                       / (len(hist21) - 1))
        if sd:
            z21 = (r21 - mu) / sd
    turn_age = None
    if len(vals) >= 90:
        signs = []
        for i in range(len(vals) - 40, len(vals)):
            c = chg(vals[i], vals[i - 21])
            signs.append(1 if (c or 0) > 0 else -1)
        cur = signs[-1]
        age = 0
        for sg in reversed(signs):
            if sg == cur:
                age += 1
            else:
                break
        turn_age = age
    ma50_cross = None
    if len(vals) >= 56:
        for k in range(1, 6):
            wend = len(vals) - k + 1
            ma_now = sum(vals[wend - 50:wend]) / 50
            ma_prev = sum(vals[wend - 51:wend - 1]) / 50
            above = vals[wend - 1] > ma_now
            was_above = vals[wend - 2] > ma_prev
            if above != was_above:
                ma50_cross = {"sessions_ago": k,
                              "direction": "UP" if above
                              else "DOWN"}
                break
    st = "FLAT"
    if r21 is not None and r63 is not None:
        s21 = 1 if r21 > 0 else -1
        s63 = 1 if r63 > 0 else -1
        forceful = z21 is not None and abs(z21) >= 1.0
        if s21 != s63 and forceful:
            st = "TURN_UP" if s21 > 0 else "TURN_DOWN"
        elif s63 > 0:
            st = "TREND_UP"
        else:
            st = "TREND_DOWN"
    rnd = (3 if diff_basis else 2)
    out = {"ret_1m": round(r21, rnd) if r21 is not None else None,
           "ret_3m": round(r63, rnd) if r63 is not None else None,
           "z_1m": round(z21, 2) if z21 is not None else None,
           "reversal_state": st,
           "turn_age_days": turn_age,
           "ma50_cross": ma50_cross}
    return out


def analyze(sym, node, name):
    se = extract_series(node)
    if se and len(se) >= 15:
        vals = [o["value"] for o in se]
        last, prev = vals[-1], vals[-2]
        lab = cadence_label(se)
        w = vals[-260:]
        lo, hi = min(w), max(w)
        # v1.2.0 basis rule: sign-crossing or near-zero series get
        # DIFFERENCE basis — pct-change there is division-by-noise
        # (the RRP/NFCI/CFNAI class from Khalid's page audit).
        diffs = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
        dmu = sum(diffs[-90:]) / max(len(diffs[-90:]), 1)
        dsd = math.sqrt(sum((x - dmu) ** 2 for x in diffs[-90:])
                        / max(len(diffs[-90:]) - 1, 1))
        mean_lvl = sum(w) / len(w)
        diff_basis = (lo < 0 < hi) or (
            dsd > 0 and abs(mean_lvl) < 2 * dsd)
        if diff_basis:
            chg = last - prev
            z = abs((chg - dmu) / dsd) if dsd else None
            chg_str = "%+.3g Δ (%s)" % (chg, lab)
            dod = None
        else:
            dod = ((last / prev - 1) * 100) if prev else None
            rets = [(vals[i] / vals[i - 1] - 1) * 100
                    for i in range(1, len(vals)) if vals[i - 1]]
            tail = rets[-90:]
            mu = sum(tail) / len(tail)
            sd = math.sqrt(sum((x - mu) ** 2 for x in tail)
                           / max(len(tail) - 1, 1))
            z = (abs((dod - mu) / sd)
                 if (sd and dod is not None) else None)
            chg_str = ("%+.2f%% (%s)" % (dod, lab)
                       if dod is not None else None)
        pos = ((last - lo) / (hi - lo) * 100) if hi > lo else 50.0
        move_state = ("RED" if z is not None and z >= 3 else
                      "AMBER" if z is not None and z >= 2 else "CALM")
        range_state = ("EXTREME" if pos <= 5 or pos >= 95 else
                       "STRETCHED" if pos <= 12 or pos >= 88 else
                       "NORMAL")
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(
                       se[-1]["date"]).replace(
                       tzinfo=timezone.utc)).days
        except Exception:
            age = None
        row = {"symbol": sym, "name": name, "resolved": True,
                "last": round(last, 4),
                "dod_pct": round(dod, 2) if dod is not None else None,
                "chg_str": chg_str,
                "basis": "diff-z" if diff_basis else "pct-z",
                "move_z": round(z, 2) if z is not None else None,
                "move_state": move_state,
                "range_pos_pct": round(pos, 1),
                "range_state": range_state,
                "n_obs": len(se), "data_age_days": age}
        row.update(trend_metrics(vals, diff_basis))
        cad_days = {"DoD": 4, "WoW": 10, "MoM": 45,
                    "QoQ": 120}.get(lab, 45)
        if age is not None and age > max(45, 3 * cad_days):
            row["stale"] = True
            row["move_state"] = "STALE"
            row["reversal_state"] = None
        return row
    lv, ld = extract_latest(node)
    if lv is not None:
        prev = None
        if isinstance(node, dict):
            for k in ("prev", "previous", "prior"):
                if isinstance(node.get(k), (int, float)):
                    prev = float(node[k])
                    break
        dod = None
        if isinstance(node, dict) and isinstance(
                node.get("chg_pct"), (int, float)):
            dod = float(node["chg_pct"])
        elif prev:
            dod = (lv / prev - 1) * 100
        age2 = None
        if isinstance(node, dict) and node.get("date"):
            try:
                age2 = (datetime.now(timezone.utc).date()
                        - datetime.fromisoformat(
                            str(node["date"])[:10]).date()).days
            except Exception:
                pass
        ms = "NO_HISTORY"
        if dod is not None:
            # pct-basis (no sigma) can never mint RED — page-audit
            # lesson: EPU-class daily indexes swing 40%+ normally.
            ms = "AMBER" if abs(dod) >= 7 else "CALM"
        return {"symbol": sym, "name": name, "resolved": True,
                "last": round(lv, 4),
                "dod_pct": round(dod, 2) if dod is not None
                else None,
                "move_z": None, "move_state": ms,
                "basis": "pct-move (no per-symbol sigma yet)"
                if dod is not None else "level-only",
                "range_pos_pct": None, "range_state": "NO_HISTORY",
                "n_obs": 2 if prev else 1,
                "via": (node.get("via")
                        if isinstance(node, dict) else None),
                "data_age_days": age2}
    out = {"symbol": sym, "name": name, "resolved": False,
           "move_state": "UNRESOLVED", "range_state": "UNRESOLVED"}
    if isinstance(node, dict) and node.get("comp_why"):
        out["detail"] = node["comp_why"]
    return out


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    wl = s3_json("data/tv-watchlists.json")
    vault = s3_json("data/tradingview.json")
    wb = s3_json("data/tv-workbench.json")
    dic = s3_json("data/symbol-dictionary.json")

    list_name, syms, n_lists = find_swan_list(wl)
    rows = []
    budget = {"n": FRED_BUDGET}
    if list_name:
        for sym in syms[:520]:
            node = vault_lookup(vault, wb, list_name, sym)
            if (node is None or not extract_series(node)) \
                    and sym.startswith("FRED:"):
                fb = fred_fallback(sym, budget)
                if fb:
                    node = fb
            if (node is None or not extract_series(node)) \
                    and any(op in sym for op in "-/+") \
                    and ":" in sym:
                fb = eval_composite(sym, budget)
                if fb and fb.get("series"):
                    node = fb
                elif fb and fb.get("comp_why"):
                    node = (node if isinstance(node, dict)
                            else {}) | {"comp_why": fb["comp_why"]}
            if node is None or not extract_series(node):
                if sym in FLEET_MAP:
                    fb = fleet_column_series(*FLEET_MAP[sym])
                    if fb:
                        node = fb
                elif sym in ECON_FRED:
                    fb = fred_fallback("FRED:" + ECON_FRED[sym],
                                       budget)
                    if fb:
                        node = fb
                elif sym.startswith("ECONOMICS:"):
                    te = fleet_doc("data/te-feed.json") or {}
                    hit = (te.get("prices") or {}).get(
                        sym.split(":", 1)[1])
                    if hit and hit.get("value") is not None:
                        node = {"value": hit["value"],
                                "date": hit.get("asof"),
                                "via": "TE latest"}
                else:
                    fb = ma200_series(sym)
                    if fb:
                        node = fb
            if node is None or not extract_series(node):
                fsid, yh = dict_aliases(dic, sym)
                if fsid:
                    fb = fred_fallback("FRED:" + fsid, budget)
                    if fb:
                        node = fb
                if (node is None or not extract_series(node)) \
                        and yh:
                    fb = yahoo_fallback(yh, YH_BUDGET)
                    if fb:
                        node = fb
            if node is None or not extract_series(node):
                if sym in CURATED_YH:
                    ya, note = CURATED_YH[sym]
                    fb = yahoo_fallback(ya, CUR_BUDGET, note)
                    if fb:
                        node = fb
                elif sym in CURATED_FRED:
                    fsid2, note2 = CURATED_FRED[sym]
                    fb = fred_fallback("FRED:" + fsid2, budget)
                    if fb:
                        fb = dict(fb)
                        fb["via"] = "FRED %s · %s" % (fsid2, note2)
                        node = fb
                elif sym.split(":", 1)[0] in (
                        "FX_IDC", "FX", "SAXO", "OANDA") and len(
                        sym.split(":", 1)[1]) == 6:
                    pair = sym.split(":", 1)[1]
                    fb = yahoo_fallback(pair + "=X", YH_BUDGET)
                    if not fb:
                        fb = yahoo_fallback(
                            pair[3:] + pair[:3] + "=X",
                            YH_BUDGET, "inverted cross",
                            invert=True)
                    if fb:
                        node = fb
                elif sym.startswith("MULTPL:"):
                    fb = multpl_fallback(sym.split(":", 1)[1],
                                         MP_BUDGET)
                    if fb:
                        node = fb
                elif sym.startswith("CBOE:"):
                    fb = cboe_fallback(sym.split(":", 1)[1],
                                       CB_BUDGET)
                    if fb:
                        node = fb
                elif sym.startswith("NASDAQ:") and \
                        re.fullmatch(r"NASDAQ:(NQ|B3)[A-Z0-9]+",
                                     sym):
                    fb = nasdaq_fallback(sym.split(":", 1)[1],
                                         NQ_BUDGET)
                    if fb:
                        node = fb
                elif sym.startswith("EUREX:FVS"):
                    fb = stoxx_fallback(ST_BUDGET)
                    if fb:
                        node = fb
                elif sym == "HOSE:VNINDEX":
                    fb = tcbs_fallback(VN_BUDGET)
                    if fb:
                        node = fb
                elif sym in TE_DIRECT:
                    fb = te_hist_fallback(sym, TE_BUDGET)
                    if fb:
                        node = fb
                elif re.fullmatch(r"(NASDAQ|NYSE|AMEX|CBOE|BATS|"
                                  r"ICEUS|SPCFD|CME|CBOT|NYMEX|"
                                  r"COMEX):[A-Z0-9]{2,12}", sym):
                    fb = yahoo_fallback(sym.split(":", 1)[1],
                                        YH_BUDGET, "heuristic")
                    if fb:
                        node = fb
            rows.append(analyze(sym, node, dict_name(dic, sym)))
    resolved = [r for r in rows if r.get("resolved")]
    with_hist = [r for r in resolved
                 if r.get("move_state") in ("CALM", "AMBER", "RED")]
    n_red = sum(1 for r in with_hist if r["move_state"] == "RED")
    n_amber = sum(1 for r in with_hist if r["move_state"] == "AMBER")
    n_extreme = sum(1 for r in with_hist
                    if r.get("range_state") == "EXTREME")
    if n_red >= 2 or (n_red >= 1 and n_amber >= 3):
        alarm = "RED"
    elif n_red >= 1 or n_amber >= 3 or n_extreme >= 5:
        alarm = "AMBER"
    else:
        alarm = "CALM"
    top = sorted(with_hist,
                 key=lambda r: -(r.get("move_z") or 0))[:5]
    resolved_live = [r for r in resolved if not r.get("stale")]
    tr = [r for r in resolved_live if r.get("reversal_state")]
    n_tu = sum(1 for r in tr if r["reversal_state"] == "TURN_UP")
    n_td = sum(1 for r in tr if r["reversal_state"] == "TURN_DOWN")
    n_up = sum(1 for r in tr if r["reversal_state"] == "TREND_UP")
    n_dn = sum(1 for r in tr
               if r["reversal_state"] == "TREND_DOWN")
    ntr = max(len(tr), 1)
    p_tu, p_td = 100.0 * n_tu / ntr, 100.0 * n_td / ntr
    gv = round(max(0.0, min(100.0, 50 + (p_tu - p_td))), 1)
    if p_tu >= 15 and p_tu > 2 * p_td:
        glabel = "REVERSING_UP"
    elif p_td >= 15 and p_td > 2 * p_tu:
        glabel = "REVERSING_DOWN"
    elif (n_up + n_dn) >= 0.6 * ntr:
        glabel = "TRENDING"
    else:
        glabel = "MIXED"
    fresh = sorted([r for r in tr
                    if r["reversal_state"] in ("TURN_UP",
                                               "TURN_DOWN")],
                   key=lambda r: (r.get("turn_age_days") or 99,
                                  -abs(r.get("z_1m") or 0)))[:8]
    gauge = {"value": gv, "label": glabel,
             "breadth": {"turning_up_pct": round(p_tu, 1),
                         "turning_down_pct": round(p_td, 1),
                         "trend_up_pct": round(100.0 * n_up
                                               / ntr, 1),
                         "trend_down_pct": round(100.0 * n_dn
                                                 / ntr, 1),
                         "n_trend_capable": len(tr)},
             "top_fresh_turns": [
                 {"symbol": f["symbol"],
                  "direction": f["reversal_state"],
                  "turn_age_days": f.get("turn_age_days"),
                  "ret_1m": f.get("ret_1m"),
                  "z_1m": f.get("z_1m")} for f in fresh],
             "doctrine": "breadth of statistically forceful "
                         "1M-vs-3M sign divergences; direction "
                         "semantics live in the named rows"}
    z_rows = [r for r in resolved_live
              if r.get("move_z") is not None]
    rng = [r for r in resolved_live
           if r.get("range_pos_pct") is not None]
    shock_b = (100.0 * sum(1 for r in z_rows
                           if r["move_z"] >= 2) / len(z_rows)
               if z_rows else 0.0)
    ext_b = (100.0 * sum(1 for r in rng
                         if r.get("range_state") == "EXTREME")
             / len(rng) if rng else 0.0)
    str_b = (100.0 * sum(1 for r in rng
                         if r.get("range_state") == "STRETCHED")
             / len(rng) if rng else 0.0)
    baro = round(min(100.0, 0.45 * shock_b + 0.40 * ext_b
                     + 0.15 * str_b), 1)
    blabel = ("CRITICAL" if baro >= 80 else
              "HIGH" if baro >= 60 else
              "ELEVATED" if baro >= 40 else
              "WATCHFUL" if baro >= 20 else "QUIET")
    top_ext = sorted(rng, key=lambda r: -abs(
        r["range_pos_pct"] - 50))[:6]
    barometer = {
        "value": baro, "label": blabel,
        "components": {
            "shock_breadth_pct": round(shock_b, 1),
            "range_extreme_pct": round(ext_b, 1),
            "range_stretched_pct": round(str_b, 1),
            "weights": {"shock": 0.45, "extreme": 0.40,
                        "stretched": 0.15},
            "n_z_rows": len(z_rows), "n_range_rows": len(rng)},
        "top_extremes": [
            {"symbol": t["symbol"],
             "range_pos_pct": t["range_pos_pct"]}
            for t in top_ext],
        "doctrine": "breadth of >=2-sigma shocks (45%) + breadth "
                    "of 1y range extremes (40%) + stretched (15%) "
                    "across the whole Black Swan strip"}
    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-liquidity-reversal",
        "as_of": now.isoformat(timespec="seconds"),
        "list_name": list_name,
        "n_lists_seen": n_lists,
        "n_members": len(syms),
        "n_resolved": len(resolved),
        "n_with_history": len(with_hist),
        "barometer": barometer,
        "gauge": gauge,
        "strip": {"alarm": alarm, "n_red": n_red,
                  "n_amber": n_amber, "n_range_extreme": n_extreme,
                  "top_movers": [
                      {"symbol": t["symbol"], "z": t.get("move_z"),
                       "dod_pct": t.get("dod_pct")} for t in top]},
        "rows": sorted(rows, key=lambda r: -(r.get("move_z") or -1)),
        "doctrine": "tail-risk alarm strip: |z| of the daily move vs "
                    "own history + 1y range extremes; no direction "
                    "semantics fabricated — read the named rows",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return {"statusCode": 200,
            "body": json.dumps({"ok": bool(list_name)
                                and len(resolved) >= 10,
                                "list": list_name,
                                "resolved": len(resolved),
                                "alarm": alarm})}
