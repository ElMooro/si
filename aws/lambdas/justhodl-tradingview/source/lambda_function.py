"""
justhodl-tradingview v1.0 — THE TRADINGVIEW METRIC VAULT (brain-constitutional).

Khalid harvested ~2,920 notes across ~524 distinct symbols from TradingView
into the brain. This engine turns that harvest into a LIVE vault:

  1. REGISTRY FROM THE BRAIN ITSELF (constitutional): parses data/brain.json
     for every [TV:RAW] tag -> symbol registry with note ids, note snippets,
     exchange prefixes seen, and note counts. Self-updating: add a TV note to
     the brain and the vault picks it up next run. No hand-maintained list.
  2. RESOLVER LADDER per symbol class (source decided by the exchanges the
     brain actually saw, never guessed):
       - any raw tag carried FRED:            -> FRED API (latest 2 obs)
       - NASDAQ:/NYSE:/AMEX: (or bare equity) -> FMP /stable batch quotes
       - futures (! suffix), ECONOMICS:, and TV-only symbols -> UNRESOLVED,
         honestly, with the brain note preserved (TradingView/TradingEconomics
         have no free API; documented, not faked).
  3. CATEGORIES auto-bucketed: rates, credit, plumbing, vol, futures, macro,
     equity, etf, index, commodity, fx, crypto, other.
  4. Output data/tradingview.json: per-symbol {symbol, category, n_notes,
     note_ids, exchanges, source, status LIVE/UNRESOLVED, value, prev, chg_pct,
     note_snippet} + coverage stats + by_category boards.

Phase 2 (separate ops): wire the vault into justhodl-risk-gate and every
other engine that needs these metrics.
"""
import json
import os
import re
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import boto3

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/tradingview.json"
MARKER = "tradingview-vault v2.3 SUFFIX-KEYS"

s3 = boto3.client("s3")

EQ_EX = {"NASDAQ", "NYSE", "AMEX", "DUS", "MC", "TSE", "HKEX", "TWSE", "KRX"}
CAT_RULES = [
    ("credit", re.compile(r"^BAML|^HQMCB|FALN|^JNK$|^HYG$|^SJNK$|^BKLN$|^CLOZ$|^IBHY$|^HYEM$|^HYXU$|^FLTR$|^CWB$")),
    ("plumbing", re.compile(r"^RRP|^RPON|^WREPO|^WRESBAL|^WTREGEN|^WDTGAL|^TOTRESNS|^BOGMB|^CASACB|^WALCL|^WLRRA|^WORAL|^WRBWFRBL|^H41RES|^MMMF|^SOFR|^AMERIBOR|^CPFF|^DCPN|^DCPF|^RIFSPP|^TEDRATE|^DPCREDIT|^EFFR$|^IORB$|^WLCFL|^WLODLL")),
    ("rates", re.compile(r"^US\d{2}M?Y$|^DGS|^DTB|^TB4WK|^FEDFUNDS|^THREEF|^TNX$|INTR$|^GB\d{2}Y|^EU\d{2}Y|^DE02Y|^JP0|^CH0|^CN\d{2}Y|^IT10Y|^NO03Y|^SS03|^GDPNOW")),
    ("vol", re.compile(r"^MOVE$|^VIX|^VVIX$|^SKEW$|^VOLI$|^VX|^SDEX$|^TDEX$|^UVXY$|^SVXY$|^NFCI")),
    ("fx", re.compile(r"^DXY$|^DTWEX|^RBUSBIS|^TWEXP|^XAUUSD|^USDX$|^USD$|^XD[NE]$|^UUP$|^USDU$|^6J")),
    ("commodity", re.compile(r"^GOLD$|^SILVER$|^WTI$|^USOIL$|^COPPER$|^PLATINUM$|^PALLADIUM$|^P[A-Z]{3,5}USDM$|^PIOREC|^PRAWM|^BDI$|^CPER$|^UGA$|^GC\d|^HG\d|^CL\d")),
    ("macro", re.compile(r"YY$|^UNEMPLOY|^ICSA$|^HOUST|^USNFP|^USLEI|^SAHM|CFNAI|^STLPPM|^USALOL|^ONMLOL|PMI$|^JPLG$|TOT$|^BOTOT|^MABOT|^USM[0-2]$|^PPIACO|^GACDFS|^TCU$|^MCUMFN|^TEMPHELP|^MSACSR|^AISRSA|^USHMI|^USBCOI|^DRTSC|^CSCICP|^SPASTT|^MAN_PMI")),
]


# ── v2.0: explicit alias ladder (exact, high-confidence only) ──────────────
ALIASES = {
    # rates -> FRED
    "US01Y": "fred:DGS1", "US02Y": "fred:DGS2", "US03Y": "fred:DGS3",
    "US05Y": "fred:DGS5", "US10Y": "fred:DGS10", "US30Y": "fred:DGS30",
    "US01MY": "fred:DGS1MO", "US03MY": "fred:DGS3MO", "US06MY": "fred:DGS6MO",
    "TNX": "fred:DGS10", "VIX": "fred:VIXCLS", "WTI": "fred:DCOILWTICO",
    "USOIL": "fred:DCOILWTICO", "TEDRATE": "fred:TEDRATE",
    "US02MY": "none:no DGS2MO series", "SOFR30DAYAVG": "fred:SOFR30DAYAVG",
    "USIRYY": "yoy:CPIAUCSL", "JPIRYY": "yoy:JPNCPIALLMINMEI",
    "USGDPYY": "yoy:GDPC1", "UNEMPLOY": "fred:UNEMPLOY",
    # indices/vol/fx -> Yahoo
    "SPX": "yahoo:^GSPC", "NDX": "yahoo:^NDX", "RUT": "yahoo:^RUT",
    "IXIC": "yahoo:^IXIC", "RUA": "yahoo:^RUA", "MOVE": "yahoo:^MOVE",
    "VIX3M": "yahoo:^VIX3M", "VVIX": "yahoo:^VVIX", "SKEW": "yahoo:^SKEW",
    "DXY": "yahoo:DX-Y.NYB", "NI225": "yahoo:^N225", "TOPIX": "yahoo:^TPX",
    "HSI": "yahoo:^HSI", "DAX": "yahoo:^GDAXI", "AEX": "yahoo:^AEX",
    "SX5E": "yahoo:^STOXX50E", "SXXP": "yahoo:^STOXX", "SENSEX": "yahoo:^BSESN",
    "XJO": "yahoo:^AXJO", "NZ50G": "yahoo:^NZ50", "KRX": "yahoo:^KS11",
    "MDAX": "yahoo:^MDAXI", "XAUUSD": "yahoo:GC=F",
    "000300": "yahoo:000300.SS", "000001": "yahoo:000001.SS",
    "2330": "yahoo:2330.TW", "700": "yahoo:0700.HK", "388": "yahoo:0388.HK",
    "8604": "yahoo:8604.T", "W4500": "none:index not on free feeds",
    # futures -> Yahoo REAL front-month
    "CL1!": "yahoo:CL=F", "GC2!": "yahoo:GC=F", "HG1!": "yahoo:HG=F",
    "6J2!": "yahoo:6J=F", "ZF1!": "yahoo:ZF=F", "SR32!": "yahoo:SR3=F",
    "GOLD": "yahoo:GC=F", "SILVER": "yahoo:SI=F", "COPPER": "yahoo:HG=F",
    "PLATINUM": "yahoo:PL=F", "PALLADIUM": "yahoo:PA=F",
    "GE1!": "disc:CME eurodollar futures ceased Jun-2023 (SOFR transition)",
    "GE2!": "disc:CME eurodollar futures ceased Jun-2023 (SOFR transition)",
    "MME1!": "yahoo:EEM",  # proxy: MSCI EM ETF for EM futures
    "JPLG": "none:TradingEconomics-paywalled; BOJ stat-search build queued",
    "CLTOT": "none:TradingEconomics-paywalled (Chile ToT)",
    "PETOT": "none:TradingEconomics-paywalled (Peru ToT)",
    # v2.1 — fleet-resolver rung (values from the system's OWN feeds) + certified FRED
    "BTPBUND": "fleet:data/euro-fragmentation.json:countries.IT.spread_vs_bund_bp",
    "IT10Y": "fred:IRLTLT01ITM156N", "GB10Y": "fred:IRLTLT01GBM156N",
    "EU10Y": "fred:IRLTLT01DEM156N", "GB30Y": "none:no free UK 30Y series",
    "USM1": "fred:M1SL", "USM2": "fred:M2SL", "USM0": "fred:BOGMBASE",
    "JPM3": "fred:MABMM301JPM189S", "CNIRYY": "yoy:CHNCPIALLMINMEI",
    "EUINTR": "fred:ECBDFR", "USINTR": "fred:DFF",
    "JPINTR": "fred:IRSTCB01JPM156N", "CHINTR": "fred:IRSTCB01CHM156N",
    "DEIRYY": "yoy:DEUCPIALLMINMEI", "EUIRYY": "yoy:CP0000EZ19M086NEST",
    "JPEXPYY": "fleet:data/asia-leads.json:korea_exports.yoy_pct",
    "TWMPMI": "none:S&P Global PMI licensed", "USRR": "fred:RRPONTSYD",
    "KOSPI": "yahoo:^KS11", "EWT": "yahoo:EWT", "USDX": "yahoo:DX-Y.NYB",
    "ES10Y": "fleetsum:data/euro-fragmentation.json:bund_benchmark_10y_pct:countries.ES.spread_vs_bund_bp",
    "FR10Y": "fleetsum:data/euro-fragmentation.json:bund_benchmark_10y_pct:countries.FR.spread_vs_bund_bp",
    "UNTAGGED": "meta:not a metric — the no-tag note bucket",
    "ES10Y-TVC": "fleetsum:data/euro-fragmentation.json:bund_benchmark_10y_pct:countries.ES.spread_vs_bund_bp",
    "FR10Y-TVC": "fleetsum:data/euro-fragmentation.json:bund_benchmark_10y_pct:countries.FR.spread_vs_bund_bp",
    "BDI": "none:referenced in eurodollar-plumbing code but not exported in its feed (producer todo)",
    "EUGDPYY": "none:referenced in macro-nowcast code but not exported (producer todo)",
}


def _dot(doc, path):
    cur = doc
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur if isinstance(cur, (int, float)) else None


_FLEET_CACHE = {}


def fleet_value(key, path):
    if key not in _FLEET_CACHE:
        try:
            _FLEET_CACHE[key] = json.loads(
                s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        except Exception:
            _FLEET_CACHE[key] = {}
    v = _dot(_FLEET_CACHE[key], path)
    if v is None:
        return None
    return {"value": v, "prev": None, "chg_pct": None,
            "asof": f"fleet:{key.split('/')[-1]}"}


def yahoo_quote(sym):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.request.quote(sym)}"
           f"?range=5d&interval=1d")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            res = json.loads(r.read())["chart"]["result"][0]
        closes = [c for c in res["indicators"]["quote"][0]["close"] if c is not None]
        if not closes:
            return None
        cur = closes[-1]
        prev = closes[-2] if len(closes) > 1 else None
        return {"value": round(cur, 4), "prev": round(prev, 4) if prev else None,
                "chg_pct": round((cur / prev - 1) * 100, 3) if prev else None,
                "asof": "yahoo_5d"}
    except Exception:
        return None


def fred_yoy(series_id):
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=14")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/2.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            obs = [o for o in json.loads(r.read()).get("observations", [])
                   if o.get("value") not in (None, "", ".")]
        if len(obs) < 13:
            return None
        cur, yr = float(obs[0]["value"]), float(obs[12]["value"])
        return {"value": round((cur / yr - 1) * 100, 2), "prev": None,
                "chg_pct": None, "asof": obs[0]["date"] + " YoY"}
    except Exception:
        return None


def get_brain():
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/brain.json")["Body"].read())


def build_registry(brain):
    reg = {}
    for n in brain.get("notes", []):
        txt = n.get("text") or ""
        m = re.match(r"\[TV:([A-Z0-9_:!\.\-]+)\]", txt)
        if not m:
            continue
        raw = m.group(1)
        parts = raw.split(":")
        ex = parts[0] if len(parts) > 1 else ""
        sym = parts[-1]
        r = reg.setdefault(sym, {"symbol": sym, "n_notes": 0, "exchanges": set(),
                                 "note_ids": [], "note_snippet": ""})
        r["n_notes"] += 1
        if ex:
            r["exchanges"].add(ex)
        if len(r["note_ids"]) < 4:
            r["note_ids"].append(n.get("id"))
        body = txt[len(m.group(0)):].strip()
        if len(body) > len(r["note_snippet"]):
            r["note_snippet"] = body[:400]
    # v2.0: scan ALL note texts for symbol mentions beyond the [TV:] header
    # (fuse-list prose like "FRED:TRESEGCNM052N + ECONOMICS:USFER ...")
    for n in brain.get("notes", []):
        txt = n.get("text") or ""
        for ex, sym in re.findall(r"\b(FRED|ECONOMICS|AMEX|NASDAQ|NYSE|TVC|ICEUS):"
                                  r"([A-Z0-9_\.\-]{2,22})\b", txt):
            if sym in reg:
                continue
            reg[sym] = {"symbol": sym, "n_notes": 1, "exchanges": {ex},
                        "note_ids": [n.get("id")], "note_snippet": txt[:300],
                        "origin": "brain_text"}
    for r in reg.values():
        r["exchanges"] = sorted(r["exchanges"])
        r.setdefault("origin", "tv_tag")
    return reg


def categorize(sym, exchanges):
    if sym.endswith("!") or any(c.isdigit() and "!" in sym for c in sym):
        return "futures"
    for cat, rx in CAT_RULES:
        if rx.search(sym):
            return cat
    if "ECONOMICS" in exchanges:
        return "macro"
    if sym.isdigit():
        return "equity"  # Asian tickers like 2330, 700
    if len(sym) <= 5 and sym.isalpha():
        return "equity"
    return "other"


def route(sym, exchanges, cat):
    if "FRED" in exchanges:
        return "fred"
    if sym.endswith("!"):
        return "unresolved_futures"
    if "ECONOMICS" in exchanges:
        return "unresolved_economics"
    if exchanges & EQ_EX if isinstance(exchanges, set) else set(exchanges) & EQ_EX:
        return "fmp"
    if cat in ("equity", "etf"):
        return "fmp"
    return "unresolved_tv_only"


def fred_latest(series_id):
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=3")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            obs = [o for o in json.loads(r.read()).get("observations", [])
                   if o.get("value") not in (None, "", ".")]
        if not obs:
            return None
        cur = float(obs[0]["value"])
        prev = float(obs[1]["value"]) if len(obs) > 1 else None
        return {"value": cur, "prev": prev, "asof": obs[0]["date"],
                "chg_pct": round((cur / prev - 1) * 100, 3) if prev else None}
    except Exception:
        return None


def fmp_quotes(symbols):
    out = {}
    for i in range(0, len(symbols), 40):
        chunk = symbols[i:i + 40]
        url = (f"https://financialmodelingprep.com/stable/batch-quote?"
               f"symbols={','.join(chunk)}&apikey={FMP_KEY}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                for q in json.loads(r.read()):
                    if isinstance(q, dict) and q.get("price") is not None:
                        out[q.get("symbol")] = {
                            "value": q.get("price"),
                            "prev": q.get("previousClose"),
                            "chg_pct": q.get("changePercentage"),
                            "asof": "live"}
        except Exception:
            pass
        time.sleep(0.25)
    return out


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[tv-vault] {MARKER}")
    brain = get_brain()
    reg = build_registry(brain)
    print(f"[tv-vault] registry: {len(reg)} symbols from {sum(r['n_notes'] for r in reg.values())} TV notes")

    rows = []
    fred_syms, fmp_syms = [], []
    for sym, r in reg.items():
        ex = set(r["exchanges"])
        cat = categorize(sym, ex)
        src = route(sym, ex, cat)
        row = dict(r)
        row["category"] = cat
        row["source"] = src
        rows.append(row)
        if src == "fred":
            fred_syms.append(sym)
        elif src == "fmp":
            fmp_syms.append(sym)

    # resolve FRED (sequential, light: 3 obs each)
    fred_vals = {}
    for s_ in fred_syms:
        v = fred_latest(s_)
        if v:
            fred_vals[s_] = v
    # resolve FMP in batches
    fmp_vals = fmp_quotes(fmp_syms)

    n_live = 0
    for row in rows:
        v = fred_vals.get(row["symbol"]) if row["source"] == "fred" else fmp_vals.get(row["symbol"])
        if v:
            row.update(v)
            row["status"] = "LIVE"
            n_live += 1
        else:
            row["status"] = "UNRESOLVED"
            row["value"] = None
            if row["source"] in ("fred", "fmp"):
                row["source"] = row["source"] + "_failed"

    # ── v2.0 LADDER 2: aliases -> second-chance FRED -> Yahoo ^ -> documented
    for row in rows:
        if row["status"] == "LIVE":
            continue
        sym = row["symbol"]
        al = ALIASES.get(sym)
        v = None
        if al:
            kind, _, tgt = al.partition(":")
            if kind == "fred":
                v = fred_latest(tgt)
                if v: row["source"] = f"fred_alias:{tgt}"
            elif kind == "yoy":
                v = fred_yoy(tgt)
                if v: row["source"] = f"fred_yoy:{tgt}"
            elif kind == "yahoo":
                v = yahoo_quote(tgt)
                if v: row["source"] = f"yahoo:{tgt}"
            elif kind == "fleet":
                fk, _, fp = tgt.partition(":")
                v = fleet_value(fk, fp)
                if v: row["source"] = f"fleet:{fk}"
            elif kind == "fleetsum":
                fk, base_p, add_p = tgt.split(":")
                b_ = fleet_value(fk, base_p)
                a_ = fleet_value(fk, add_p)
                if b_ and a_:
                    v = {"value": round(b_["value"] + a_["value"] / 100.0, 3),
                         "prev": None, "chg_pct": None,
                         "asof": f"fleet:{fk.split('/')[-1]} computed"}
                    row["source"] = f"fleetsum:{fk}"
            elif kind == "meta":
                row["status"] = "META"; row["resolution_note"] = tgt
                continue
            elif kind == "disc":
                row["status"] = "DISCONTINUED"; row["source"] = "cme"; row["resolution_note"] = tgt
                continue
            elif kind == "none":
                row["status"] = "NO_FREE_SOURCE"; row["resolution_note"] = tgt
                continue
        if v is None and not al and sym.isalnum() and 3 <= len(sym) <= 22 and not sym.endswith("!"):
            v = fred_latest(sym)  # second-chance: bare symbol IS a FRED id (alnum!)
            if v: row["source"] = "fred_2nd_chance"
        if v is None and not al and sym.isalpha() and len(sym) <= 6:
            v = yahoo_quote("^" + sym)
            if v: row["source"] = f"yahoo:^{sym}"
        if v is None and not al and sym.isalpha() and 2 <= len(sym) <= 5:
            v = yahoo_quote(sym)  # plain ETF/equity fallback (UVXY/SVXY/IBHY...)
            if v: row["source"] = f"yahoo:{sym}"
        if v is None and not al and sym.endswith("!"):
            v = yahoo_quote(sym.rstrip("0123456789!") + "=F")
            if v: row["source"] = f"yahoo:{sym.rstrip('0123456789!')}=F"
        if v:
            row.update(v)
            row["status"] = "LIVE"
            n_live += 1
        elif row["status"] == "UNRESOLVED":
            row["status"] = "NO_FREE_SOURCE"
            row.setdefault("resolution_note", "no free API found (TV/TradingEconomics only)")
        time.sleep(0.12)

    rows.sort(key=lambda r: (-r["n_notes"], r["symbol"]))
    by_cat = defaultdict(list)
    for r in rows:
        by_cat[r["category"]].append(r["symbol"])

    out = {
        "engine": "justhodl-tradingview",
        "version": "1.0",
        "marker": MARKER,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "brain_constitution": "registry parsed live from data/brain.json [TV:*] tags — "
                              "self-updating; every symbol carries its note ids",
        "n_symbols": len(rows),
        "n_tv_notes": sum(r["n_notes"] for r in rows),
        "n_live": n_live,
        "n_unresolved": len(rows) - n_live,
        "coverage_pct": round(n_live / max(1, len(rows)) * 100, 1),
        "by_category_counts": {k: len(v) for k, v in sorted(by_cat.items())},
        "unresolved_reason": "futures(!)/ECONOMICS:/TV-only symbols have no free API "
                             "(TradingView+TradingEconomics paywalled) — preserved with "
                             "notes, never faked; FRED/FMP failures marked *_failed",
        "symbols": rows,
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str),
                  ContentType="application/json", CacheControl="max-age=900")
    print(f"[tv-vault] DONE {out['elapsed_s']}s live={n_live}/{len(rows)}")
    return {"ok": True, "n_symbols": len(rows), "n_live": n_live,
            "coverage_pct": out["coverage_pct"]}
