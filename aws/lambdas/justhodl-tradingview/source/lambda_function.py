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
MARKER = "tradingview-vault v1.0"

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
    for r in reg.values():
        r["exchanges"] = sorted(r["exchanges"])
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
