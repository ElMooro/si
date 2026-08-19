"""justhodl-floor-audit v1.0.0 -- Senseless-Drawdown / Asset-Floor Auditor.

Khalid's doctrine (2026-08-19): a huge price dump is only "sensible" if the
balance sheet allows it. If a company's LIQUID stack (cash + investments +
crypto marked to LIVE prices - debt) covers a large share of market cap --
BTBT holding ETH worth ~1/3 of its own mcap is the canonical case -- then a
narrative-driven drawdown that exceeds the repricing of those assets is
SENSELESS and must be flagged. Backlog / RPO (contracted future revenue)
extends the floor.

The honesty core: a treasury company SHOULD fall when its coin falls.
So each drawdown window is DECOMPOSED:
    asset_driven_dd = crypto_coverage x primary_asset_return(window)
    residual_dd     = observed_dd - asset_driven_dd
Only the RESIDUAL can be senseless. sense_score = 100 x explained fraction.

Real data only:
  - SEC XBRL companyfacts (balance-sheet legs, shares, RPO) with tag + end +
    form provenance on every leg. Customer-custody crypto tags are BLOCKED
    (never counted as company assets).
  - SEC frames API auto-discovers every filer reporting CryptoAssetFairValue
    (universe from the tag itself, not a hand list).
  - Polygon daily aggs for equity closes and X:{BTC,ETH,SOL}USD marks;
    Coinbase Exchange keyless fallback for crypto.
  - data/backlog-mined.json join (field-asserted; JOIN_BROKEN is surfaced,
    never a silent zero -- the spx-beaters boom lesson, ops 4817).

Output: data/floor-audit.json (alerts + per-ticker why_block fusion contract
for why.html) + data/floor-audit/history/YYYY-MM-DD.json snapshots.
G0 schema gate runs before any S3 put. Verdict ladder:
  BELOW_LIQUID_FLOOR > SENSELESS_DRAWDOWN > STRETCHED > ASSET_DRIVEN > IN_LINE
"""
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.0.2"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/floor-audit.json"
CFG_KEY = "data/floor-audit/config.json"
HIST_PREFIX = "data/floor-audit/history/"
BACKLOG_KEY = "data/backlog-mined.json"

SEC_UA = "JustHodl research ops@justhodl.ai"
POLY = os.environ.get("POLYGON_API_KEY") or ""

s3 = boto3.client("s3", region_name="us-east-1")

# Crypto ownership tags (ASU 2023-08 fair-value regime). ONLY these count.
CRYPTO_TAGS = ["CryptoAssetFairValue",
               "CryptoAssetFairValueCurrent",
               "CryptoAssetFairValueNoncurrent"]
# Customer / platform-user custody -- NEVER company assets (Coinbase-class
# filers report these; counting them would fabricate a floor).
BLOCKED_CRYPTO_TAGS = ["CryptoAssetHeldForPlatformUserFairValue",
                       "CryptoAssetHeldForPlatformUser",
                       "SafeguardingAssetPlatformUserCryptoAsset"]

DEFAULT_CONFIG = {
    "version": 1,
    "watchlist": {
        # primary_asset drives the live re-mark + decomposition leg.
        "BTBT": {"primary_asset": "ETH"}, "BMNR": {"primary_asset": "ETH"},
        "SBET": {"primary_asset": "ETH"}, "BTCS": {"primary_asset": "ETH"},
        "MSTR": {"primary_asset": "BTC"}, "MARA": {"primary_asset": "BTC"},
        "RIOT": {"primary_asset": "BTC"}, "CLSK": {"primary_asset": "BTC"},
        "HUT":  {"primary_asset": "BTC"}, "HIVE": {"primary_asset": "BTC"},
        "CIFR": {"primary_asset": "BTC"}, "GLXY": {"primary_asset": "BTC"},
        "SMLR": {"primary_asset": "BTC"},
        "DFDV": {"primary_asset": "SOL"}, "UPXI": {"primary_asset": "SOL"},
    },
    "discovery": {"enabled": True, "tag": "CryptoAssetFairValue",
                  "max_add": 25, "default_primary": "BTC"},
    "fund_blocklist": ["IBIT", "ETHA", "EZBC", "ARKB", "HODL", "FBTC",
                       "GBTC", "ETHE", "BITB", "BTCO", "BRRR", "ETHB",
                       "ETHV", "BTCW", "DEFI", "BITO"],
    "thresholds": {
        "dd_trigger": {"5": -0.15, "20": -0.25, "60": -0.35, "120": -0.45},
        "coverage_high": 0.50, "coverage_mid": 0.30,
        "coverage_min_report": 0.10,
        "residual_senseless": -0.15, "residual_stretched": -0.20,
        "explained_ok": 0.60, "ar_haircut": 0.85, "dilution_qoq": 0.08,
    },
    "backlog_join": {"key": BACKLOG_KEY, "root": "by_ticker",
                     "value_field": "backlog_usd",
                     "status_field": "status"},
}


# ---------------------------------------------------------------- http --
def http_json(url, headers=None, timeout=30, retries=3):
    hdr = {"User-Agent": SEC_UA}
    hdr.update(headers or {})
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=hdr)
            with urllib.request.urlopen(req, timeout=timeout) as h:
                raw = h.read()
                if h.headers.get("Content-Encoding") == "gzip" or \
                        raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (403, 404):
                raise
            time.sleep(1.2 * (i + 1))
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.2 * (i + 1))
    raise last


def s3_json(key):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def s3_put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


# ---------------------------------------------------- pure: xbrl binds --
def pick_latest(entries, allowed_forms=("10-Q", "10-K", "20-F", "40-F",
                                        "10-Q/A", "10-K/A", "6-K")):
    """Latest instant across filings: max end, tiebreak max filed.
    Entries repeat across filings -- dedupe on (end,val) implicitly by
    ordering. Returns the winning entry dict or None."""
    best = None
    for e in entries or []:
        if e.get("form") not in allowed_forms:
            continue
        if e.get("val") is None or not e.get("end"):
            continue
        k = (e["end"], e.get("filed") or "")
        if best is None or k > (best["end"], best.get("filed") or ""):
            best = e
    return best


def xbrl_usd(facts, tags):
    """First tag in ladder with a latest USD instant. Returns
    (value, provenance) or (None, None). Provenance is auditable:
    tag, end, form, filed."""
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    for tag in tags:
        node = gaap.get(tag)
        if not node:
            continue
        best = pick_latest((node.get("units") or {}).get("USD"))
        if best:
            return float(best["val"]), {"tag": tag, "end": best["end"],
                                        "form": best.get("form"),
                                        "filed": best.get("filed")}
    return None, None


def shares_series(facts):
    """dei EntityCommonStockSharesOutstanding (fallback gaap) -- returns
    list of (end, val) sorted asc, deduped on end (latest filed wins)."""
    for ns, tag in (("dei", "EntityCommonStockSharesOutstanding"),
                    ("us-gaap", "CommonStockSharesOutstanding")):
        node = ((facts.get("facts") or {}).get(ns) or {}).get(tag)
        if not node:
            continue
        by_end = {}
        cover_forms = ("10-Q", "10-K", "20-F", "40-F", "8-K",
                       "10-Q/A", "10-K/A", "6-K")
        for e in (node.get("units") or {}).get("shares") or []:
            if e.get("val") is None or not e.get("end"):
                continue
            if e.get("form") not in cover_forms:
                continue
            if float(e["val"]) < 1000:  # registration placeholders
                continue
            cur = by_end.get(e["end"])
            if cur is None or (e.get("filed") or "") > (cur.get("filed")
                                                        or ""):
                by_end[e["end"]] = e
        if by_end:
            return sorted(((k, float(v["val"])) for k, v in by_end.items()))
    return []


import re as _re
_CRYPTO_NAME = _re.compile(r"(CryptoAsset|DigitalAsset)")
_CRYPTO_FV = _re.compile(r"FairValue")
_CRYPTO_BAD = _re.compile(r"(PlatformUser|Customer|Safeguard|Custod|"
                          r"Payable|Liabilit|Borrow|Loaned|Collateral|"
                          r"Receivable|Restricted|Pledged)")


def crypto_fv_crossns(facts):
    """Cross-namespace scan for company-owned crypto fair value.
    BTBT-class filers tag under their entity namespace, not us-gaap.
    Deterministic: name must contain CryptoAsset|DigitalAsset AND
    FairValue, must NOT match custody/liability patterns; latest USD
    instant wins, largest value tiebreak. Bind is cited as ns:tag."""
    best = None
    for ns, tags in (facts.get("facts") or {}).items():
        if ns == "dei":
            continue
        for tag, node in (tags or {}).items():
            if not (_CRYPTO_NAME.search(tag) and
                    _CRYPTO_FV.search(tag)):
                continue
            if _CRYPTO_BAD.search(tag):
                continue
            e = pick_latest((node.get("units") or {}).get("USD"))
            if not e:
                continue
            key = (e["end"], float(e["val"]))
            if best is None or key > best[0]:
                best = (key, float(e["val"]),
                        {"tag": "%s:%s" % (ns, tag), "end": e["end"],
                         "form": e.get("form"),
                         "filed": e.get("filed"),
                         "doctrine": "cross-ns scan; custody/liability "
                                     "patterns blocked"})
    if best:
        return best[1], best[2]
    return None, None


def broker_balance_sheet(facts):
    """Exchanges/brokers (HOOD/COIN-class) carry customer-adjacent
    balance sheets the treasury floor model was not built for. Detect
    via custody/segregation markers and quarantine honestly."""
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    markers = ("CashSegregatedUnderFederalAndOtherRegulations",
               "PayablesToCustomers",
               "CryptoAssetHeldForPlatformUserFairValue",
               "SafeguardingLiabilityPlatformUserCryptoAsset",
               "SafeguardingAssetPlatformUserCryptoAsset")
    hits = [m for m in markers if m in gaap]
    return hits


def crypto_fv(facts):
    """Company-owned crypto fair value ONLY. Blocked custody tags are
    asserted absent from the read set (they may exist in the filing; we
    simply never bind them). Returns (value, prov) with prov noting the
    exclusion doctrine."""
    for bad in BLOCKED_CRYPTO_TAGS:
        assert bad not in CRYPTO_TAGS  # design invariant, not data check
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    # If both Current+Noncurrent exist and no total tag, sum them.
    total, prov = xbrl_usd(facts, ["CryptoAssetFairValue"])
    if total is not None:
        prov["doctrine"] = "company-owned only; custody tags blocked"
        return total, prov
    cur = gaap.get("CryptoAssetFairValueCurrent")
    non = gaap.get("CryptoAssetFairValueNoncurrent")
    if cur or non:
        v = 0.0
        ends = []
        for node in (cur, non):
            if not node:
                continue
            best = pick_latest((node.get("units") or {}).get("USD"))
            if best:
                v += float(best["val"])
                ends.append(best["end"])
        if ends:
            return v, {"tag": "CryptoAssetFairValueCurrent+Noncurrent",
                       "end": max(ends), "form": "10-Q/10-K",
                       "doctrine": "company-owned only; custody tags "
                                   "blocked"}
    return None, None


def floor_stack(facts, ar_haircut, crypto_mark, crypto_prov):
    """Assemble the liquid floor from XBRL legs + the LIVE crypto mark
    passed in. Pure. Returns dict with legs[], totals, nlav.
    Every leg carries provenance; absent legs are honest zeros with
    bind=None (never guessed)."""
    legs = []

    def leg(name, tags, sign=1, haircut=1.0):
        v, p = xbrl_usd(facts, tags)
        legs.append({"name": name, "raw": v,
                     "value": (None if v is None
                               else round(v * haircut * sign, 2)),
                     "haircut": haircut, "sign": sign, "bind": p})

    leg("cash", ["CashAndCashEquivalentsAtCarryingValue",
                 "CashCashEquivalentsRestrictedCashAndRestricted"
                 "CashEquivalents"])
    leg("st_investments", ["ShortTermInvestments",
                           "MarketableSecuritiesCurrent"])
    leg("lt_investments", ["LongTermInvestments",
                           "MarketableSecuritiesNoncurrent"])
    # crypto leg: live-marked value injected by caller (provenance chained)
    legs.append({"name": "crypto_marked", "raw": crypto_mark,
                 "value": (None if crypto_mark is None
                           else round(crypto_mark, 2)),
                 "haircut": 1.0, "sign": 1, "bind": crypto_prov})
    leg("receivables", ["AccountsReceivableNetCurrent",
                        "ReceivablesNetCurrent"], haircut=ar_haircut)
    # Debt: prefer split (noncurrent+current); else total. Sum distinct
    # concepts only -- never both split AND total (double count).
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    d_non, p_non = xbrl_usd(facts, ["LongTermDebtNoncurrent"])
    d_cur, p_cur = xbrl_usd(facts, ["LongTermDebtCurrent"])
    if d_non is not None or d_cur is not None:
        dv = (d_non or 0.0) + (d_cur or 0.0)
        dprov = {"tag": "LongTermDebtNoncurrent+Current",
                 "end": max([x["end"] for x in (p_non, p_cur) if x]),
                 "form": (p_non or p_cur).get("form")}
    else:
        dv, dprov = xbrl_usd(facts, ["LongTermDebt"])
    stb, pstb = xbrl_usd(facts, ["ShortTermBorrowings", "CommercialPaper"])
    debt_total = (dv or 0.0) + (stb or 0.0)
    legs.append({"name": "debt", "raw": debt_total,
                 "value": round(-debt_total, 2), "haircut": 1.0,
                 "sign": -1,
                 "bind": {"long_term": dprov, "short_term": pstb,
                          "note": "leases excluded v1.0"}})
    _ = gaap  # (kept for future preferred/lease legs)
    assets = sum(x["value"] for x in legs
                 if x["sign"] > 0 and x["value"] is not None)
    nlav = assets - debt_total
    return {"legs": legs, "gross_liquid_assets": round(assets, 2),
            "debt_total": round(debt_total, 2), "nlav": round(nlav, 2)}


# ------------------------------------------------- pure: price math ----
def drawdowns(closes, windows):
    """dd over each trailing window vs that window's rolling max."""
    out = {}
    n = len(closes)
    if n < 2:
        return {str(w): None for w in windows}
    last = closes[-1]
    for w in windows:
        seg = closes[-min(int(w) + 1, n):]
        mx = max(seg)
        out[str(w)] = round(last / mx - 1.0, 6) if mx > 0 else None
    return out


def window_return(closes, w):
    n = len(closes)
    if n < 2:
        return None
    i = max(0, n - int(w) - 1)
    base = closes[i]
    return round(closes[-1] / base - 1.0, 6) if base > 0 else None


def decompose(dd, crypto_cov, asset_ret):
    """asset_driven + residual == dd (identity). explained_frac in [0,1]
    is how much of a DOWN move the asset leg explains."""
    if dd is None:
        return None, None, None
    ad = round((crypto_cov or 0.0) * (asset_ret or 0.0), 6)
    res = round(dd - ad, 6)
    if dd < 0:
        expl = max(0.0, min(1.0, (ad / dd) if dd != 0 else 0.0))
    else:
        expl = None
    return ad, res, (None if expl is None else round(expl, 4))


def verdict(dd_map, decomp, coverage, th):
    """Worst triggered window governs. Returns verdict dict incl.
    sense_score (100 x explained fraction of the worst triggered dump)."""
    trig = []
    for w, lim in th["dd_trigger"].items():
        d = dd_map.get(str(w))
        if d is not None and d <= lim:
            trig.append((int(w), d))
    if coverage is None:
        return {"verdict": "NO_FLOOR_DATA", "severity": "NONE",
                "triggered_windows": [w for w, _ in trig],
                "sense_score": None, "worst_window": None}
    below = coverage >= 1.0
    if not trig:
        v = "BELOW_LIQUID_FLOOR" if below else "IN_LINE"
        sev = "CRITICAL" if below else "NONE"
        return {"verdict": v, "severity": sev, "triggered_windows": [],
                "sense_score": None, "worst_window": None}
    # worst residual across triggered windows
    worst_w, worst_res, worst_expl = None, 0.0, None
    for w, _ in trig:
        ad, res, expl = decomp.get(str(w), (None, None, None))
        if res is not None and res < worst_res:
            worst_w, worst_res, worst_expl = w, res, expl
    if worst_w is None:
        worst_w = trig[0][0]
        _, worst_res, worst_expl = decomp.get(str(worst_w),
                                              (None, 0.0, None))
    sense = (None if worst_expl is None
             else int(round(100 * worst_expl)))
    if below:
        v, sev = "BELOW_LIQUID_FLOOR", "CRITICAL"
    elif coverage >= th["coverage_high"] and \
            worst_res is not None and \
            worst_res <= th["residual_senseless"]:
        v, sev = "SENSELESS_DRAWDOWN", "HIGH"
    elif coverage >= th["coverage_mid"] and \
            worst_res is not None and \
            worst_res <= th["residual_stretched"]:
        v, sev = "STRETCHED", "MEDIUM"
    elif worst_expl is not None and worst_expl >= th["explained_ok"]:
        v, sev = "ASSET_DRIVEN", "INFO"
    else:
        v, sev = "IN_LINE", "NONE"
    return {"verdict": v, "severity": sev,
            "triggered_windows": [w for w, _ in sorted(trig)],
            "worst_window": worst_w,
            "worst_residual": worst_res, "sense_score": sense}


# --------------------------------------------------------- G0 gate -----
REQ_TICKER_FIELDS = ("ticker", "mcap_usd", "shares", "last_close",
                     "floor", "coverage", "drawdowns", "decomposition",
                     "verdict", "why_block")


def g0_validate(payload):
    assert payload.get("engine") == "justhodl-floor-audit"
    assert payload.get("version") == VERSION
    assert payload.get("as_of")
    tk = payload.get("tickers") or {}
    assert len(tk) > 0, "G0: zero tickers"
    ok = 0
    for t, rec in tk.items():
        if rec.get("status") != "OK":
            continue
        for f in REQ_TICKER_FIELDS:
            assert rec.get(f) is not None, "G0: %s missing %s" % (t, f)
        assert isinstance(rec["floor"].get("legs"), list) and \
            rec["floor"]["legs"], "G0: %s empty floor legs" % t
        ok += 1
    assert ok >= max(3, int(0.6 * len(tk))), \
        "G0: only %d/%d OK (<60%%)" % (ok, len(tk))
    assert isinstance(payload.get("alerts"), list)
    return ok


# ------------------------------------------------------ live fetchers --
def sec_ticker_map():
    j = http_json("https://www.sec.gov/files/company_tickers.json")
    t2c, c2t = {}, {}
    for _, row in j.items():
        tk = str(row.get("ticker") or "").upper()
        cik = int(row.get("cik_str") or 0)
        if tk and cik:
            t2c[tk] = cik
            c2t.setdefault(cik, tk)
    return t2c, c2t


def sec_companyfacts(cik):
    time.sleep(0.15)  # SEC pacing
    return http_json("https://data.sec.gov/api/xbrl/companyfacts/"
                     "CIK%010d.json" % cik, timeout=60)


def sec_frames_discover(tag, max_add):
    """Walk back <=5 quarterly instant frames; union CIKs reporting the
    company-owned crypto tag. Universe from the tag itself."""
    now = datetime.now(timezone.utc)
    y, q = now.year, (now.month - 1) // 3 + 1
    found = {}
    tried = []
    for _ in range(5):
        per = "CY%dQ%dI" % (y, q)
        tried.append(per)
        try:
            j = http_json("https://data.sec.gov/api/xbrl/frames/us-gaap/"
                          "%s/USD/%s.json" % (tag, per))
            for d in j.get("data") or []:
                cik = int(d.get("cik") or 0)
                v = d.get("val")
                if cik and v:
                    found[cik] = max(found.get(cik, 0), float(v))
        except Exception:  # noqa: BLE001
            pass
        q -= 1
        if q == 0:
            y, q = y - 1, 4
        if len(found) >= max_add * 3:
            break
        time.sleep(0.15)
    top = sorted(found.items(), key=lambda kv: -kv[1])[:max_add]
    return [c for c, _ in top], tried


def poly_daily(sym, days=430):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    j = http_json("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/"
                  "%s/%s?adjusted=true&sort=asc&limit=50000&apiKey=%s"
                  % (sym, start.isoformat(), end.isoformat(), POLY))
    res = j.get("results") or []
    dates = [datetime.fromtimestamp(r["t"] / 1000,
                                    tz=timezone.utc).date().isoformat()
             for r in res]
    closes = [float(r["c"]) for r in res]
    return dates, closes


def coinbase_daily(pair, days=430):
    """Keyless fallback: Coinbase Exchange daily candles (300/req max)."""
    end = datetime.now(timezone.utc)
    dates, closes = [], []
    cursor = end
    while (end - cursor).days < days:
        start = cursor - timedelta(days=290)
        url = ("https://api.exchange.coinbase.com/products/%s/candles"
               "?granularity=86400&start=%s&end=%s"
               % (pair, start.isoformat(), cursor.isoformat()))
        j = http_json(url, headers={"User-Agent": "justhodl-floor-audit"})
        for row in sorted(j, key=lambda r: r[0]):
            d = datetime.fromtimestamp(row[0],
                                       tz=timezone.utc).date().isoformat()
            if d not in dates:
                dates.append(d)
                closes.append(float(row[4]))
        cursor = start
        time.sleep(0.25)
    pairs = sorted(zip(dates, closes))
    return [p[0] for p in pairs], [p[1] for p in pairs]


def crypto_series(asset):
    pair = {"BTC": ("X:BTCUSD", "BTC-USD"),
            "ETH": ("X:ETHUSD", "ETH-USD"),
            "SOL": ("X:SOLUSD", "SOL-USD")}[asset]
    try:
        d, c = poly_daily(pair[0])
        if len(c) >= 200:
            return d, c, "polygon"
    except Exception:  # noqa: BLE001
        pass
    d, c = coinbase_daily(pair[1])
    return d, c, "coinbase"


def spot_at(dates, closes, iso_date):
    """Closest close on or before iso_date; None if series starts after."""
    best = None
    for d, c in zip(dates, closes):
        if d <= iso_date:
            best = c
        else:
            break
    return best


# ----------------------------------------------------------- handler ---
def audit_ticker(tk, cik, cfg, crypto, backlog, notes):
    th = cfg["thresholds"]
    meta = cfg["watchlist"].get(tk) or {}
    primary = meta.get("primary_asset") or \
        cfg["discovery"]["default_primary"]
    facts = sec_companyfacts(cik)

    # shares + dilution guard (self-contained XBRL QoQ)
    sh = shares_series(facts)
    if not sh:
        return {"status": "SKIP", "reason": "no shares series"}
    shares = sh[-1][1]
    dil = None
    if len(sh) >= 2 and sh[-2][1] > 0:
        dil = round(sh[-1][1] / sh[-2][1] - 1.0, 4)
    dilution_active = dil is not None and dil >= th["dilution_qoq"]

    # crypto: filing FV -> live mark via primary-asset ratio
    fv, fv_prov = crypto_fv(facts)
    if fv is None:
        fv, fv_prov = crypto_fv_crossns(facts)
    crypto_mark, ratio = None, None
    if fv is not None and fv_prov:
        cd, cc, csrc = crypto[primary]
        base = spot_at(cd, cc, fv_prov["end"])
        if base and base > 0:
            ratio = round(cc[-1] / base, 6)
            crypto_mark = fv * ratio
            fv_prov = dict(fv_prov)
            fv_prov.update({"primary_asset": primary,
                            "filing_fv_usd": round(fv, 2),
                            "mark_ratio_spot_over_filing": ratio,
                            "spot_source": csrc,
                            "spot_now": cc[-1], "spot_at_filing": base})
        else:
            crypto_mark = fv
            fv_prov = dict(fv_prov)
            fv_prov["note"] = "filing predates price window -- unscaled"

    floor = floor_stack(facts, th["ar_haircut"], crypto_mark, fv_prov)

    # equity prices -> mcap, drawdowns
    dts, closes = poly_daily(tk)
    if len(closes) < 30:
        return {"status": "SKIP", "reason": "thin price history "
                                            "(%d bars)" % len(closes)}
    last_close = closes[-1]
    mcap = shares * last_close
    if mcap <= 0:
        return {"status": "SKIP", "reason": "non-positive mcap"}
    coverage = round(floor["nlav"] / mcap, 4)
    crypto_cov = round((crypto_mark or 0.0) / mcap, 4)

    windows = sorted(int(w) for w in th["dd_trigger"])
    dd = drawdowns(closes, windows)
    cd, cc, _src = crypto[primary]
    decomp = {}
    for w in windows:
        aret = window_return(cc, w)
        decomp[str(w)] = decompose(dd[str(w)], crypto_cov, aret)
    vd = verdict(dd, decomp, coverage, th)

    # backlog / RPO leg (field-asserted join + direct XBRL RPO)
    rpo, rpo_prov = xbrl_usd(
        facts, ["RevenueRemainingPerformanceObligation"])
    bl_val, bl_status = None, None
    bj = cfg["backlog_join"]
    if backlog is None:
        bl_status = "JOIN_BROKEN(feed missing)"
        notes.add("backlog feed %s missing" % bj["key"])
    else:
        root = backlog.get(bj["root"])
        if not isinstance(root, dict):
            bl_status = "JOIN_BROKEN(root '%s' absent)" % bj["root"]
            notes.add(bl_status)
        else:
            row = root.get(tk)
            if row is None:
                bl_status = "NOT_COVERED"
            else:
                bl_status = row.get(bj["status_field"]) or "UNKNOWN"
                v = row.get(bj["value_field"])
                bl_val = float(v) if isinstance(v, (int, float)) else None
    # RPO and mined backlog can overlap -- report both, never sum blindly
    committed = rpo if rpo is not None else bl_val
    committed_cov = (round(committed / mcap, 4)
                     if committed is not None else None)

    # v1.0.1 quarantine ladder (first live tape, ops 4914/4915):
    # (a) FUND_WRAPPER -- an ETF/trust IS its assets; cov~1.0 with a
    #     ~all-crypto stack is a tautology, never an alert.
    # (b) SUSPECT_INPUTS -- coverage>10x or micro mcap means the shares
    #     bind or the price bind is pathological; quarantine honestly.
    in_watchlist = tk in (cfg.get("watchlist") or {})
    broker_hits = broker_balance_sheet(facts)
    is_fund = tk in set(cfg.get("fund_blocklist") or []) or \
        (not in_watchlist and coverage is not None and
         0.94 <= coverage <= 1.08 and crypto_cov >= 0.90)
    if broker_hits and not in_watchlist:
        vd = {"verdict": "BROKER_BALANCE_SHEET", "severity": "NONE",
              "triggered_windows": vd.get("triggered_windows", []),
              "worst_window": None, "sense_score": None,
              "note": "customer-custody/segregation markers %s -- "
                      "treasury floor model not applicable; "
                      "quarantined" % ",".join(broker_hits[:2])}
    elif is_fund:
        vd = {"verdict": "FUND_WRAPPER", "severity": "NONE",
              "triggered_windows": vd.get("triggered_windows", []),
              "worst_window": None, "sense_score": None,
              "note": "ETF/trust: mcap tracks the crypto stack by "
                      "construction; excluded from alerting"}
    elif not in_watchlist and coverage is not None and \
            (coverage > 10 or mcap < 3e6):
        vd = {"verdict": "SUSPECT_INPUTS", "severity": "NONE",
              "triggered_windows": vd.get("triggered_windows", []),
              "worst_window": vd.get("worst_window"),
              "sense_score": None,
              "note": "coverage %.1fx / mcap $%.1fM implausible -- "
                      "shares or price bind pathological; quarantined, "
                      "not alerted" % (coverage, mcap / 1e6)}

    if dilution_active and vd["severity"] in ("HIGH", "MEDIUM"):
        vd = dict(vd)
        vd["severity"] = "MEDIUM" if vd["severity"] == "HIGH" else "LOW"
        vd["dilution_softened"] = True

    why_block = {
        "verdict": vd["verdict"], "severity": vd["severity"],
        "sense_score": vd["sense_score"],
        "coverage": coverage, "crypto_coverage": crypto_cov,
        "nlav_usd": floor["nlav"], "mcap_usd": round(mcap, 2),
        "dd20": dd.get("20"),
        "residual20": (decomp.get("20") or (None, None, None))[1],
        "committed_rev_coverage": committed_cov,
        "dilution_qoq": dil,
        "evidence": [
            "NLAV $%.0fM vs mcap $%.0fM -> %.0f%% liquid coverage"
            % (floor["nlav"] / 1e6, mcap / 1e6, coverage * 100),
        ] + ([("crypto (%s) marked live: $%.0fM = %.0f%% of mcap"
               % (primary, crypto_mark / 1e6, crypto_cov * 100))]
             if crypto_mark else [])
          + ([("RPO/backlog $%.0fM = %.0f%% of mcap"
               % (committed / 1e6, committed_cov * 100))]
             if committed else []),
    }

    return {
        "status": "OK", "ticker": tk, "cik": cik,
        "primary_asset": primary,
        "shares": shares, "shares_asof": sh[-1][0],
        "dilution_qoq": dil, "dilution_active": dilution_active,
        "last_close": last_close, "price_asof": dts[-1],
        "mcap_usd": round(mcap, 2),
        "floor": floor, "coverage": coverage,
        "crypto_coverage": crypto_cov,
        "rpo_usd": rpo, "rpo_bind": rpo_prov,
        "backlog_usd": bl_val, "backlog_status": bl_status,
        "committed_rev_coverage": committed_cov,
        "drawdowns": dd,
        "decomposition": {w: {"asset_driven": a, "residual": r,
                              "explained_frac": e}
                          for w, (a, r, e) in decomp.items()},
        "verdict": vd, "why_block": why_block,
    }


def lambda_handler(event=None, context=None):
    event = event or {}
    cfg = s3_json(CFG_KEY)
    if not cfg:
        cfg = DEFAULT_CONFIG
        s3_put(CFG_KEY, cfg)
    th = cfg["thresholds"]
    notes = set()

    t2c, c2t = sec_ticker_map()

    # universe: watchlist + tag-discovered filers
    universe = {tk: t2c.get(tk) for tk in cfg["watchlist"]}
    discovered = []
    if cfg["discovery"]["enabled"] and not event.get("skip_discovery"):
        ciks, tried = sec_frames_discover(cfg["discovery"]["tag"],
                                          cfg["discovery"]["max_add"])
        for cik in ciks:
            tk = c2t.get(cik)
            if tk and tk not in universe:
                universe[tk] = cik
                discovered.append(tk)
        notes.add("frames tried: %s" % ",".join(tried))
    universe = {k: v for k, v in universe.items() if v}

    # crypto marks (one series per asset in play)
    assets = {(cfg["watchlist"].get(t) or {}).get("primary_asset")
              or cfg["discovery"]["default_primary"] for t in universe}
    crypto = {a: crypto_series(a) for a in sorted(assets)}

    backlog = s3_json(cfg["backlog_join"]["key"])

    tickers, alerts = {}, []
    for tk, cik in universe.items():
        try:
            rec = audit_ticker(tk, cik, cfg, crypto, backlog, notes)
        except Exception as e:  # noqa: BLE001
            rec = {"status": "ERROR", "reason": str(e)[:200]}
        rec["ticker"] = tk
        tickers[tk] = rec
        if rec.get("status") == "OK" and \
                rec["verdict"]["severity"] in ("CRITICAL", "HIGH",
                                               "MEDIUM"):
            alerts.append({"ticker": tk,
                           "severity": rec["verdict"]["severity"],
                           "verdict": rec["verdict"]["verdict"],
                           "sense_score": rec["verdict"]["sense_score"],
                           "coverage": rec["coverage"],
                           "worst_residual":
                               rec["verdict"].get("worst_residual"),
                           "headline": rec["why_block"]["evidence"][0]})
    sev_rank = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}
    alerts.sort(key=lambda a: (sev_rank.get(a["severity"], 9),
                               a.get("worst_residual") or 0))

    now = datetime.now(timezone.utc)
    payload = {
        "engine": "justhodl-floor-audit", "version": VERSION,
        "as_of": now.isoformat(timespec="seconds"),
        "doctrine": "a dump is only sensible if the balance sheet allows "
                    "it; residual beyond live asset repricing vs a "
                    ">=50%-covered liquid floor is SENSELESS; custody "
                    "crypto never counts; NOT_DISCLOSED stays honest",
        "universe_n": len(universe), "discovered": sorted(discovered),
        "crypto_sources": {a: s for a, (_, _, s) in crypto.items()},
        "thresholds": th, "notes": sorted(notes),
        "alerts": alerts, "tickers": tickers,
        "fund_wrappers": sorted(t for t, x in tickers.items()
                                if x.get("status") == "OK" and
                                x["verdict"]["verdict"] ==
                                "FUND_WRAPPER"),
        "suspect_inputs": sorted(t for t, x in tickers.items()
                                 if x.get("status") == "OK" and
                                 x["verdict"]["verdict"] ==
                                 "SUSPECT_INPUTS"),
        "broker_sheets": sorted(t for t, x in tickers.items()
                                if x.get("status") == "OK" and
                                x["verdict"]["verdict"] ==
                                "BROKER_BALANCE_SHEET"),
        "fusion": {"why_html": "tickers[T].why_block",
                   "history": HIST_PREFIX + "YYYY-MM-DD.json"},
    }
    ok = g0_validate(payload)
    payload["g0_ok_tickers"] = ok
    s3_put(OUT_KEY, payload)
    s3_put(HIST_PREFIX + now.date().isoformat() + ".json", payload)
    return {"ok": ok, "universe": len(universe),
            "alerts": len(alerts), "as_of": payload["as_of"]}
