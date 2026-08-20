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

VERSION = "1.1.0"
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
    "backlog_universe": {"enabled": True, "max_add": 15,
                         "min_backlog_usd": 300000000.0},
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
        "explained_ok": 0.60,
        "committed_floor": 1.5, "committed_high": 3.0, "ar_haircut": 0.85, "dilution_qoq": 0.08,
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


_BROKER_PAT = _re.compile(
    r"(SegregatedUnderFederal|PayablesToCustomers|PayablesToUsers|"
    r"SafeguardingLiabilit|SafeguardingAsset|HeldForPlatformUser|"
    r"FundsHeldForClients|DepositsFromCustomers)")


def broker_balance_sheet(facts):
    """Exchanges/brokers (HOOD/COIN-class) carry customer-adjacent
    balance sheets the treasury floor model was not built for.
    ops-4916 lesson: exact-name markers missed HOOD, whose segregated
    cash tags as CashAndSecuritiesSegregatedUnderFederalAndOther...
    and whose payables live in its OWN namespace (hood:PayablesToUsers).
    Pattern scan across every non-dei namespace; hits are cited ns:tag."""
    hits = []
    for ns, tags in (facts.get("facts") or {}).items():
        if ns == "dei":
            continue
        for tag in (tags or {}):
            if _BROKER_PAT.search(tag):
                hits.append("%s:%s" % (ns, tag))
    return sorted(set(hits))[:8]


def crypto_fv(facts):
    """Company-owned crypto fair value ONLY, resolved RECENCY-FIRST.

    ops-4916 lesson (BTBT): the parent tag CryptoAssetFairValue carried
    a stale $2.3M fact (end 2026-03-31) while the fresh Q2'26 position
    lived in the Current/Noncurrent splits ($120.1M each, end
    2026-06-30). Preferring the parent unconditionally read a ~$800M
    treasury at 0.5% crypto. Doctrine now: the freshest instant 'end'
    across parent, splits, and cross-namespace extension tags governs;
    the parent is authoritative only when it is itself fresh; fresh
    splits are summed; every superseded stale fact is cited in
    provenance, never silently dropped."""
    for bad in BLOCKED_CRYPTO_TAGS:
        assert bad not in CRYPTO_TAGS  # design invariant, not data check
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}

    def _latest(tag):
        node = gaap.get(tag)
        if not node:
            return None
        e = pick_latest((node.get("units") or {}).get("USD"))
        if not e:
            return None
        return (e["end"], float(e["val"]), e)

    parent = _latest("CryptoAssetFairValue")
    cur = _latest("CryptoAssetFairValueCurrent")
    non = _latest("CryptoAssetFairValueNoncurrent")
    xv, xp = crypto_fv_crossns(facts)

    ends = [c[0] for c in (parent, cur, non) if c]
    if xp:
        ends.append(xp["end"])
    if not ends:
        return None, None
    fresh = max(ends)
    doctrine = ("company-owned only; custody tags blocked; "
                "recency-first across parent/splits/extension tags")

    if parent and parent[0] == fresh:
        _, v, e = parent
        return v, {"tag": "CryptoAssetFairValue", "end": e["end"],
                   "form": e.get("form"), "filed": e.get("filed"),
                   "doctrine": doctrine + " (parent fresh)"}

    parts = [(t, c) for t, c in
             (("CryptoAssetFairValueCurrent", cur),
              ("CryptoAssetFairValueNoncurrent", non))
             if c and c[0] == fresh]
    if parts:
        v = sum(c[1] for _, c in parts)
        prov = {"tag": "+".join(t for t, _ in parts), "end": fresh,
                "form": parts[0][1][2].get("form"),
                "filed": parts[0][1][2].get("filed"),
                "doctrine": doctrine}
        if parent:
            prov["superseded_parent"] = {"end": parent[0],
                                         "val": parent[1],
                                         "why": "stale vs splits"}
        if len(parts) == 2 and parts[0][1][1] == parts[1][1][1]:
            prov["split_equal_note"] = (
                "Current==Noncurrent to the dollar; summed per "
                "classified-balance-sheet reading (spec-checked on "
                "BTBT: sum lands at ~1/3 of mcap, matching cost basis "
                "of $269M within a normal drawdown)")
        return v, prov

    if xp and xp["end"] == fresh:
        xp = dict(xp)
        xp["doctrine"] = xp.get("doctrine", "") + "; recency-first"
        return xv, xp
    return None, None  # unreachable given ends nonempty; honest guard


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


def verdict(dd_map, decomp, coverage, th, committed_cov=None):
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
    cc = committed_cov
    contract = cc is not None and cc >= th.get("committed_floor", 1.5)
    if not trig:
        if below:
            v, sev = "BELOW_LIQUID_FLOOR", "CRITICAL"
        elif contract:
            # v1.1: the order book is the floor. Not a dump warning --
            # a standing fact: committed revenue exceeds the whole cap.
            v, sev = "BACKLOG_FLOOR", "INFO"
        else:
            v, sev = "IN_LINE", "NONE"
        return {"verdict": v, "severity": sev, "triggered_windows": [],
                "sense_score": None, "worst_window": None,
                "committed_coverage": cc}
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
    elif contract and worst_res is not None and \
            worst_res <= th["residual_stretched"]:
        # v1.1: crypto-light names whose committed contracts already
        # exceed the market cap, dumped beyond what any asset move
        # explains. Khalid's "backlog orders/contracts" leg, promoted
        # from a reported field to a verdict.
        v = "CONTRACT_BACKED_DUMP"
        sev = "HIGH" if cc >= th.get("committed_high", 3.0) else "MEDIUM"
    elif worst_expl is not None and worst_expl >= th["explained_ok"]:
        v, sev = "ASSET_DRIVEN", "INFO"
    else:
        v, sev = "IN_LINE", "NONE"
    return {"verdict": v, "severity": sev,
            "triggered_windows": [w for w, _ in sorted(trig)],
            "worst_window": worst_w, "committed_coverage": cc,
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
    fv, fv_prov = crypto_fv(facts)  # recency-first; crossns folded in
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
    # v1.1: the contract book is a floor leg, so the verdict is taken
    # only once both the liquid stack AND the order book are known.
    vd = verdict(dd, decomp, coverage, th, committed_cov)

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
    backlog_seed = []
    bu = cfg.get("backlog_universe") or {}
    _bl_early = s3_json(cfg["backlog_join"]["key"]) if bu.get(
        "enabled") else None
    if _bl_early:
        rows = []
        for tk, node in (_bl_early.get(
                cfg["backlog_join"]["root"]) or {}).items():
            try:
                val = float((node or {}).get(
                    cfg["backlog_join"]["value_field"]) or 0)
            except (TypeError, ValueError):
                continue
            if tk in universe or val < bu.get("min_backlog_usd", 3e8):
                continue
            if t2c.get(tk):
                rows.append((val, tk))
        for val, tk in sorted(rows, reverse=True)[:bu.get("max_add",
                                                          15)]:
            universe[tk] = t2c[tk]
            backlog_seed.append(tk)
        notes.add("backlog-seeded %d name(s) >= $%.0fM committed"
                  % (len(backlog_seed),
                     bu.get("min_backlog_usd", 3e8) / 1e6))
    elif bu.get("enabled"):
        notes.add("backlog universe pass skipped: %s unreadable"
                  % cfg["backlog_join"]["key"])

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
                    "crypto never counts; committed contracts are a floor leg too; NOT_DISCLOSED stays honest",
        "universe_n": len(universe), "discovered": sorted(discovered),
        "backlog_seeded": sorted(backlog_seed),
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
        "contract_floors": sorted(t for t, x in tickers.items()
                                  if x.get("status") == "OK" and
                                  x["verdict"]["verdict"] in
                                  ("BACKLOG_FLOOR",
                                   "CONTRACT_BACKED_DUMP")),
        "fusion": {"why_html": "tickers[T].why_block",
                   "history": HIST_PREFIX + "YYYY-MM-DD.json"},
    }
    ok = g0_validate(payload)
    payload["g0_ok_tickers"] = ok
    s3_put(OUT_KEY, payload)
    s3_put(HIST_PREFIX + now.date().isoformat() + ".json", payload)
    return {"ok": ok, "universe": len(universe),
            "alerts": len(alerts), "as_of": payload["as_of"]}
