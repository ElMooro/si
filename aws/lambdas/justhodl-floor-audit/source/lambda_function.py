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

VERSION = "2.1.0"
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
    "market_sweep": {
        # Every SEC filer, screened daily off XBRL frames + one grouped
        # price tape (~11 HTTP calls for the whole market). The deep
        # forensic audit then runs only where a floor could plausibly
        # exist -- screening 6,000 names is cheap, auditing them is not.
        "enabled": True, "max_deep": 120, "prescreen_min_cov": 0.40,
        "deep_budget_s": 660,
        "min_mcap_usd": 15000000.0, "screen_publish": 400,
        "premium_flag": 2.0,
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
        "committed_floor": 1.5, "committed_high": 3.0,
        "runway_min_months": 12.0, "dilution_yoy_veto": 0.35,
        "min_adv_usd": 250000.0, "ar_haircut": 0.85, "dilution_qoq": 0.08,
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
        dv, dprov = xbrl_usd(facts, [
            "LongTermDebt",
            "LongTermDebtAndCapitalLeaseObligations",
            "DebtLongtermAndShorttermCombinedAmount",
            "DebtInstrumentCarryingAmount",
            "LineOfCreditFacilityAmountOutstanding",
            "NotesPayable", "LoansPayable"])
    stb, pstb = xbrl_usd(facts, ["ShortTermBorrowings", "CommercialPaper"])
    debt_total = (dv or 0.0) + (stb or 0.0)
    legs.append({"name": "debt", "raw": debt_total,
                 "value": round(-debt_total, 2), "haircut": 1.0,
                 "sign": -1,
                 "bind": {"long_term": dprov, "short_term": pstb,
                          "bound": bool(dprov or pstb),
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


ADV_MEMO = {}  # sym -> 20d average dollar volume (filled by poly_daily)


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
    tail = res[-20:]
    if tail:
        ADV_MEMO[sym] = sum(float(r.get("c") or 0) * float(r.get("v") or 0)
                            for r in tail) / len(tail)
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

# --------------------------------------- pure: tiers, quality, burn ----
CAP_TIERS = [("mega", 2e11), ("large", 1e10), ("mid", 2e9),
             ("small", 3e8), ("micro", 5e7), ("nano", 0.0)]


def cap_tier(mcap):
    """Blue chip to nano. Published on every row so the desk can be
    read one tier at a time -- a 300% floor on a $40M shell and on a
    $40B industrial are not the same claim."""
    for name, lo in CAP_TIERS:
        if mcap is not None and mcap >= lo:
            return name
    return "nano"


ASSET_WEIGHTS = {"cash": 1.00, "st_investments": 0.95,
                 "crypto_marked": 0.75, "lt_investments": 0.60,
                 "receivables": 0.50}


def asset_quality(floor):
    """0-100: how cash-like the floor actually is. Cash is a floor; a
    receivable is a promise and a coin stack is a floor that moves."""
    num = den = 0.0
    for lg in floor["legs"]:
        if lg["sign"] < 0 or not lg["value"]:
            continue
        num += lg["value"] * ASSET_WEIGHTS.get(lg["name"], 0.5)
        den += lg["value"]
    return None if den <= 0 else int(round(100 * num / den))


def ocf_ttm(facts):
    """Trailing-twelve-month operating cash flow: sum of the last four
    quarterly durations, else the latest annual. This is the leg that
    separates a floor from an ice cube."""
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    for tag in ("NetCashProvidedByUsedInOperatingActivities",
                "NetCashProvidedByUsedInOperatingActivities"
                "ContinuingOperations"):
        node = gaap.get(tag)
        if not node:
            continue
        rows = []
        for e in (node.get("units") or {}).get("USD") or []:
            if e.get("val") is None or not e.get("start") \
                    or not e.get("end"):
                continue
            try:
                span = (datetime.fromisoformat(e["end"])
                        - datetime.fromisoformat(e["start"])).days
            except ValueError:
                continue
            rows.append((span, e["end"], float(e["val"]), e.get("form")))
        if not rows:
            continue
        rows.sort(key=lambda r: r[1])
        q = [r for r in rows if 80 <= r[0] <= 100]
        if len(q) >= 4:
            last4 = q[-4:]
            return sum(r[2] for r in last4), {
                "tag": tag, "basis": "sum of 4 quarterly filings",
                "window": "%s..%s" % (last4[0][1], last4[-1][1])}
        a = [r for r in rows if 340 <= r[0] <= 380]
        if a:
            return a[-1][2], {"tag": tag, "basis": "latest annual",
                              "end": a[-1][1], "form": a[-1][3]}
    return None, None


def runway_months(floor, ocf):
    """Months of cash-like assets at the current burn. Returns
    (months, state); a self-funding company gets state not a number --
    an important distinction, never a missing value."""
    if ocf is None:
        return None, "unknown"
    if ocf >= 0:
        return None, "self_funding"
    liquid = sum(lg["value"] for lg in floor["legs"]
                 if lg["name"] in ("cash", "st_investments")
                 and lg["value"])
    burn_m = abs(ocf) / 12.0
    if burn_m <= 0:
        return None, "self_funding"
    return round(liquid / burn_m, 1), "burning"


def durability(rw_m, rw_state, dil_yoy, debt_total, gross):
    """0-100 with its reasons attached. A discount to a melting floor
    is not a discount -- it is a countdown."""
    flags = []
    if rw_state == "self_funding":
        sc = 90.0
    elif rw_m is None:
        sc, _ = 50.0, flags.append("cash-flow statement not bound -- "
                                   "durability unproven")
    elif rw_m >= 36:
        sc = 85.0
    elif rw_m >= 24:
        sc = 72.0
    elif rw_m >= 12:
        sc, _ = 45.0, flags.append("under 2 years of runway at the "
                                   "current burn (%.0f months)" % rw_m)
    elif rw_m >= 6:
        sc, _ = 22.0, flags.append("under 1 year of runway (%.0f "
                                   "months) -- the floor is melting"
                                   % rw_m)
    else:
        sc, _ = 5.0, flags.append("under 6 months of runway (%.0f) -- "
                                  "a funding event is likely" % rw_m)
    if dil_yoy is not None and dil_yoy >= 0.10:
        pen = 35 if dil_yoy >= 0.50 else (20 if dil_yoy >= 0.25 else 8)
        sc -= pen
        flags.append("share count up %.0f%% year-over-year -- the "
                     "floor per share is being diluted"
                     % (dil_yoy * 100))
    if gross and gross > 0:
        lev = (debt_total or 0.0) / gross
        if lev >= 0.75:
            sc -= 25
            flags.append("debt is %.0f%% of gross liquid assets"
                         % (lev * 100))
        elif lev >= 0.40:
            sc -= 10
            flags.append("debt is %.0f%% of gross liquid assets"
                         % (lev * 100))
    return int(max(0, min(100, round(sc)))), flags


def _interp(x, pts):
    if x is None:
        return None
    if x <= pts[0][0]:
        return float(pts[0][1])
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x <= x1:
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return float(pts[-1][1])


def discount_score(coverage):
    """How much floor a dollar of market cap buys."""
    return None if coverage is None else int(round(_interp(
        coverage, [(0.10, 0), (0.30, 12), (0.50, 28), (0.80, 46),
                   (1.00, 64), (1.30, 79), (1.80, 90), (3.00, 97)])))


def mispricing_score(worst_res):
    """Size of the dump that live asset moves cannot explain."""
    if worst_res is None or worst_res >= 0:
        return 0
    return int(max(0, min(100, round(-worst_res * 220))))


# ------------------------------------------ pure: the retail call ------
NO_CALL_VERDICTS = ("FUND_WRAPPER", "SUSPECT_INPUTS",
                    "BROKER_BALANCE_SHEET", "NO_FLOOR_DATA")


def recommend(ctx, th):
    """The decision layer. Vetoes fire BEFORE any score, because the
    classic way to lose money on a balance-sheet screen is to buy a
    genuine discount to a floor that is being burned or issued away.
    Returns action + conviction + plain-English why + risks + the
    condition that would flip it."""
    v = ctx["verdict"]
    cov, cc = ctx["coverage"], ctx["crypto_coverage"]
    dur, dflags = ctx["durability"], ctx["durability_flags"]
    qual = ctx["asset_quality"] or 50
    mis = mispricing_score(ctx["worst_residual"])
    disc = discount_score(cov) or 0
    prem = ctx["premium_to_nav"]
    reasons, risks, vetoes = [], list(dflags), []

    if v in NO_CALL_VERDICTS:
        return {"action": "NO_CALL", "conviction": 0,
                "composite": None, "legs": None,
                "plain": {"FUND_WRAPPER": "This is a fund or trust: its "
                          "price tracks the assets it holds by design, "
                          "so a floor comparison says nothing.",
                          "SUSPECT_INPUTS": "The filed share count or "
                          "the price bind is implausible here, so any "
                          "number this desk produced would be noise.",
                          "BROKER_BALANCE_SHEET": "A broker holds "
                          "customer assets on its balance sheet. That "
                          "is not shareholder value and this model "
                          "does not apply.",
                          "NO_FLOOR_DATA": "No usable balance-sheet "
                          "filing was found, so there is no floor to "
                          "measure against."}[v],
                "reasons": [], "risks": [], "vetoes": [v],
                "invalidation": "n/a"}

    composite = (0.30 * disc + 0.18 * qual + 0.32 * dur + 0.20 * mis)

    if cov is not None and cov >= 1.0:
        reasons.append("market cap is BELOW the net liquid assets: "
                       "%.0f cents of floor per dollar of price"
                       % (cov * 100))
    elif cov is not None and cov >= 0.5:
        reasons.append("%.0f%% of the market cap is already covered by "
                       "net liquid assets" % (cov * 100))
    if mis >= 40:
        reasons.append("%.0f points of the drawdown are not explained "
                       "by any move in the assets themselves"
                       % (-100 * (ctx["worst_residual"] or 0)))
    if ctx["committed_cov"] and ctx["committed_cov"] >= 1.0:
        reasons.append("committed contracts/backlog are %.1fx the "
                       "market cap" % ctx["committed_cov"])
    if qual >= 85:
        reasons.append("the floor is mostly cash and short-term "
                       "investments, not receivables or illiquid "
                       "holdings")
    if not ctx.get("debt_bound", True) and (cov or 0) >= 0.60:
        vetoes.append("debt_unbound")
        risks.append("no debt figure could be bound from the filings, "
                     "so this floor may be overstated -- treat the "
                     "coverage number as an upper bound")
    adv = ctx.get("adv_usd")
    thin = adv is not None and adv < th.get("min_adv_usd", 250000.0)
    if thin:
        risks.append("thin market: about $%.0fk traded per day, so a "
                     "position is hard to build or exit at these "
                     "prices" % (adv / 1000.0))
    if cc and cc >= 0.30:
        risks.append("%.0f%% of the floor case rests on crypto marked "
                     "live -- it moves every day" % (cc * 100))

    if dur < 25:
        vetoes.append("durability")
        action = "AVOID"
        conv = int(min(90, 55 + (25 - dur)))
    elif ctx["dilution_yoy"] is not None and \
            ctx["dilution_yoy"] >= th.get("dilution_yoy_veto", 0.35):
        vetoes.append("dilution")
        action, conv = "AVOID", 70
    elif prem is not None and prem >= th.get("premium_flag", 2.0) \
            and (cc or 0) >= 0.25 and (cov or 0) >= 0.35:
        # coverage floor added v2.1: below it the coin stack is a
        # rounding error on the enterprise and the "buy it directly"
        # argument is not honest -- that is an operating company that
        # happens to hold some crypto, not a wrapper.
        action = "REDUCE"
        conv = int(min(90, 40 + 20 * prem))
        reasons.append("price is %.2fx the live value of the assets "
                       "held -- you are paying a %.0f%% premium for a "
                       "stack you could buy directly"
                       % (prem, (prem - 1) * 100))
    elif composite >= 72 and (cov or 0) >= 0.95 and dur >= 55 \
            and not vetoes and not thin:
        action, conv = "BUY", int(round(composite))
    elif composite >= 58 and (cov or 0) >= 0.60 and not vetoes:
        action, conv = ("WATCH" if thin else "ACCUMULATE",
                        int(round(composite)))
    elif composite >= 42 or vetoes:
        action, conv = "WATCH", int(round(composite))
    else:
        action, conv = "PASS", int(round(100 - composite))

    plain = {
        "BUY": "The market is paying less than the company's own net "
               "liquid assets, the drawdown is not explained by those "
               "assets falling, and the balance sheet is durable "
               "enough to wait. This is the setup this desk exists to "
               "find.",
        "ACCUMULATE": "A real discount with a workable balance sheet, "
                      "but not the full set -- size it smaller than a "
                      "high-conviction position and add on weakness.",
        "WATCH": "The floor is interesting but at least one leg is "
                 "unproven. Put it on the list; do not chase it.",
        "PASS": "The price is not far enough below what the balance "
                "sheet supports to be worth the risk here.",
        "AVOID": "There is a discount, and there is a reason: the "
                 "floor itself is shrinking through burn or share "
                 "issuance. Cheap gets cheaper.",
        "REDUCE": "You are paying a premium to assets you could own "
                  "directly. If you hold this for the asset exposure, "
                  "the exposure is available cheaper.",
    }[action]

    inval = {
        "BUY": "a quarter showing the cash burn accelerating, or a "
               "share issuance above 10%",
        "ACCUMULATE": "runway falling under 12 months, or dilution "
                      "above 25% year-over-year",
        "WATCH": "the missing leg resolving -- a clean cash-flow "
                 "filing or the dump becoming asset-explained",
        "PASS": "coverage crossing 60% of market cap on a further "
                "decline",
        "AVOID": "a financing that removes the funding risk, or the "
                 "burn turning positive",
        "REDUCE": "the premium compressing back toward 1.2x",
    }[action]

    return {"action": action, "conviction": int(max(0, min(99, conv))),
            "composite": round(composite, 1),
            "legs": {"discount": disc, "asset_quality": qual,
                     "durability": dur, "mispricing": mis},
            "plain": plain, "reasons": reasons, "risks": risks,
            "vetoes": vetoes, "invalidation": inval}


# -------------------------------------- live: whole-market prescreen ---
def merge_frames(maps):
    """Newest-first merge: the most recent filing wins per company,
    older frames only fill companies the newer one has not reported
    yet. Pure, so the density logic is testable."""
    out = {}
    for m in maps:
        for cik, val in m.items():
            if cik not in out:
                out[cik] = val
    return out


def frames_map(taxonomy, tag, unit, quarters=5, dense=3000):
    """One XBRL frames call = that tag for EVERY filer that reported it
    in that quarter -- which is what makes a whole-market sweep
    affordable. TRAP (ops 4921): the CURRENT quarter's frame holds only
    the handful of companies that have filed so far (8 filers screened,
    gate caught it). Never accept the first non-empty frame; walk back
    merging newest-first until the union is dense."""
    now = datetime.now(timezone.utc)
    y, q = now.year, (now.month - 1) // 3 + 1
    maps, used = [], []
    for _ in range(quarters):
        frame = "CY%dQ%dI" % (y, q)
        try:
            js = http_json("https://data.sec.gov/api/xbrl/frames/"
                           "%s/%s/%s/%s.json" % (taxonomy, tag, unit,
                                                 frame),
                           timeout=90, retries=2)
            rows = js.get("data") or []
            if rows:
                maps.append({int(r["cik"]): float(r["val"])
                             for r in rows if r.get("val") is not None})
                used.append("%s(%d)" % (frame, len(rows)))
        except Exception:  # noqa: BLE001
            pass
        merged_n = sum(len(m) for m in maps)
        if merged_n >= dense:
            break
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    return merge_frames(maps), ",".join(used) or None


def poly_grouped(days_back=7):
    """One call = the last close for every US-listed name."""
    for i in range(days_back):
        d = (datetime.now(timezone.utc)
             - timedelta(days=i)).date().isoformat()
        try:
            js = http_json("https://api.polygon.io/v2/aggs/grouped/"
                           "locale/us/market/stocks/%s?adjusted=true"
                           "&apiKey=%s" % (d, POLY), timeout=90,
                           retries=2)
            rows = js.get("results") or []
            if len(rows) > 1000:
                return {r["T"]: r["c"] for r in rows if r.get("c")}, d
        except Exception:  # noqa: BLE001
            continue
    return {}, None


PRESCREEN_TAGS = [
    ("cash", "us-gaap", "CashAndCashEquivalentsAtCarryingValue", 1),
    ("cash_ifrs", "ifrs-full", "CashAndCashEquivalents", 1),
    ("st_inv", "us-gaap", "ShortTermInvestments", 1),
    ("lt_inv", "us-gaap", "LongTermInvestments", 1),
    ("crypto", "us-gaap", "CryptoAssetFairValue", 1),
    ("debt_nc", "us-gaap", "LongTermDebtNoncurrent", -1),
    ("debt_c", "us-gaap", "LongTermDebtCurrent", -1),
    ("st_borrow", "us-gaap", "ShortTermBorrowings", -1),
]


def market_prescreen(cfg, c2t, notes):
    """Screen the ENTIRE filer universe -- every cap tier, plus the
    foreign private issuers that file IFRS with the SEC -- on an
    approximate floor. Cheap, wide, and honestly labelled: no live
    crypto mark, no receivables, no per-leg provenance. Its only job
    is to decide who deserves the expensive forensic audit."""
    ms = cfg.get("market_sweep") or {}
    if not ms.get("enabled"):
        return [], {}
    px, px_day = poly_grouped()
    if not px:
        notes.add("market sweep skipped: grouped price tape unavailable")
        return [], {}
    shares, sh_frame = frames_map("dei",
                                  "EntityCommonStockSharesOutstanding",
                                  "shares")
    if not shares:
        shares, sh_frame = frames_map(
            "us-gaap", "CommonStockSharesOutstanding", "shares")
    if len(shares) < 800:
        notes.add("market sweep skipped: share-count frames too thin "
                  "(%d filers, frames %s)" % (len(shares), sh_frame))
        return [], {}
    legs, frames_used = {}, {"shares": sh_frame, "prices": px_day}
    for name, tax, tag, sign in PRESCREEN_TAGS:
        m, fr = frames_map(tax, tag, "USD")
        legs[name] = (m, sign)
        frames_used[name] = fr
    rpo, rpo_fr = frames_map("us-gaap",
                             "RevenueRemainingPerformanceObligation",
                             "USD")
    frames_used["rpo"] = rpo_fr

    min_mcap = ms.get("min_mcap_usd", 15e6)
    rows = []
    for cik, sh in shares.items():
        tk = c2t.get(cik)
        if not tk or sh <= 0:
            continue
        close = px.get(tk)
        if not close:
            continue
        mcap = sh * close
        if mcap < min_mcap:
            continue
        gross = 0.0
        debt = 0.0
        parts = {}
        for name, (m, sign) in legs.items():
            v = m.get(cik)
            if v is None:
                continue
            parts[name] = v
            if sign > 0:
                gross += v
            else:
                debt += v
        if not parts:
            continue
        nlav = gross - debt
        rows.append({
            "ticker": tk, "cik": cik, "mcap_usd": round(mcap, 2),
            "cap_tier": cap_tier(mcap),
            "approx_nlav_usd": round(nlav, 2),
            "approx_coverage": round(nlav / mcap, 4),
            "crypto_in_floor": round(parts.get("crypto", 0.0), 2),
            "committed_coverage": (round(rpo[cik] / mcap, 4)
                                   if cik in rpo else None),
            "legs_bound": sorted(parts),
        })
    rows.sort(key=lambda r: -(r["approx_coverage"] or 0))
    notes.add("market sweep: %d filers priced and screened (%s tape, "
              "%s share frame)" % (len(rows), px_day, sh_frame))
    return rows, frames_used


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
    dil_yoy = None
    if len(sh) >= 5 and sh[-5][1] > 0:
        dil_yoy = round(sh[-1][1] / sh[-5][1] - 1.0, 4)
    elif len(sh) >= 2 and sh[0][1] > 0 and dil is not None:
        dil_yoy = round((1 + dil) ** 4 - 1.0, 4)  # annualised QoQ
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

    # v2.0 durability + decision layer (after the verdict is final)
    ocf, ocf_prov = ocf_ttm(facts)
    rw_m, rw_state = runway_months(floor, ocf)
    dur_score, dur_flags = durability(
        rw_m, rw_state, dil_yoy, floor["debt_total"],
        floor["gross_liquid_assets"])
    qual = asset_quality(floor)
    tier = cap_tier(mcap)
    debt_leg = [lg for lg in floor["legs"] if lg["name"] == "debt"]
    debt_bound = bool(debt_leg and (debt_leg[0].get("bind") or {})
                      .get("bound"))
    adv_usd = ADV_MEMO.get(tk)
    premium = (round(mcap / floor["nlav"], 3)
               if floor["nlav"] and floor["nlav"] > 0 else None)
    rec = recommend({
        "verdict": vd["verdict"], "coverage": coverage,
        "crypto_coverage": crypto_cov, "committed_cov": committed_cov,
        "durability": dur_score, "durability_flags": dur_flags,
        "asset_quality": qual, "premium_to_nav": premium,
        "dilution_yoy": dil_yoy,
        "worst_residual": vd.get("worst_residual"),
        "debt_bound": debt_bound, "adv_usd": adv_usd,
    }, th)

    why_block = {
        "verdict": vd["verdict"], "severity": vd["severity"],
        "action": rec["action"], "conviction": rec["conviction"],
        "cap_tier": tier, "runway_months": rw_m,
        "runway_state": rw_state, "durability": dur_score,
        "asset_quality": qual, "premium_to_nav": premium,
        "debt_bound": debt_bound, "adv_usd_20d": adv_usd,
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
        "cap_tier": tier,
        "dilution_qoq": dil, "dilution_yoy": dil_yoy,
        "dilution_active": dilution_active,
        "ocf_ttm_usd": ocf, "ocf_bind": ocf_prov,
        "runway_months": rw_m, "runway_state": rw_state,
        "durability_score": dur_score, "durability_flags": dur_flags,
        "asset_quality_score": qual, "premium_to_nav": premium,
        "recommendation": rec,
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
    screen, frames_used = [], {}
    try:
        screen, frames_used = market_prescreen(cfg, c2t, notes)
    except Exception as e:  # noqa: BLE001
        notes.add("market sweep error (universe falls back to "
                  "watchlist+discovery): %s" % str(e)[:120])
    ms = cfg.get("market_sweep") or {}
    screen_seed = []
    if screen:
        seen = set(universe)
        for row in screen:
            if len(screen_seed) >= ms.get("max_deep", 120):
                break
            if row["ticker"] in seen:
                continue
            if (row["approx_coverage"] or 0) < ms.get(
                    "prescreen_min_cov", 0.40) and \
                    (row["committed_coverage"] or 0) < 1.5:
                continue
            universe[row["ticker"]] = row["cik"]
            screen_seed.append(row["ticker"])
            seen.add(row["ticker"])
        notes.add("deep tier: %d name(s) promoted from the market "
                  "screen" % len(screen_seed))

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
    # Wall-clock budget: a partial board published on time beats a
    # complete board that never lands because the Lambda ceiling hit
    # mid-sweep. Watchlist and discovery names are audited first
    # (dict order), so a truncation only ever drops the tail of the
    # market-screen promotions -- and says so.
    t_start = time.time()
    budget = (cfg.get("market_sweep") or {}).get("deep_budget_s", 660)
    truncated = []
    for tk, cik in universe.items():
        if time.time() - t_start > budget:
            truncated.append(tk)
            continue
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

    if truncated:
        notes.add("deep audit truncated at the %ds budget: %d name(s) "
                  "deferred to the next run" % (budget, len(truncated)))
    now = datetime.now(timezone.utc)
    payload = {
        "engine": "justhodl-floor-audit", "version": VERSION,
        "as_of": now.isoformat(timespec="seconds"),
        "doctrine": "a dump is only sensible if the balance sheet allows "
                    "it; residual beyond live asset repricing vs a "
                    ">=50%-covered liquid floor is SENSELESS; custody "
                    "crypto never counts; committed contracts are a floor leg "
                    "too; a floor that is being burned or diluted away "
                    "is vetoed before any discount is credited; "
                    "NOT_DISCLOSED stays honest",
        "universe_n": len(universe), "discovered": sorted(discovered),
        "backlog_seeded": sorted(backlog_seed),
        "screen_seeded": sorted(screen_seed),
        "deep_truncated": sorted(truncated),
        "screened_n": len(screen),
        "screen_frames": frames_used,
        "screen": screen[:(cfg.get("market_sweep") or {}).get(
            "screen_publish", 400)],
        "screen_cap_tiers": {t: sum(1 for r in screen
                                    if r["cap_tier"] == t)
                             for t in ("mega", "large", "mid", "small",
                                       "micro", "nano")},
        "actions": {a: sorted(t for t, x in tickers.items()
                              if x.get("status") == "OK" and
                              (x.get("recommendation") or {})
                              .get("action") == a)
                    for a in ("BUY", "ACCUMULATE", "WATCH", "PASS",
                              "AVOID", "REDUCE", "NO_CALL")},
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
            "screened": len(screen),
            "alerts": len(alerts), "as_of": payload["as_of"]}
