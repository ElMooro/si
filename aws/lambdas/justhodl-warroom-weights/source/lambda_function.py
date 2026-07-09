#!/usr/bin/env python3
"""justhodl-warroom-weights -- EARNED per-mechanism barometer weights.

Khalid go 2026-07-09: "should we give more weight to some indicators than
others" -- answered with EVIDENCE, not opinion. For each of the 12 warroom
mechanisms this engine builds a long-history monthly proxy composite from
the same series families the mechanism watches, then event-studies it
against the platform's 8 curated crisis windows (same CRISES list the
plumbing aggregator charts):

  HIT      -- proxy stress crossed >=70th rolling percentile inside the
              6 months BEFORE the crisis onset (it LED);
  LEAD     -- how many months early the first crossing came;
  FALSE    -- >=70 crossings in months far from any crisis window.

raw score = hit_rate * (1 - clamped false-alarm penalty) + small lead
bonus; raws are normalised to mean 1 across the LEARNED set, then
SHRUNK 50% toward the equal prior and clamped to [0.6, 1.6] -- with only
8 events, heavy shrinkage is the honest statistical choice. Mechanisms
whose inputs cannot be reconstructed deep enough (sentinel live-states,
CFTC pre-history, factor style-ratios) keep weight 1.0, status
EQUAL_PRIOR, disclosed. Output: data/warroom-weights.json, consumed by
the warroom's 'earned' barometer view. Monthly schedule -- weights are
slow-moving by design. Real data only.
"""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/warroom-weights.json"
FRED_KEY = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
            or "2f057499936072679d8843d7fce99989")
FMP = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY") or ""

CRISES = [
    ["1997-07", "1998-10", "Asian / LTCM"],
    ["2000-03", "2002-10", "Dot-com"],
    ["2007-08", "2009-06", "GFC"],
    ["2011-07", "2012-07", "Euro debt"],
    ["2015-08", "2016-02", "China / oil"],
    ["2020-02", "2020-05", "COVID"],
    ["2022-01", "2022-10", "Rate shock"],
    ["2023-03", "2023-05", "SVB / banks"],
]
LEAD_MONTHS = 6          # look-back window before onset
QUIET_BUFFER = 6         # months after window end still "excused"
CROSS = 70.0             # percentile threshold = "flashing"
ROLL = 36                # rolling percentile window, months

_LAST = [0.0]


def _get(url, timeout=25):
    for attempt in (0, 1):
        try:
            gap = time.time() - _LAST[0]
            if gap < 0.13:
                time.sleep(0.13 - gap)
            _LAST[0] = time.time()
            req = urllib.request.Request(url, headers={"User-Agent":
                                                       "justhodl-weights"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as he:
            if attempt == 0 and he.code in (429, 500, 502, 503):
                time.sleep(3)
                continue
            print("[weights] %s: %s" % (url[:80], he))
            return None
        except Exception as e:
            if attempt == 0:
                time.sleep(2)
                continue
            print("[weights] %s: %s" % (url[:80], e))
            return None


def fred_monthly(sid, start="1990-01-01"):
    """{'YYYY-MM': value} month-end sampled."""
    d = _get("https://api.stlouisfed.org/fred/series/observations"
             "?series_id=%s&api_key=%s&file_type=json&observation_start=%s"
             % (sid, FRED_KEY, start))
    out = {}
    for o in (d or {}).get("observations", []):
        v = o.get("value")
        if v in (".", "", None):
            continue
        out[o["date"][:7]] = float(v)     # last obs in month wins
    return out


def fmp_monthly(sym, start="1993-01-01"):
    d = _get("https://financialmodelingprep.com/stable/"
             "historical-price-eod/light?symbol=%s&from=%s&apikey=%s"
             % (sym, start, FMP), timeout=40)
    rows = d if isinstance(d, list) else (d or {}).get("historical") or []
    out = {}
    for r in sorted(rows, key=lambda x: x.get("date", "")):
        try:
            out[r["date"][:7]] = float(r.get("price") or r.get("close"))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception as e:
        print("[weights] s3 %s: %s" % (key, e))
        return {}


def months_axis(series_list):
    keys = set()
    for s in series_list:
        keys |= set(s.keys())
    return sorted(keys)


def roll_pctile(vals):
    """vals: list of (month, value) ascending -> {month: 0-100 pctile of
    trailing ROLL window}."""
    out = {}
    for i, (mth, v) in enumerate(vals):
        lo = max(0, i - ROLL + 1)
        win = [x for _, x in vals[lo:i + 1] if x is not None]
        if len(win) < 12 or v is None:
            continue
        out[mth] = 100.0 * sum(1 for x in win if x <= v) / len(win)
    return out


def series_stress(monthly, direction):
    vals = sorted(monthly.items())
    if direction == "fall":
        vals = [(m, -v) for m, v in vals]
    return roll_pctile(vals)


def mech_proxy(specs):
    """specs: list of {monthly: {...}, dir: rise|fall} -> mean stress per
    month across available series."""
    stresses = [series_stress(s["monthly"], s["dir"]) for s in specs]
    axis = months_axis(stresses)
    out = {}
    for m in axis:
        pts = [s[m] for s in stresses if m in s]
        if pts:
            out[m] = sum(pts) / len(pts)
    return out


def m_add(mth, k):
    y, m = int(mth[:4]), int(mth[5:7])
    m += k
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return "%04d-%02d" % (y, m)


def event_study(stress):
    if not stress:
        return None
    months = sorted(stress.keys())
    first, last = months[0], months[-1]
    hits, leads, covered = 0, [], 0
    excused = set()
    for c0, c1, _name in CRISES:
        for k in range(-LEAD_MONTHS, 1):
            excused.add(m_add(c0, k))
        cur = c0
        while cur <= m_add(c1, QUIET_BUFFER):
            excused.add(cur)
            cur = m_add(cur, 1)
    for c0, c1, name in CRISES:
        w0 = m_add(c0, -LEAD_MONTHS)
        if first > w0 or last < c0:
            continue                       # proxy doesn't span this event
        covered += 1
        cross_at = None
        cur = w0
        while cur <= c0:
            if stress.get(cur, 0) >= CROSS:
                cross_at = cur
                break
            cur = m_add(cur, 1)
        if cross_at:
            hits += 1
            y0, mo0 = int(c0[:4]), int(c0[5:7])
            y1, mo1 = int(cross_at[:4]), int(cross_at[5:7])
            leads.append((y0 - y1) * 12 + (mo0 - mo1))
    quiet = [m for m in months if m not in excused]
    fa = (sum(1 for m in quiet if stress[m] >= CROSS) / len(quiet)
          if quiet else 0.0)
    if not covered:
        return None
    hit_rate = hits / covered
    raw = hit_rate * (1 - min(fa * 3.0, 0.6)) \
        + (sum(leads) / len(leads) / 12.0 * 0.2 if leads else 0.0)
    return {"n_crises_covered": covered, "hits": hits,
            "hit_rate": round(hit_rate, 3),
            "mean_lead_months": (round(sum(leads) / len(leads), 1)
                                 if leads else None),
            "false_alarm_rate": round(fa, 3), "raw_score": round(raw, 4),
            "span": "%s -> %s" % (first, last)}


def spread(a, b):
    return {m: a[m] - b[m] for m in a if m in b}


def build_proxies():
    """mechanism -> (list of {monthly, dir}, note) or (None, why)."""
    P = {}
    t10y2 = fred_monthly("T10Y2Y", "1990-01-01")
    hy = fred_monthly("BAMLH0A0HYM2", "1996-12-01")
    claims = fred_monthly("IC4WSA", "1990-01-01")
    P["macro_grid"] = ([{"monthly": t10y2, "dir": "fall"},
                        {"monthly": hy, "dir": "rise"},
                        {"monthly": claims, "dir": "rise"}],
                       "T10Y2Y inversion + HY OAS + jobless claims")
    ted = fred_monthly("TEDRATE", "1990-01-01")
    fcp = fred_monthly("RIFSPPFAAD90NB", "1997-01-01")
    tb3 = fred_monthly("DTB3", "1990-01-01")
    dw = fred_monthly("WLCFLPCL", "2003-01-01")
    P["funding"] = ([{"monthly": ted, "dir": "rise"},
                     {"monthly": spread(fcp, tb3), "dir": "rise"},
                     {"monthly": dw, "dir": "rise"}],
                    "TED (to 2022) + fin CP-bill + discount window")
    P["vol"] = ([{"monthly": fred_monthly("VIXCLS", "1990-01-01"),
                  "dir": "rise"}], "VIX")
    dxy = fred_monthly("DTWEXBGS", "2006-01-01")
    dxy_old = fred_monthly("TWEXB", "1995-01-01")
    P["dollar"] = ([{"monthly": dxy, "dir": "rise"},
                    {"monthly": dxy_old, "dir": "rise"}],
                   "broad dollar (new + discontinued legacy)")
    ciss = s3_json("data/ciss-stress.json")
    comp_hist = None
    container = ciss.get("series") or ciss.get("indices") or ciss
    items = (container.values() if isinstance(container, dict)
             else container if isinstance(container, list) else [])
    for s in items:
        if isinstance(s, dict) and "Composite" in str(s.get("label")):
            comp_hist = {str(p[0])[:7]: float(p[1])
                         for p in (s.get("history") or [])
                         if p and p[1] is not None}
            break
    if comp_hist:
        P["ciss"] = ([{"monthly": comp_hist, "dir": "rise"}],
                     "ECB CISS composite (full history)")
    else:
        P["ciss"] = (None, "composite history not found in feed")
    spy = fmp_monthly("SPY", "1993-01-01") if FMP else {}
    if spy:
        mths = sorted(spy.keys())
        ddown, run = {}, None
        for m in mths:
            run = spy[m] if run is None else max(run, spy[m])
            ddown[m] = (run - spy[m]) / run * 100.0
        P["global_stress"] = ([{"monthly": ddown, "dir": "rise"}],
                              "SPY drawdown from running high (1993->)")
    else:
        P["global_stress"] = (None, "FMP key unavailable")
    eem = fmp_monthly("EEM", "2003-04-01") if FMP else {}
    if eem and spy:
        ratio = {m: eem[m] / spy[m] for m in eem if m in spy and spy[m]}
        P["leading_markets"] = ([{"monthly": ratio, "dir": "fall"}],
                                "EEM/SPY relative tape (2003->)")
    else:
        P["leading_markets"] = (None, "ETF history unavailable")
    ph = s3_json("data/plumbing-history.json")
    pl_specs = []
    POLAR = {"DRISCFLM": "rise", "UEMP27OV": "rise", "MCUMFN": "fall",
             "CASFRIW027SBOG": "rise", "RMFSL": "rise"}
    for ind in (ph.get("indicators") or []):
        iid = ind.get("id")
        if iid in POLAR and ind.get("history"):
            mon = {str(p[0])[:7]: float(p[1]) for p in ind["history"]
                   if p and p[1] is not None}
            if len(mon) > 60:
                pl_specs.append({"monthly": mon, "dir": POLAR[iid]})
    P["plumbing"] = ((pl_specs, "aggregator deep history: SLOOS spreads, "
                      "27wk+ unemployed, capacity (inv), foreign-bank "
                      "cash, retail MMF") if len(pl_specs) >= 3
                     else (None, "deep-history file too thin"))
    cust = fred_monthly("WMTSECL1", "2002-12-01")
    swpt = fred_monthly("SWPT", "2003-01-01")
    P["eurodollar"] = ([{"monthly": cust, "dir": "fall"},
                        {"monthly": swpt, "dir": "rise"}],
                       "foreign custody (inv) + Fed swap lines")
    P["factor_regime"] = (None, "style-ratio history rebuild queued -- "
                                "equal prior")
    P["cftc"] = (None, "COT full-history build queued -- equal prior")
    P["alerts"] = (None, "live-state sentinel has no reconstructable "
                         "history -- equal prior (structural)")
    return P


def lambda_handler(event=None, context=None):
    started = time.time()
    proxies = build_proxies()
    mech, raws = {}, {}
    for key, (specs, note) in proxies.items():
        if not specs:
            mech[key] = {"status": "EQUAL_PRIOR", "weight": 1.0,
                         "note": note}
            continue
        stress = mech_proxy(specs)
        es = event_study(stress)
        if not es or es["n_crises_covered"] < 3:
            mech[key] = {"status": "EQUAL_PRIOR", "weight": 1.0,
                         "note": note + " -- <3 crises covered"}
            continue
        es["status"] = "LEARNED"
        es["note"] = note
        mech[key] = es
        raws[key] = es["raw_score"]
    if raws:
        mean_raw = sum(raws.values()) / len(raws)
        for key, r in raws.items():
            norm = r / mean_raw if mean_raw else 1.0
            w = 0.5 * 1.0 + 0.5 * norm          # 50% shrink to equal
            mech[key]["weight"] = round(max(0.6, min(1.6, w)), 3)
            mech[key]["weight_prenorm"] = round(norm, 3)
    out = {"engine": "justhodl-warroom-weights", "schema": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "method": ("Per-mechanism monthly proxy (rolling %dm "
                      "percentile) event-studied vs %d curated crisis "
                      "windows; hit = >=%dth pct inside %dmo before "
                      "onset; raw = hit_rate*(1-false-alarm penalty)+"
                      "lead bonus; weights = 0.5*equal + 0.5*normalised "
                      "raw, clamped [0.6,1.6]. Mechanisms without deep "
                      "history keep 1.0 (EQUAL_PRIOR)."
                      % (ROLL, len(CRISES), int(CROSS), LEAD_MONTHS)),
           "crises": CRISES,
           "n_learned": sum(1 for v in mech.values()
                            if v.get("status") == "LEARNED"),
           "mechanisms": mech,
           "duration_s": round(time.time() - started, 1),
           "disclaimer": "Weights are evidence-shrunk estimates from 8 "
                         "events; not advice."}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=1).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=3600")
    print("[weights] wrote %s: %d learned / %d mechanisms"
          % (OUT_KEY, out["n_learned"], len(mech)))
    return {"ok": True, "n_learned": out["n_learned"],
            "weights": {k: v.get("weight") for k, v in mech.items()}}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=1))
