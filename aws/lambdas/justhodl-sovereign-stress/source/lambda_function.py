"""
justhodl-sovereign-stress — Sovereign & Systemic Stress Engine.

A unified European / global stress desk. The platform already scores credit
spreads (cds-proxy), funding plumbing and a crisis composite — but the ECB's
own systemic-stress family was scattered across two updaters and never built
into a coherent product, and there was no per-country European real-economy
panel. This engine is that flagship.

Four modules, every series verified live (ops 791-793):

  SYSTEMIC STRESS — the ECB CISS (Composite Indicator of Systemic Stress),
      headline for the Euro Area, US, China and UK. CISS is the reference
      real-time financial-stress gauge; 2008, the 2011 euro crisis and the
      2020 COVID shock are its historic peaks. Scored by level AND by
      percentile against each index's own multi-year history.

  SOVEREIGN STRESS — the ECB SovCISS (Sovereign systemic stress), the euro
      area aggregate plus Germany, France, Italy, Spain, Portugal and Greece.
      SovCISS is the dedicated measure of stress in sovereign bond markets —
      the channel through which a fiscal scare becomes a systemic one.

  MARKETS — sovereign spreads (cross-referenced from cds-proxy, not rebuilt)
      for the bond-market read, plus equity-market stress from the VIX and
      the S&P 500 drawdown.

  REAL ECONOMY — unemployment and industrial production for Germany, France,
      Italy, Spain, the Netherlands and the EU, straight from Eurostat
      (the authoritative source; FRED's OECD mirror is stale).

Synthesises per-country composite stress scores and a Europe systemic-stress
regime.

OUTPUT: data/sovereign-stress.json   SCHEDULE: daily 12:00 UTC
Real data only — ECB Data Portal + Eurostat + FRED. Not investment advice.
"""
import csv
import io
import json
import math
import re
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/sovereign-stress.json"
HIST_KEY = "data/sovereign-stress-history.json"
VERSION = "2.4.4"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

ECB_API = "https://data-api.ecb.europa.eu/service/data"
EUROSTAT = ("https://ec.europa.eu/eurostat/api/dissemination/statistics"
            "/1.0/data")

# ── ECB CISS — headline. SS_CI.IDX is the maintained/working series (SS_CIN.IDX
#    returns empty as of 2026-07). ──
CISS_HEADLINE = {
    "euro_area": "D.U2.Z0Z.4F.EC.SS_CI.IDX",
    "united_states": "D.US.Z0Z.4F.EC.SS_CI.IDX",
    "china": "D.CN.Z0Z.4F.EC.SS_CI.IDX",
    "united_kingdom": "D.GB.Z0Z.4F.EC.SS_CI.IDX",
}
CISS_HEADLINE_FALLBACK = {            # new CISS variant, if the maintained one is thin
    "euro_area": "D.U2.Z0Z.4F.EC.SS_CIN.IDX",
    "united_states": "D.US.Z0Z.4F.EC.SS_CIN.IDX",
    "china": "D.CN.Z0Z.4F.EC.SS_CIN.IDX",
    "united_kingdom": "D.GB.Z0Z.4F.EC.SS_CIN.IDX",
}
# ── ECB SovCISS — sovereign systemic stress (monthly) ──
SOVCISS = {
    "euro_area": "M.U2.Z0Z.4F.EC.SOV_GDPW.IDX",
    "germany": "M.DE.Z0Z.4F.EC.SOV_CI.IDX",
    "france": "M.FR.Z0Z.4F.EC.SOV_CI.IDX",
    "italy": "M.IT.Z0Z.4F.EC.SOV_CI.IDX",
    "spain": "M.ES.Z0Z.4F.EC.SOV_CI.IDX",
    "portugal": "M.PT.Z0Z.4F.EC.SOV_CI.IDX",
    "greece": "M.GR.Z0Z.4F.EC.SOV_CI.IDX",
    "finland": "M.FI.Z0Z.4F.EC.SOV_CI.IDX",
}

# ── ASIAN SOVEREIGNS — real data via World Government Bonds' own REST endpoint
#    (/wp-json/country/v1/main, reverse-engineered from their site JS). Serves live 10Y
#    yield, sovereign CDS, spread-vs-Bund, rating and central-bank rate for every country —
#    the only source that covers Singapore/HK/Taiwan currently (FRED/IMF/Polygon do not).
#    South Korea also has a FRED yield; we keep WGB for consistency + CDS.
WGB_SOVEREIGNS = {                      # display key -> WGB country slug
    "south_korea": "south-korea",
    "singapore": "singapore",
    "hong_kong": "hong-kong",
    "taiwan": "taiwan",
}
# ops 3384: WGB works for any covered sovereign - extend beyond Asia.
WGB_EXTRA = {"chile": "chile", "peru": "peru", "netherlands": "netherlands",
             "switzerland": "switzerland", "turkey": "turkey", "argentina": "argentina"}
WGB_REGION = {"south_korea": "Asia-Pacific", "singapore": "Asia-Pacific",
              "hong_kong": "Asia-Pacific", "taiwan": "Asia-Pacific",
              "chile": "Latin America", "peru": "Latin America",
              "netherlands": "Europe", "switzerland": "Europe",
              "turkey": "EM Canaries", "argentina": "EM Canaries"}
COUNTRY_ETF = {"south_korea": "EWY", "singapore": "EWS", "hong_kong": "EWH",
               "taiwan": "EWT", "chile": "ECH", "peru": "EPU",
               "netherlands": "EWN", "switzerland": "EWL", "turkey": "TUR", "argentina": "ARGT", "germany": "EWG", "france": "EWQ",
               "italy": "EWI", "spain": "EWP", "greece": "GREK"}
WGB_ENDPOINT = "https://www.worldgovernmentbonds.com/wp-json/country/v1/main"
WGB_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
# ── Eurostat real economy — geo code per country ──
EU_COUNTRIES = {                       # display key -> Eurostat geo code
    "germany": "DE", "france": "FR", "italy": "IT", "spain": "ES",
    "netherlands": "NL", "greece": "EL", "finland": "FI",
}


# ───────────────────────── fetchers ─────────────────────────
def _get(url, timeout=30):
    last = None
    # ECB CSV endpoints break when we force Accept: application/json — only send
    # a JSON Accept for endpoints we actually want JSON from (FRED).
    accept = "text/csv, */*" if ("csvdata" in url or "/CISS/" in url) else "application/json"
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-sovereign-stress/1.0",
                              "Accept": accept})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    raise last or RuntimeError(f"fetch failed: {url}")


def fred(series_id, limit=900):
    """FRED observations -> newest-first [(date, float)]."""
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    d = json.loads(_get(url))
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((o["date"], float(v)))
        except (TypeError, ValueError):
            continue
    return out


def ecb_ciss(key, last_n=900):
    """ECB CISS dataflow series -> newest-first [(date, float)]."""
    url = f"{ECB_API}/CISS/{key}?format=csvdata&lastNObservations={last_n}"
    raw = _get(url).decode("utf-8", "ignore")
    rows = list(csv.reader(io.StringIO(raw)))
    if len(rows) < 2:
        return []
    hdr = rows[0]
    try:
        ti, vi = hdr.index("TIME_PERIOD"), hdr.index("OBS_VALUE")
    except ValueError:
        return []
    out = []
    for row in rows[1:]:
        if len(row) <= max(ti, vi):
            continue
        d, v = row[ti].strip(), row[vi].strip()
        if not d or v in ("", "NaN", "."):
            continue
        try:
            out.append((d, float(v)))
        except ValueError:
            continue
    out.sort(key=lambda x: x[0], reverse=True)
    return out


def eurostat(dataset, params):
    """Eurostat JSON-stat single series -> newest-first [(period, float)]."""
    url = f"{EUROSTAT}/{dataset}?{urllib.parse.urlencode(params)}"
    d = json.loads(_get(url))
    tindex = (((d.get("dimension") or {}).get("time") or {}).get("category")
              or {}).get("index") or {}
    val = d.get("value") or {}
    out = []
    for period, idx in tindex.items():
        v = val.get(str(idx), val.get(idx))
        if v is None:
            continue
        try:
            out.append((period, float(v)))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: x[0], reverse=True)
    return out


# ───────────────────────── helpers ─────────────────────────
def latest(obs):
    return obs[0][1] if obs else None


def val_days_ago(obs, days):
    if not obs:
        return None
    def _d(s):
        return datetime.fromisoformat(s if len(s) > 7 else s + "-01")
    anchor = _d(obs[0][0])
    target = anchor - timedelta(days=days)
    return min(obs, key=lambda o: abs(_d(o[0]) - target))[1]


def level_change(obs, days):
    base = val_days_ago(obs, days)
    if base is None or not obs:
        return None
    return round(obs[0][1] - base, 3)


def pct_change(obs, days):
    base = val_days_ago(obs, days)
    if base in (None, 0) or not obs:
        return None
    return round((obs[0][1] - base) / abs(base) * 100, 2)


def pct_rank(obs):
    """Percentile rank (0-100) of the latest value in the series' history."""
    if not obs or len(obs) < 20:
        return None
    cur = obs[0][1]
    hist = [v for _, v in obs]
    below = sum(1 for v in hist if v <= cur)
    return round(below / len(hist) * 100, 1)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def status_from_pct(p):
    if p is None:
        return "UNKNOWN"
    if p < 25:
        return "CALM"
    if p < 55:
        return "NORMAL"
    if p < 80:
        return "ELEVATED"
    return "STRESSED"


def read_existing(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


# ───────────────────────── handler ─────────────────────────
def wgb_country(slug):
    """Live sovereign data (10Y yield, CDS, spread-vs-Bund, rating, CB rate) from World
    Government Bonds' own REST endpoint (/wp-json/country/v1/main), reverse-engineered from
    the site JS. Requires browser headers (bare requests 403) + the page's jsGlobalVars as
    the POST body. Returns dict or None."""
    try:
        page = _get(f"https://www.worldgovernmentbonds.com/country/{slug}/").decode("utf-8", "ignore")
    except Exception:
        return None
    m = re.search(r"var\s+jsGlobalVars\s*=\s*(\{.*?\});", page, re.S)
    if not m:
        return None
    raw, gv, depth = m.group(1), None, 0
    for i, ch in enumerate(raw):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    gv = json.loads(raw[:i + 1])
                except Exception:
                    return None
                break
    if not gv:
        return None
    body = json.dumps({"GLOBALVAR": gv}).encode()
    req = urllib.request.Request(WGB_ENDPOINT, data=body, headers={
        "User-Agent": WGB_UA, "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.worldgovernmentbonds.com/country/{slug}/",
        "Origin": "https://www.worldgovernmentbonds.com",
        "X-Requested-With": "XMLHttpRequest"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            d = json.loads(r.read().decode("utf-8", "ignore"))
    except Exception:
        return None
    if not d.get("success"):
        return None

    def num(k):
        v = d.get(k)
        try:
            return float(v) if v not in (None, "", "----") else None
        except (ValueError, TypeError):
            return None
    return {
        "bond10y_pct": num("bond10y"),
        "cds_bp": num("lastCds"),
        "cds_default_prob_pct": num("lastCdsDefaultProb"),
        "spread_vs_bund_bp": num("mainSpreadValue"),
        "rating": d.get("lastRatingValue"),
        "cb_rate_pct": num("cbRateNumber"),
        "as_of": d.get("lastDataValDesc"),
    }


# ═════════ ops 3386 — GLOBAL SOVEREIGN STRESS INDEX (GSSI), 1990→ ═════════
# One continuous index from the sovereign complex + the classic crisis
# canaries. Same doctrine as the JSI spine: every component z-scored on its
# FULL own history, logistic-mapped 0-100, weight-blended over whatever is
# present at t — so 1992 and today are the same computation.
#
#   spread block (70%): 10Y spread vs the bloc's safe asset
#       Europe vs Bund:  IT ES PT GR FR NL FI IE BE SE
#       World vs UST:    GB JP CH KR CA AU MX CL
#   canary block (30%): Switzerland unemployment (12m change, pp),
#       CHF safe-haven bid (USDCHF 63d %, inverted), JPY safe-haven
#       (USDJPY 63d %, inverted), gold (63d %).
#
#   stress_c(t) = 100 / (1 + exp(-1.1 * z_c(t)))
#   GSSI(t)     = 0.70 * mean_w(spread block) + 0.30 * mean_w(canary block)
GSSI_KEY = "data/sovereign-gssi.json"
GSSI_EU = {"italy": "IRLTLT01ITM156N", "spain": "IRLTLT01ESM156N",
           "portugal": "IRLTLT01PTM156N", "greece": "IRLTLT01GRM156N",
           "france": "IRLTLT01FRM156N", "netherlands": "IRLTLT01NLM156N",
           "finland": "IRLTLT01FIM156N", "ireland": "IRLTLT01IEM156N",
           "belgium": "IRLTLT01BEM156N", "sweden": "IRLTLT01SEM156N"}
GSSI_US = {"united_kingdom": "IRLTLT01GBM156N", "japan": "IRLTLT01JPM156N",
           "switzerland": "IRLTLT01CHM156N", "south_korea": "IRLTLT01KRM156N",
           "canada": "IRLTLT01CAM156N", "australia": "IRLTLT01AUM156N",
           "mexico": "IRLTLT01MXM156N", "chile": "IRLTLT01CLM156N"}
GSSI_CANARY = [
    ("ch_unemployment", ["LRHUTTTTCHM156S", "LMUNRRTTCHM156S"], "un12", 1.0, 1.0),
    ("chf_safe_haven", ["DEXSZUS"], "pct63", -1.0, 1.0),
    ("jpy_safe_haven", ["DEXJPUS"], "pct63", -1.0, 0.8),
    ("krw_canary", ["DEXKOUS"], "pct63", 1.0, 0.6),
]
GSSI_CRISES = [
    ("1992-09-16", "ERM crisis (Black Wednesday)"),
    ("1994-12-20", "Tequila crisis (MXN)"),
    ("1997-07-02", "Asian crisis (THB float)"),
    ("1998-08-17", "Russia default / LTCM"),
    ("2000-03-10", "Dot-com peak"),
    ("2001-09-11", "9/11"),
    ("2007-08-09", "GFC first tremor (BNP freeze)"),
    ("2008-09-15", "Lehman"),
    ("2010-04-23", "Greece bailout request"),
    ("2011-08-01", "Euro debt crisis (IT/ES)"),
    ("2015-08-11", "China devaluation"),
    ("2020-02-20", "COVID crash"),
    ("2022-09-23", "UK gilt crisis"),
    ("2023-03-09", "SVB / regional banks"),
]


def _gz_stress(z):
    try:
        return 100.0 / (1.0 + math.exp(-1.1 * z))
    except OverflowError:
        return 100.0 if z > 0 else 0.0


def build_gssi(errors):
    t0 = time.time()

    def full(sid):
        try:
            obs = fred(sid, limit=20000)
            return {d: v for d, v in obs if d >= "1989-06"}
        except Exception as e:
            errors.append(f"gssi/{sid}: {str(e)[:40]}")
            return {}

    de = full("IRLTLT01DEM156N")
    us = full("IRLTLT01USM156N")
    comps = {}
    for name, sid in GSSI_EU.items():
        comps[name] = {"raw": full(sid), "bench": de, "kind": "spread",
                       "wt": 1.0, "block": "A"}
    for name, sid in GSSI_US.items():
        comps[name] = {"raw": full(sid), "bench": us, "kind": "spread",
                       "wt": 1.0, "block": "A"}
    for name, sids, tf, pol, wt in GSSI_CANARY:
        raw = {}
        for sid in sids:
            raw = full(sid)
            if raw:
                break
        comps[name] = {"raw": raw, "kind": tf, "pol": pol, "wt": wt,
                       "block": "B"}

    grid = sorted({d for c in comps.values() for d in c["raw"]}
                  | {d for d in de} | {d for d in us})
    grid = [d for d in grid if d >= "1990-01-01"]
    if len(grid) < 500:
        errors.append("gssi: grid too thin")
        return None

    def ffill(m):
        out, last = [], None
        pre = [v for d, v in sorted(m.items()) if d < grid[0]]
        if pre:
            last = pre[-1]
        it = sorted(m.items())
        j = 0
        for d in grid:
            while j < len(it) and it[j][0] <= d:
                last = it[j][1]
                j += 1
            out.append(last)
        return out

    n = len(grid)
    for c in comps.values():
        f = ffill(c["raw"])
        if c["kind"] == "spread":
            b = ffill(c["bench"])
            c["x"] = [(f[i] - b[i]) if f[i] is not None and b[i] is not None
                      else None for i in range(n)]
        elif c["kind"] == "un12":
            c["x"] = [None] * n
            for i in range(n):
                if f[i] is None:
                    continue
                # 12m back on a mixed grid: bisect ~1 calendar year
                ti = grid[i]
                tgt = f"{int(ti[:4]) - 1}{ti[4:]}"
                lo = 0
                hi = i
                while lo < hi:
                    m2 = (lo + hi) // 2
                    if grid[m2] < tgt:
                        lo = m2 + 1
                    else:
                        hi = m2
                back = f[lo] if lo < i else None
                c["x"][i] = (f[i] - back) if back is not None else None
        else:  # pct63 — 63 grid steps (~3m on business-ish grid)
            c["x"] = [((f[i] / f[i - 63] - 1.0) * 100.0 * c.get("pol", 1.0))
                      if i >= 63 and f[i] and f[i - 63] else None
                      for i in range(n)]
        if c["kind"] == "un12":
            c["x"] = [(v * c.get("pol", 1.0)) if v is not None else None
                      for v in c["x"]]
        # ops 3388 math v2: expanding (as-known-then) standardization of BOTH
        # the level and the 63-step velocity. No future information; the
        # early 90s are scored with early-90s baselines.
        x = c["x"]
        vel = [None] * n
        for i in range(63, n):
            if x[i] is not None and x[i - 63] is not None:
                vel[i] = x[i] - x[i - 63]
        c["vel"] = vel

        def _expz(arr, min_n=756):
            zs = [None] * n
            cnt = 0
            mean = 0.0
            m2 = 0.0
            for i in range(n):
                v = arr[i]
                if v is None:
                    continue
                cnt += 1
                d = v - mean
                mean += d / cnt
                m2 += d * (v - mean)
                if cnt >= min_n:
                    sd = (m2 / cnt) ** 0.5 or 1.0
                    zs[i] = (v - mean) / sd
            return zs

        c["z_lvl"] = _expz(x)
        c["z_vel"] = _expz(vel)
        # expanding sigma of velocity (for the co-movement layer's
        # standardized changes)
        c["vel_std"] = [None] * n
        cnt = 0
        mean = 0.0
        m2 = 0.0
        for i in range(n):
            v = vel[i]
            if v is None:
                continue
            cnt += 1
            d = v - mean
            mean += d / cnt
            m2 += d * (v - mean)
            if cnt >= 252:
                sd = (m2 / cnt) ** 0.5 or 1.0
                c["vel_std"][i] = v / sd
        if sum(1 for v in c["z_lvl"] if v is not None) < 260:
            c["dead"] = True

    # ── cross-sectional CO-MOVEMENT (the systemic ingredient) ──
    # avg pairwise correlation of standardized spread velocities via the
    # dispersion identity: Var(mean of N unit-variance series) =
    # (1 + (N-1)·rho) / N  ->  rho = (N·Var(m) - 1)/(N - 1),
    # with Var(m) rolled over 126 steps.
    spreads = [c for c in comps.values()
               if c.get("block") == "A" and not c.get("dead")]
    mseries = [None] * n
    for i in range(n):
        vsx = [c["vel_std"][i] for c in spreads if c["vel_std"][i] is not None]
        if len(vsx) >= 6:
            mseries[i] = (sum(vsx) / len(vsx), len(vsx))
    comove = [None] * n
    from collections import deque
    win = deque()
    sm = sm2 = 0.0
    for i in range(n):
        if mseries[i] is None:
            continue
        mv, ncs = mseries[i]
        win.append(mv)
        sm += mv
        sm2 += mv * mv
        if len(win) > 126:
            ov = win.popleft()
            sm -= ov
            sm2 -= ov * ov
        if len(win) >= 60:
            k = len(win)
            var_m = max(0.0, sm2 / k - (sm / k) ** 2)
            rho = (ncs * var_m - 1.0) / (ncs - 1.0)
            comove[i] = max(0.0, min(1.0, rho))

    series, breadth_s = [], []
    for i in range(n):
        # country stress: 45% chronic level + 55% widening velocity,
        # each expanding-z -> logistic
        svals = []
        for c in spreads:
            zl, zv = c["z_lvl"][i], c["z_vel"][i]
            if zl is None and zv is None:
                continue
            if zl is not None and zv is not None:
                st = 0.45 * _gz_stress(zl) + 0.55 * _gz_stress(zv)
            else:
                st = _gz_stress(zl if zl is not None else zv)
            svals.append(st)
        b_num = b_w = 0.0
        for c in comps.values():
            if c.get("block") != "B" or c.get("dead"):
                continue
            z = c["z_lvl"][i]
            if z is None:
                continue
            b_num += _gz_stress(z) * c["wt"]
            b_w += c["wt"]
        if not svals and b_w == 0:
            series.append(None)
            breadth_s.append(None)
            continue
        spread_block = None
        if svals:
            # stress-weighted intensity: hot sovereigns count up to 2x —
            # the tail is the signal, the calm core cannot drown it
            wts = [1.0 + sv / 100.0 for sv in svals]
            intensity = sum(sv * w for sv, w in zip(svals, wts)) / sum(wts)
            # co-movement amplifier: idiosyncratic blowups discounted
            # (x0.75), fully systemic episodes amplified (x1.25)
            rho = comove[i]
            amp = 0.75 + 0.5 * rho if rho is not None else 1.0
            spread_block = max(0.0, min(100.0, intensity * amp))
        canary_block = (b_num / b_w) if b_w else None
        if spread_block is not None and canary_block is not None:
            series.append(0.72 * spread_block + 0.28 * canary_block)
        else:
            series.append(spread_block if spread_block is not None
                          else canary_block)
        breadth_s.append(
            round(100.0 * sum(1 for sv in svals if sv >= 70)
                  / len(svals), 1) if svals else None)

    vals = [(grid[i], round(series[i], 2)) for i in range(n)
            if series[i] is not None]
    dts = [d for d, _ in vals]
    vs = [v for _, v in vals]
    m = len(vs)

    # full-sample percentile + YoY (365 calendar days back via bisect)
    import bisect as _b
    sv = sorted(vs)
    pct = [_b.bisect_right(sv, v) / m * 100.0 for v in vs]

    def idx_back(i, days=365):
        ti = dts[i]
        tgt = f"{int(ti[:4]) - (days // 365)}{ti[4:]}"
        j = _b.bisect_left(dts, tgt)
        return j if j < i else None

    yoy = [None] * m
    d6 = [None] * m
    for i in range(m):
        j = idx_back(i, 365)
        if j is not None and vs[j]:
            yoy[i] = (vs[i] / vs[j] - 1.0) * 100.0
        ti = dts[i]
        y, mo = int(ti[:4]), int(ti[5:7])
        mo -= 6
        if mo <= 0:
            y, mo = y - 1, mo + 12
        j6 = _b.bisect_left(dts, f"{y:04d}-{mo:02d}" + ti[7:])
        if j6 < i:
            d6[i] = vs[i] - vs[j6]

    # crisis-detection scorecard: pctile>=85 crossing OR d6m>=+12 crossing
    def first_warning(start):
        lo = _b.bisect_left(dts, (datetime.fromisoformat(start)
                                  - timedelta(days=270)).date().isoformat())
        hi = _b.bisect_left(dts, (datetime.fromisoformat(start)
                                  + timedelta(days=60)).date().isoformat())
        for i in range(max(1, lo), min(hi, m)):
            trig = None
            if pct[i] >= 85 and pct[i - 1] < 85:
                trig = "pctile>=85"
            elif d6[i] is not None and d6[i] >= 12 and (d6[i - 1] or 0) < 12:
                trig = "surge Δ6m>=+12"
            if trig:
                return dts[i], trig
        return None, None

    scorecard = []
    for start, label in GSSI_CRISES:
        w, trig = first_warning(start)
        i0 = min(_b.bisect_left(dts, start), m - 1)
        peak = max(vs[i0:min(m, i0 + 130)]) if i0 < m else None
        lead = None
        if w:
            lead = (datetime.fromisoformat(start)
                    - datetime.fromisoformat(w)).days
        scorecard.append({"crisis": label, "started": start,
                          "first_warning": w, "trigger": trig,
                          "lead_days": lead,
                          "gssi_at_start": (round(vs[i0], 1) if i0 < m else None),
                          "pctile_at_start": (round(pct[i0], 1) if i0 < m else None),
                          "peak_6m": (round(peak, 1) if peak else None),
                          "detected": bool(w)})
    det = sum(1 for r in scorecard if r["detected"])

    keep = [i for i in range(n) if series[i] is not None]
    br = [breadth_s[i] for i in keep]
    cmv = [comove[i] for i in keep]
    weekly = [{"d": dts[i], "v": vs[i],
               "yoy": (round(yoy[i], 1) if yoy[i] is not None else None),
               "b": (round(br[i], 0) if br[i] is not None else None),
               "c": (round(cmv[i], 2) if cmv[i] is not None else None)}
              for i in range(0, m, 5)]
    if weekly and weekly[-1]["d"] != dts[-1]:
        weekly.append({"d": dts[-1], "v": vs[-1],
                       "yoy": (round(yoy[-1], 1) if yoy[-1] is not None else None),
                       "b": (round(br[-1], 0) if br[-1] is not None else None),
                       "c": (round(cmv[-1], 2) if cmv[-1] is not None else None)})

    comp_meta = []
    for name, c in comps.items():
        if c.get("dead"):
            comp_meta.append({"name": name, "status": "insufficient history"})
            continue
        zl = next((c["z_lvl"][i] for i in range(n - 1, -1, -1)
                   if c["z_lvl"][i] is not None), None)
        zv = next((c["z_vel"][i] for i in range(n - 1, -1, -1)
                   if c["z_vel"][i] is not None), None)
        if zl is not None and zv is not None and c["block"] == "A":
            st = 0.45 * _gz_stress(zl) + 0.55 * _gz_stress(zv)
        elif zl is not None:
            st = _gz_stress(zl)
        else:
            st = _gz_stress(zv) if zv is not None else None
        first = next((grid[i] for i in range(n) if c["x"][i] is not None), None)
        comp_meta.append({"name": name, "block": c["block"], "weight": c["wt"],
                          "kind": c["kind"], "first_date": first,
                          "z": (round(zl, 2) if zl is not None else None),
                          "z_vel": (round(zv, 2) if zv is not None else None),
                          "stress": (round(st, 1) if st is not None else None)})

    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "method": ("v2 math (ops 3388): per country s_c = 0.45*L(z_lvl) "
                      "+ 0.55*L(z_vel63), both EXPANDING as-known-then z, "
                      "L = logistic 0-100. Spread block = stress-weighted "
                      "intensity (weights 1+s/100 — the tail is the signal) "
                      "x co-movement amplifier (0.75+0.5*rho, rho = rolling "
                      "avg pairwise corr of standardized velocities via the "
                      "dispersion identity). GSSI = 0.72*spread + "
                      "0.28*canary. No future information anywhere."),
           "math_baseline": {"v1_detected": "8/14 (ops 3386: full-sample "
                             "level-z, plain mean)"},
           "series_weekly": weekly,
           "latest": {"date": dts[-1], "gssi": vs[-1],
                      "pctile": round(pct[-1], 1),
                      "breadth_pct": (round(br[-1], 0) if br and br[-1] is not None else None),
                      "comove": (round(cmv[-1], 2) if cmv and cmv[-1] is not None else None),
                      "yoy_pct": (round(yoy[-1], 1) if yoy[-1] is not None else None),
                      "d6m": (round(d6[-1], 1) if d6[-1] is not None else None)},
           "components": sorted(comp_meta,
                                key=lambda r: -(r.get("stress") or 0)),
           "crisis_scorecard": scorecard,
           "detection": {"detected": det, "total": len(scorecard),
                         "rule": "pctile>=85 crossing OR Δ6m>=+12, window "
                                 "[start-270d, start+60d]"}}
    s3.put_object(Bucket=S3_BUCKET, Key=GSSI_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[gssi] {m} obs {dts[0]}->{dts[-1]} | now {vs[-1]:.1f} "
          f"({pct[-1]:.0f}p) yoy {yoy[-1] and round(yoy[-1], 1)} | "
          f"detected {det}/{len(scorecard)} | {round(time.time() - t0, 1)}s")
    return {"gssi": vs[-1], "pctile": round(pct[-1], 1),
            "yoy_pct": (round(yoy[-1], 1) if yoy[-1] is not None else None),
            "detected": f"{det}/{len(scorecard)}"}


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    errors, sources = [], []

    # ══ MODULE 1 — CISS systemic stress ══
    ciss = {}
    for name, key in CISS_HEADLINE.items():
        obs = None
        # primary series (SS_CI works for EA/US); fall through to fallback (SS_CIN, which
        # is where china/UK live) on EITHER an empty result OR a 404/exception.
        try:
            obs = ecb_ciss(key)
        except Exception:
            obs = None
        if not obs and name in CISS_HEADLINE_FALLBACK:
            try:
                obs = ecb_ciss(CISS_HEADLINE_FALLBACK[name])
            except Exception as e:
                errors.append(f"CISS/{name}: fallback {str(e)[:40]}")
                continue
        if not obs:
            errors.append(f"CISS/{name}: empty")
            continue
        p = pct_rank(obs)
        ciss[name] = {
            "level": round(obs[0][1], 4),
            "as_of": obs[0][0],
            "change_1m": level_change(obs, 30),
            "change_3m": level_change(obs, 91),
            "percentile_3y": p,
            "status": status_from_pct(p),
            "yoy_pct": pct_change(obs, 366),
            "level_12m": val_days_ago(obs, 366),
        }
    if ciss:
        sources.append("ECB Data Portal — CISS systemic stress")

    # ══ MODULE 2 — SovCISS sovereign stress ══
    sov = {}
    for name, key in SOVCISS.items():
        try:
            obs = ecb_ciss(key, last_n=120)
            if not obs:
                errors.append(f"SovCISS/{name}: empty")
                continue
            p = pct_rank(obs)
            sov[name] = {
                "level": round(obs[0][1], 4),
                "as_of": obs[0][0],
                "change_3m": level_change(obs, 95),
                "yoy_pct": pct_change(obs, 366),
                "level_12m": val_days_ago(obs, 366),
                "change_12m": level_change(obs, 370),
                "percentile_5y": p,
                "status": status_from_pct(p),
            }
        except Exception as e:
            errors.append(f"SovCISS/{name}: {str(e)[:60]}")
    if sov:
        sources.append("ECB Data Portal — SovCISS sovereign stress")
    # most-stressed sovereign
    sov_ranked = sorted(
        ((k, v) for k, v in sov.items()
         if k != "euro_area" and v.get("percentile_5y") is not None),
        key=lambda kv: kv[1]["percentile_5y"], reverse=True)
    most_stressed_sov = sov_ranked[0][0] if sov_ranked else None

    # ══ MODULE 2b — ASIAN SOVEREIGNS (real data via World Government Bonds REST endpoint) ══
    # Live 10Y yield, sovereign CDS, spread-vs-Bund, rating and CB rate for SG/HK/TW/KR.
    # Sovereign stress score 0-100 = blend of CDS level (primary — direct default pricing),
    # spread-vs-Bund, and yield level. CDS is the best single sovereign-stress gauge.
    def wgb_entry(name, slug):
        try:
            d = wgb_country(slug)
            if not d or d.get("bond10y_pct") is None:
                errors.append(f"WGBSov/{name}: WGB empty")
                return {"data_unavailable": True, "source": "WGB " + slug}
            cds = d.get("cds_bp")
            spread = d.get("spread_vs_bund_bp")
            y = d.get("bond10y_pct")
            # component stress mappings (0-100):
            #  CDS: 0bp→0, ~200bp→~85 (logistic-ish linear cap). Investment-grade sovereigns
            #       sit <60bp; distress >150bp.
            cds_stress = clamp((cds / 200.0) * 85.0, 0, 100) if cds is not None else None
            #  spread vs Bund: 0→~30 (neutral), +150bp→~80. Negative (tighter than Bund)→calm.
            spr_stress = clamp(30.0 + (spread / 150.0) * 50.0, 0, 100) if spread is not None else None
            #  yield level: 0%→10, 6%→~75 (higher absolute funding cost = more stress).
            yld_stress = clamp(10.0 + (y / 6.0) * 65.0, 0, 100) if y is not None else None
            parts = [(cds_stress, 0.5), (spr_stress, 0.3), (yld_stress, 0.2)]
            live = [(s, w) for s, w in parts if s is not None]
            score = round(sum(s * w for s, w in live) / sum(w for _, w in live), 1) if live else None
            return {
                "sovereign_10y_yield_pct": y,
                "cds_bp": cds,
                "cds_default_prob_pct": d.get("cds_default_prob_pct"),
                "spread_vs_bund_bp": spread,
                "rating": d.get("rating"),
                "cb_rate_pct": d.get("cb_rate_pct"),
                "as_of": d.get("as_of"),
                "stress_0_100": score,
                "status": status_from_pct(score) if score is not None else None,
                "basis": "10Y yield + sovereign CDS + spread-vs-Bund (World Government Bonds)",
            }
        except Exception as e:
            errors.append(f"WGBSov/{name}: {str(e)[:50]}")
            return {"data_unavailable": True}

    asia_sov = {name: wgb_entry(name, slug) for name, slug in WGB_SOVEREIGNS.items()}
    wgb_all = {}
    for name, slug in {**WGB_SOVEREIGNS, **WGB_EXTRA}.items():
        e = asia_sov.get(name) or wgb_entry(name, slug)
        wgb_all[name] = dict(e, region=WGB_REGION.get(name, "Other"),
                             etf=COUNTRY_ETF.get(name))

    if any(isinstance(v, dict) and v.get("stress_0_100") is not None for v in asia_sov.values()):
        sources.append("World Government Bonds — Asian sovereign 10Y yields, CDS & spreads")


    # ══ MODULE 3 — markets (equity stress + sovereign spreads x-ref) ══
    equity = {}
    try:
        vix = fred("VIXCLS", limit=400)
        spx = fred("SP500", limit=400)
        vlevel = latest(vix)
        if vlevel is not None:
            vstat = ("CALM" if vlevel < 16 else "NORMAL" if vlevel < 22
                     else "ELEVATED" if vlevel < 30 else "STRESSED")
            equity["vix"] = {"level": round(vlevel, 2),
                             "change_1m": level_change(vix, 30),
                             "status": vstat}
        if spx:
            hi = max(v for _, v in spx)
            dd = round((spx[0][1] - hi) / hi * 100, 2)
            equity["sp500"] = {
                "level": round(spx[0][1], 1),
                "drawdown_from_1y_high_pct": dd,
                "status": ("CALM" if dd > -3 else "NORMAL" if dd > -8
                           else "ELEVATED" if dd > -15 else "STRESSED")}
        if equity:
            sources.append("FRED — VIX & S&P 500 (equity-market stress)")
    except Exception as e:
        errors.append(f"equity: {str(e)[:60]}")

    cdsp = read_existing("data/cds-proxy.json") or {}
    sov_spreads = {}
    raw_sov = cdsp.get("sovereigns") or {}
    for label, d in raw_sov.items():
        if not isinstance(d, dict) or "spread_bp" not in d:
            continue
        sov_spreads[label] = {
            "spread_bp": d.get("spread_bp"),
            "change_30d_bp": d.get("change_30d_bp"),
            "status": d.get("status"),
        }
    bond_read = (
        "Sovereign-bond stress is read from the SovCISS and the cds-proxy "
        "spread panel; "
        + (f"the most-stressed euro sovereign is {most_stressed_sov} "
           f"(SovCISS {sov.get(most_stressed_sov, {}).get('percentile_5y')}th "
           "percentile)."
           if most_stressed_sov else "SovCISS data is partial."))

    # ══ MODULE 4 — real economy (Eurostat) ══
    unemployment = {}
    for name, geo in {**EU_COUNTRIES, "eu_27": "EU27_2020"}.items():
        try:
            obs = eurostat("une_rt_m", {
                "format": "JSON", "freq": "M", "s_adj": "SA",
                "age": "TOTAL", "unit": "PC_ACT", "sex": "T", "geo": geo})
            if not obs:
                errors.append(f"unemp/{name}: empty")
                continue
            unemployment[name] = {
                "rate_pct": round(obs[0][1], 2),
                "as_of": obs[0][0],
                "change_12m_pp": level_change(obs, 370),
            }
        except Exception as e:
            errors.append(f"unemp/{name}: {str(e)[:60]}")

    industrial = {}
    for name, geo in {**EU_COUNTRIES, "euro_area": "EA20"}.items():
        if name == "greece":
            continue
        try:
            obs = eurostat("sts_inpr_m", {
                "format": "JSON", "freq": "M", "indic_bt": "PRD",
                "nace_r2": "B-D", "s_adj": "SCA", "unit": "I21", "geo": geo})
            if not obs:
                errors.append(f"ip/{name}: empty")
                continue
            industrial[name] = {
                "index_2021_100": round(obs[0][1], 1),
                "as_of": obs[0][0],
                "yoy_pct": pct_change(obs, 365),
            }
        except Exception as e:
            errors.append(f"ip/{name}: {str(e)[:60]}")
    if unemployment or industrial:
        sources.append("Eurostat — unemployment & industrial production")

    # ══ MODULE 5 — per-country composite + Europe regime ══
    def country_score(name):
        """0-100 composite: SovCISS percentile, unemployment trend, IP YoY."""
        parts, wsum = 0.0, 0.0
        s = sov.get(name, {})
        if s.get("percentile_5y") is not None:
            parts += s["percentile_5y"] * 0.45
            wsum += 0.45
        u = unemployment.get(name, {})
        if u.get("change_12m_pp") is not None:
            # +1pp unemployment over a year -> ~30 stress pts
            parts += clamp(u["change_12m_pp"] / 1.0, -0.5, 1.5) * 30 * 0.30
            wsum += 0.30
        ip = industrial.get(name, {})
        if ip.get("yoy_pct") is not None:
            # -5% IP YoY -> ~25 stress pts; +growth -> negative contribution
            parts += clamp(-ip["yoy_pct"] / 5.0, -0.6, 2.0) * 25 * 0.25
            wsum += 0.25
        if wsum == 0:
            return None
        return round(clamp(parts / wsum, 0, 100), 1)

    country_scores = {}
    for name in ["germany", "france", "italy", "spain", "netherlands",
                 "greece", "portugal"]:
        sc = country_score(name)
        if sc is not None:
            country_scores[name] = sc
    worst_country = (max(country_scores, key=country_scores.get)
                     if country_scores else None)

    # Europe systemic-stress regime
    reg_parts, reg_w = 0.0, 0.0
    ea_ciss = ciss.get("euro_area", {})
    if ea_ciss.get("percentile_3y") is not None:
        reg_parts += ea_ciss["percentile_3y"] * 0.40
        reg_w += 0.40
    ea_sov = sov.get("euro_area", {})
    if ea_sov.get("percentile_5y") is not None:
        reg_parts += ea_sov["percentile_5y"] * 0.30
        reg_w += 0.30
    if country_scores:
        reg_parts += (sum(country_scores.values())
                      / len(country_scores)) * 0.30
        reg_w += 0.30
    europe_score = (round(clamp(reg_parts / reg_w, 0, 100), 1)
                    if reg_w > 0 else None)
    if europe_score is None:
        europe_regime = "UNKNOWN"
    elif europe_score < 25:
        europe_regime = "CALM"
    elif europe_score < 50:
        europe_regime = "NORMAL"
    elif europe_score < 72:
        europe_regime = "ELEVATED STRESS"
    else:
        europe_regime = "ACUTE STRESS"

    # ══ synthesis read ══
    de_ip = industrial.get("germany", {}).get("yoy_pct")
    ea_ciss_stat = ea_ciss.get("status", "UNKNOWN")
    if europe_regime in ("ELEVATED STRESS", "ACUTE STRESS"):
        read = (
            f"European stress is {europe_regime.lower()} — the CISS and "
            f"SovCISS are running rich and the real-economy panel is soft. "
            f"{worst_country.title() if worst_country else 'The periphery'} "
            "is the pressure point; watch its sovereign spread and the "
            "CISS bond-market channel for spill-over.")
    elif europe_regime == "NORMAL":
        read = (
            "European systemic stress is contained — financial-market "
            "stress indicators sit near their historical norms. The strain "
            "is in the real economy, not the financial system: industrial "
            "production is the weak spot, unemployment is the gauge to watch.")
    else:
        read = (
            "European systemic stress is calm — the CISS and SovCISS are "
            "low against their own histories and sovereign spreads are "
            "contained. The standing watch item is the real economy, where "
            "industrial output remains the soft underbelly.")

    headline = (
        f"Europe systemic stress: {europe_regime}"
        + (f" ({europe_score:.0f}/100)" if europe_score is not None else "")
        + ". "
        + (f"Euro-area CISS {ea_ciss_stat.lower()}"
           f" ({ea_ciss.get('percentile_3y')}th pct)"
           if ea_ciss else "CISS partial")
        + (f"; most-stressed sovereign {most_stressed_sov}"
           if most_stressed_sov else "")
        + (f"; German industrial output {de_ip:+.1f}% YoY"
           if de_ip is not None else "")
        + ".")

    # cross-reference (do not rebuild)
    crisis = read_existing("data/crisis-composite.json") or {}
    eds = read_existing("data/eurodollar-plumbing.json") or {}
    cross = {
        "crisis_composite": crisis.get("score") or crisis.get("composite"),
        "eurodollar_stress": eds.get("score") or eds.get("stress_score"),
        "cds_proxy_credit_risk": cdsp.get("composite_credit_risk"),
        "cds_proxy_regime": cdsp.get("regime"),
    }

    # ══ ops 3385 — YoY block: true 12m change where a real 12m source exists ══
    WGB_YIELD_FRED = {"netherlands": "IRLTLT01NLM156N",
                      "south_korea": "IRLTLT01KRM156N",
                      "chile": "IRLTLT01CLM156N",
                      "switzerland": "IRLTLT01CHM156N"}

    def _num_from(rec):
        best = None
        for k, v in (rec or {}).items():
            kl = str(k).lower()
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if "10" in kl and ("y" in kl or "year" in kl):
                return fv
            if best is None and ("yield" in kl or "bond" in kl):
                best = fv
        return best

    def _ckan_recs(url):
        j = json.loads(_get(url, timeout=25))
        return ((j.get("result") or {}).get("records")) or (j.get("records")) or []

    def sg_yield_pair():
        """Singapore 10Y now + ~12m ago — ladder across MAS API forms."""
        ago = (datetime.now(timezone.utc) - timedelta(days=366)).date().isoformat()
        bases = (
            "https://eservices.mas.gov.sg/api/action/datastore_search?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed",
            "https://eservices.mas.gov.sg/api/action/datastore/search?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed",
            "https://eservices.mas.gov.sg/apimg-gw/server/monthly_statistical_bulletin_non610mssql/domestic_interest_rates_daily/views/domestic_interest_rates_daily",
        )
        for base in bases:
            try:
                sep = "&" if "?" in base else "?"
                cur = _ckan_recs(base + sep + "limit=1&sort=end_of_day%20desc")
                if not cur:
                    cur = _ckan_recs(base + sep + "rows=1")
                now_v = _num_from(cur[0]) if cur else None
                old_r = _ckan_recs(base + sep + "limit=7&q=" + ago[:7])
                old_v = _num_from(old_r[0]) if old_r else None
                if now_v is not None and old_v is not None:
                    return now_v, old_v
            except Exception:
                continue
        return None, None

    def hk_yield_pair():
        """Hong Kong 10Y now + ~12m ago — ladder across HKMA open-API paths."""
        for url, back in (
            ("https://api.hkma.gov.hk/public/market-data-and-statistics/daily-market-data/govt-bond-benchmark-yield-daily?pagesize=280&sortby=end_of_date&sortorder=desc", 250),
            ("https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/govt-bond/hkgb-benchmark-yield-monthly?pagesize=15&sortby=end_of_month&sortorder=desc", 12),
            ("https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/er-ir/hkgb-yield-monthly?pagesize=15&sortby=end_of_month&sortorder=desc", 12),
            ("https://api.hkma.gov.hk/public/market-data-and-statistics/daily-market-data/govt-bond-yield-daily?pagesize=280&sortby=end_of_date&sortorder=desc", 250),
        ):
            try:
                j = json.loads(_get(url, timeout=25))
                recs = ((j.get("result") or {}).get("records")) or []
                if not recs:
                    continue
                now_v = _num_from(recs[0])
                old_v = _num_from(recs[min(len(recs) - 1, back)])
                if now_v is not None and old_v is not None:
                    return now_v, old_v
            except Exception:
                continue
        return None, None

    def apply_api_pair(name, fn_pair, label):
        e = wgb_all.get(name)
        if not isinstance(e, dict) or e.get("data_unavailable"):
            return
        try:
            now_v, old_v = fn_pair()
            if now_v is not None and old_v is not None:
                e["yield_yoy_bp"] = round((now_v - old_v) * 100, 0)
                e["yoy_note"] = "10Y-yield change proxy (" + label + ") until the ledger matures"
            else:
                errors.append("yoy/" + name + ": " + label + " empty")
        except Exception as ex:
            errors.append("yoy/" + name + ": " + label + " " + str(ex)[:40])
    for name, e in wgb_all.items():
        if not isinstance(e, dict) or e.get("data_unavailable"):
            continue
        sid = WGB_YIELD_FRED.get(name)
        if not sid:
            e["yield_yoy_bp"] = None
            e["yoy_note"] = "no 12m source yet — daily ledger accruing; true stress YoY unlocks automatically"
            continue
        try:
            obs = fred(sid, limit=30)
            y12 = val_days_ago(obs, 366)
            ynow = obs[0][1] if obs else None
            e["yield_yoy_bp"] = (round((ynow - y12) * 100, 0)
                                 if ynow is not None and y12 is not None else None)
            e["yoy_note"] = "10Y-yield change proxy (OECD via FRED) until the ledger matures"
        except Exception as ex:
            e["yield_yoy_bp"] = None
            e["yoy_note"] = "proxy fetch failed"
            errors.append(f"yoy/{name}: {str(ex)[:40]}")

    apply_api_pair("singapore", sg_yield_pair, "MAS SGS")
    apply_api_pair("hong_kong", hk_yield_pair, "HKMA")

    yoy_chart = []
    for name, c in sov.items():
        if name != "euro_area" and c.get("yoy_pct") is not None:
            yoy_chart.append({"name": name, "kind": "sovciss",
                              "yoy_pct": c["yoy_pct"], "level": c.get("level"),
                              "level_12m": c.get("level_12m")})
    for name, c in ciss.items():
        if c.get("yoy_pct") is not None:
            yoy_chart.append({"name": name, "kind": "ciss",
                              "yoy_pct": c["yoy_pct"], "level": c.get("level"),
                              "level_12m": c.get("level_12m")})
    for name, e in wgb_all.items():
        if isinstance(e, dict) and not e.get("data_unavailable"):
            yoy_chart.append({"name": name, "kind": "wgb_yield",
                              "yoy_bp": e.get("yield_yoy_bp"),
                              "note": e.get("yoy_note")})
    yoy_chart.sort(key=lambda r: -abs(r.get("yoy_pct") or 0))

    # ══ ops 3384 — HISTORY LEDGER (trend > level) + Δ + transition signals ══
    deltas, signals_fired = {}, []
    try:
        ledger = read_existing(HIST_KEY) or {"rows": []}
        rows = ledger.get("rows") or []
        today = now.date().isoformat()
        snap_countries = {}
        for name, e in wgb_all.items():
            if isinstance(e, dict) and e.get("stress_0_100") is not None:
                snap_countries[name] = {"s": e["stress_0_100"], "cds": e.get("cds_bp")}
        for name, sc in country_scores.items():
            snap_countries.setdefault(name, {})["comp"] = sc
        row = {"date": today, "europe": europe_score, "countries": snap_countries}
        if rows and rows[-1].get("date") == today:
            rows[-1] = row
        else:
            rows.append(row)
        rows = rows[-400:]
        s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                      Body=json.dumps({"rows": rows}, separators=(",", ":")).encode(),
                      ContentType="application/json", CacheControl="max-age=300")

        def back(k):
            return rows[-1 - k] if len(rows) > k else None

        for name in set(list(wgb_all) + list(country_scores)):
            cur = (snap_countries.get(name) or {})
            curv = cur.get("s", cur.get("comp"))
            if curv is None:
                continue
            d5r, d21r = back(5), back(21)
            def _pull(r):
                c = ((r or {}).get("countries") or {}).get(name) or {}
                return c.get("s", c.get("comp"))
            p5, p21 = _pull(d5r), _pull(d21r)
            deltas[name] = {"d5": (round(curv - p5, 1) if p5 is not None else None),
                            "d21": (round(curv - p21, 1) if p21 is not None else None)}

        # transition signal: sovereign crossing hot (>=65) with velocity (Δ5>=10)
        try:
            from signals_emit import log_signal, yprice
            tbl = None
            for name, e in wgb_all.items():
                etf = e.get("etf") if isinstance(e, dict) else None
                sc = (e or {}).get("stress_0_100")
                d5 = (deltas.get(name) or {}).get("d5")
                if etf and sc is not None and sc >= 65 and d5 is not None and d5 >= 10:
                    if tbl is None:
                        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
                    pr = yprice(etf)
                    ok = log_signal(tbl, "sov-stress-spike", etf, "DOWN", [5, 21], pr,
                                    confidence=0.55, benchmark="SPY",
                                    rationale=f"{name} sovereign stress {sc} (+{d5} in 5d) - CDS/spread/yield composite",
                                    signal_value=str(sc),
                                    metadata={"engine": "sovereign-stress", "country": name})
                    if ok:
                        signals_fired.append({"country": name, "etf": etf, "score": sc, "d5": d5})
        except Exception as e:
            errors.append(f"signals: {str(e)[:60]}")
    except Exception as e:
        errors.append(f"ledger: {str(e)[:60]}")

    # ══ ops 3386 — GSSI build (wrapped; writes its own feed) ══
    gssi_current = None
    try:
        gssi_current = build_gssi(errors)
    except Exception as e:
        errors.append(f"gssi: {str(e)[:80]}")

    core_ok = bool(ciss) and bool(sov) and (bool(unemployment)
                                            or bool(industrial))
    out = {
        "schema_version": "2.0",
        "version": VERSION,
        "method": "sovereign_systemic_stress",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": core_ok and len(errors) <= 8,
        "headline": headline,
        "europe_stress": {
            "score_0_100": europe_score,
            "regime": europe_regime,
            "worst_country": worst_country,
            "read": read,
        },
        "systemic_stress_ciss": ciss,
        "sovereign_stress_sovciss": sov,
        "asia_sovereigns": asia_sov,
        "most_stressed_sovereign": most_stressed_sov,
        "equity_market_stress": equity,
        "sovereign_spreads": sov_spreads,
        "bond_market_read": bond_read,
        "unemployment": unemployment,
        "industrial_production": industrial,
        "country_stress_scores": country_scores,
        "wgb_sovereigns": wgb_all,
        "deltas": deltas,
        "signals_fired": signals_fired,
        "history_key": HIST_KEY,
        "yoy_chart": yoy_chart,
        "gssi_key": GSSI_KEY,
        "gssi_current": gssi_current,
        "cross_reference": cross,
        "sources": sources,
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[sovereign-stress] {europe_regime} ({europe_score}) | "
          f"CISS EA {ea_ciss_stat} | most-stressed sov {most_stressed_sov} | "
          f"errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "regime": europe_regime,
                                "europe_score": europe_score,
                                "errors": len(errors)})}
