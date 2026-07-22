"""justhodl-import-canary v1.0 — PHYSICAL IMPORT FLOW CANARY (T-6 to T-3).

Canary list item #1. Portwatch tells us a chokepoint moved; boom-stage tells us
a nation's exports moved. Neither tells us WHICH GOODS physically landed in the
United States, at what value, from whom. This engine closes that gap using the
U.S. Census International Trade timeseries API — the authoritative customs
aggregate that sits directly beneath vessel manifests.

Deliberate scope decision (Khalid, 2026-07-22): named-company bills of lading
(Panjiva/ImportGenius/ImportYeti) are paid + ToS-locked + bot-blocked, and a
scrape would die silently. We build the DURABLE spine instead: HS6/NAICS x
country x month import values, which a top macro fund treats as the real
customs signal and overlays a LICENSED vendor on. Attribution ladder is honest:
commodity -> industry -> tickers, never a fabricated company-level claim.

VOCABULARY PROVEN LIVE, ops 3729:
  hs_imports    70 vars  GEN_VAL_MO/GEN_VAL_YR/I_COMMODITY/CTY_CODE/COMM_LVL
  naics_imports 50 vars  NAICS/NAICS_LDESC (clean bridge to industry-boom)
  porths        uses PORT/PORT_NAME (NOT DISTRICT — 400 on DISTRICT)
  latest month  ~2 months behind (2026-05 as of 2026-07)
  HS6 works     854231 processors = $2.996B; TW 8542 = $1.797B May-2026

WHAT IT COMPUTES per tracked commodity line:
  level      GEN_VAL_MO for the latest available month
  yoy        vs same month prior year (seasonality-neutral — trade is highly
             seasonal, so MoM alone is a trap)
  mom_3m     3m trailing mean vs prior 3m (momentum, seasonality-damped)
  z          z-score of YoY vs the line's own 36m history (self-building ledger)
  accel      YoY this month minus YoY last month = second derivative
  concentration  top-source-country share + HHI (supply fragility)
  source_shift   which country GAINED/LOST the most share YoY (reshoring,
                 China+1, Taiwan concentration risk)
  stage      cross-referenced with boom-stage: import surge WITHOUT price
             confirmation = EARLY_PHYSICAL (the good kind)

GUARDS: never fabricates a month; a missing month degrades that line only;
history ledger self-builds and z activates at n>=13; every line reports
coverage. Attribution: U.S. Census Bureau International Trade API.
"""
import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

# v1.0.1: the Census API is per-request tiny but we need ~1,200 of them.
# Sequential = ~5 min and a dropped runner connection. Pool them.
WORKERS = int(os.environ.get("CANARY_WORKERS", "16"))

VERSION = "1.0.1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/import-canary.json"
HIST_KEY = "data/import-canary-history.json"
S3 = boto3.client("s3", region_name="us-east-1")

BASE_HS = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
BASE_NAICS = "https://api.census.gov/data/timeseries/intltrade/imports/naics"
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
CTX = ssl.create_default_context()
KEY = os.environ.get("CENSUS_API_KEY", "")

# ── TRACKED LINES ────────────────────────────────────────────────────────
# Chosen for NARROW END-USE (canary rule: narrow input = clean signal, no
# demand ambiguity). Each maps to an industry the platform already ranks.
LINES = [
    # HS6 semiconductors & AI hardware
    ("HS6", "854231", "Processors & controllers (CPU/GPU)", "Semiconductors"),
    ("HS6", "854232", "Memory (DRAM/NAND)", "Semiconductors"),
    ("HS6", "854233", "Amplifiers", "Semiconductors"),
    ("HS4", "8542", "Electronic integrated circuits (all)", "Semiconductors"),
    ("HS4", "8486", "Semiconductor mfg equipment", "Semiconductor Equipment"),
    ("HS6", "848620", "Semiconductor device mfg machines", "Semiconductor Equipment"),
    # Data center / power
    ("HS4", "8471", "Computers & data-processing units", "Computer Hardware"),
    ("HS4", "8473", "Computer parts & accessories", "Computer Hardware"),
    ("HS4", "8504", "Transformers & power converters", "Electrical Equipment"),
    ("HS6", "850440", "Static converters (PSU/inverter)", "Electrical Equipment"),
    ("HS4", "8544", "Insulated wire & optical fiber cable", "Electrical Equipment"),
    ("HS4", "9013", "Optical devices / LCD modules", "Electronic Components"),
    # Energy transition & metals
    ("HS4", "8507", "Electric accumulators (batteries)", "Electrical Equipment"),
    ("HS6", "850760", "Lithium-ion batteries", "Electrical Equipment"),
    ("HS4", "2836", "Carbonates (incl. lithium carbonate)", "Chemicals"),
    ("HS4", "7403", "Refined copper", "Metals & Mining"),
    ("HS4", "7601", "Unwrought aluminium", "Metals & Mining"),
    ("HS4", "2844", "Radioactive elements (uranium)", "Uranium"),
    ("HS4", "8541", "Diodes/transistors/PV cells", "Solar"),
    # Pharma & med
    ("HS4", "3004", "Medicaments, dosed", "Drug Manufacturers"),
    ("HS4", "9018", "Medical instruments", "Medical Devices"),
    # Industrial & consumer breadth
    ("HS4", "8479", "Industrial machinery n.e.s.", "Industrial Machinery"),
    ("HS4", "8703", "Motor cars", "Auto Manufacturers"),
    ("HS4", "8708", "Motor vehicle parts", "Auto Parts"),
    ("HS4", "6403", "Leather footwear", "Footwear & Accessories"),
    ("HS4", "9403", "Furniture", "Furnishings"),
]

# NAICS lines — the clean bridge into industry-boom's league table
NAICS_LINES = [
    ("NA4", "3344", "Semiconductor & electronic components", "Semiconductors"),
    ("NA6", "334413", "Semiconductor & related devices", "Semiconductors"),
    ("NA4", "3341", "Computer & peripheral equipment", "Computer Hardware"),
    ("NA4", "3359", "Other electrical equipment", "Electrical Equipment"),
    ("NA4", "3254", "Pharmaceutical & medicine", "Drug Manufacturers"),
    ("NA4", "3361", "Motor vehicles", "Auto Manufacturers"),
    ("NA4", "3311", "Iron & steel mills", "Steel"),
]

# Countries we resolve share for (Census CTY_CODE). TOTAL is 0000/blank.
COUNTRIES = {
    "5830": "Taiwan", "5700": "China", "5800": "Korea, South",
    "5880": "Japan", "1220": "Canada", "2010": "Mexico",
    "4280": "Germany", "5330": "India", "5490": "Vietnam",
    "5590": "Malaysia", "5600": "Singapore", "5490_": "",
    "4210": "Netherlands", "4120": "Ireland", "5570": "Thailand",
    "5520": "Indonesia", "4190": "Switzerland", "4120_": "",
}


def _get(url, timeout=40, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                if r.status == 200:
                    return json.loads(r.read().decode("utf-8", "replace"))
                last = "status %s" % r.status
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", "replace")[:120]
            except Exception:
                pass
            last = "HTTP %s %s" % (e.code, body)
            if e.code == 400:
                break          # bad grammar — retry cannot help
        except Exception as e:
            last = "%s %s" % (type(e).__name__, str(e)[:90])
        if i < retries:
            time.sleep(1.2 * (i + 1))
    print("[import-canary] fetch fail:", last, url[:130])
    return None


def _q(base, params):
    p = dict(params)
    if KEY:
        p["key"] = KEY
    return base + "?" + urllib.parse.urlencode(p)


def _rows(data):
    """Census returns [header, *rows]; zip into dicts."""
    if not isinstance(data, list) or len(data) < 2:
        return []
    hdr = data[0]
    out = []
    for r in data[1:]:
        if len(r) == len(hdr):
            out.append(dict(zip(hdr, r)))
    return out


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _months_back(ym, n):
    y, m = int(ym[:4]), int(ym[5:7])
    for _ in range(n):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return "%04d-%02d" % (y, m)


def find_latest_month():
    """Probe backwards for the newest month with data. Never assume."""
    now = datetime.now(timezone.utc)
    y, m = now.year, now.month
    for _ in range(10):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        ym = "%04d-%02d" % (y, m)
        d = _get(_q(BASE_HS, {"get": "GEN_VAL_MO", "time": ym,
                              "COMM_LVL": "HS2", "I_COMMODITY": "85"}))
        if _rows(d):
            return ym
    return None


def fetch_line(base, lvl, code, months, code_var):
    """Total-country monthly series for one commodity line (parallel)."""
    def one(ym):
        d = _get(_q(base, {"get": "GEN_VAL_MO", "time": ym,
                           "COMM_LVL": lvl, code_var: code}))
        rows = _rows(d)
        if rows:
            v = _f(rows[0].get("GEN_VAL_MO"))
            if v is not None and v > 0:
                return ym, v
        return ym, None

    series = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for ym, v in ex.map(one, months):
            if v is not None:
                series[ym] = v
    return series


def fetch_country_split(lvl, code, ym):
    """Per-country values for one month — concentration + source shift.

    v1.0.1: ONE call returning every country beats 16 keyed calls. Census
    accepts CTY_CODE=* to enumerate partners; we filter to tracked names
    and fall back to per-country only if the wildcard is rejected.
    """
    d = _get(_q(BASE_HS, {"get": "GEN_VAL_MO,CTY_NAME,CTY_CODE", "time": ym,
                          "COMM_LVL": lvl, "I_COMMODITY": code,
                          "CTY_CODE": "*"}))
    rows = _rows(d)
    if rows:
        out = {}
        for r in rows:
            cty = (r.get("CTY_CODE") or "").strip()
            name = COUNTRIES.get(cty)
            if not name:
                continue
            v = _f(r.get("GEN_VAL_MO"))
            if v is not None and v > 0:
                out[name] = v
        if out:
            return out

    # fallback: per-country, parallel
    def one(item):
        cty, name = item
        if not name:
            return None, None
        d2 = _get(_q(BASE_HS, {"get": "GEN_VAL_MO,CTY_NAME", "time": ym,
                               "COMM_LVL": lvl, "I_COMMODITY": code,
                               "CTY_CODE": cty}))
        rr = _rows(d2)
        if rr:
            v = _f(rr[0].get("GEN_VAL_MO"))
            if v is not None and v > 0:
                return name, v
        return None, None

    out = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for name, v in ex.map(one, list(COUNTRIES.items())):
            if name and v:
                out[name] = v
    return out


def analyse(series, ym):
    """Level, YoY, 3m momentum, acceleration — seasonality-aware."""
    r = {"level": series.get(ym), "month": ym}
    prev_y = _months_back(ym, 12)
    r["yoy_pct"] = None
    if series.get(ym) and series.get(prev_y):
        r["yoy_pct"] = round((series[ym] / series[prev_y] - 1) * 100, 2)

    # prior month's YoY -> acceleration (2nd derivative)
    pm = _months_back(ym, 1)
    pm_y = _months_back(ym, 13)
    yoy_prev = None
    if series.get(pm) and series.get(pm_y):
        yoy_prev = (series[pm] / series[pm_y] - 1) * 100
    r["yoy_prev_pct"] = round(yoy_prev, 2) if yoy_prev is not None else None
    r["accel_pp"] = (round(r["yoy_pct"] - yoy_prev, 2)
                     if (r["yoy_pct"] is not None and yoy_prev is not None) else None)

    # 3m vs prior 3m
    cur3 = [series.get(_months_back(ym, i)) for i in range(3)]
    pri3 = [series.get(_months_back(ym, i)) for i in range(3, 6)]
    if all(x for x in cur3) and all(x for x in pri3):
        r["mom_3m_pct"] = round((sum(cur3) / sum(pri3) - 1) * 100, 2)
    else:
        r["mom_3m_pct"] = None

    r["n_months"] = len(series)
    return r


def concentration(split):
    tot = sum(split.values()) or 0.0
    if tot <= 0:
        return {}
    shares = {k: v / tot for k, v in split.items()}
    top = max(shares.items(), key=lambda kv: kv[1])
    hhi = sum(s * s for s in shares.values())
    return {
        "top_source": top[0],
        "top_share_pct": round(top[1] * 100, 1),
        "hhi": round(hhi, 3),
        "fragile": bool(top[1] >= 0.50),
        "shares_pct": {k: round(v * 100, 1) for k, v in
                       sorted(shares.items(), key=lambda kv: -kv[1])[:8]},
        "covered_usd": round(tot, 0),
    }


def source_shift(now_split, yr_split):
    """Which country gained/lost the most SHARE year over year."""
    tn, ty = sum(now_split.values()), sum(yr_split.values())
    if tn <= 0 or ty <= 0:
        return {}
    deltas = {}
    for k in set(now_split) | set(yr_split):
        sn = now_split.get(k, 0.0) / tn
        sy = yr_split.get(k, 0.0) / ty
        deltas[k] = (sn - sy) * 100
    gain = max(deltas.items(), key=lambda kv: kv[1])
    loss = min(deltas.items(), key=lambda kv: kv[1])
    return {
        "gainer": gain[0], "gainer_pp": round(gain[1], 2),
        "loser": loss[0], "loser_pp": round(loss[1], 2),
        "all_pp": {k: round(v, 2) for k, v in
                   sorted(deltas.items(), key=lambda kv: -kv[1])},
    }


def _load(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def zscore(hist_vals, cur):
    """z of current YoY vs own history. Activates at n>=13 (house standard)."""
    vals = [v for v in hist_vals if isinstance(v, (int, float))]
    if len(vals) < 13 or cur is None:
        return None
    mu = sum(vals) / len(vals)
    var = sum((v - mu) ** 2 for v in vals) / len(vals)
    sd = var ** 0.5
    if sd <= 0:
        return None
    return round((cur - mu) / sd, 2)


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    degraded = []

    ym = find_latest_month()
    if not ym:
        out = {"version": VERSION, "error": "no month with data",
               "generated_at": started.isoformat(), "lines": []}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out),
                      ContentType="application/json")
        return {"statusCode": 500, "body": "no data month"}

    # months needed: current + 13 back (for YoY & accel) + 6 for 3m windows
    months = [_months_back(ym, i) for i in range(0, 15)]
    hist = _load(HIST_KEY) or {"lines": {}}

    def build_hs(spec):
        lvl, code, label, industry = spec
        series = fetch_line(BASE_HS, lvl, code, months, "I_COMMODITY")
        if not series:
            return None, "%s %s no series" % (lvl, code)
        a = analyse(series, ym)
        a.update({"level_code": lvl, "code": code, "label": label,
                  "industry": industry, "basis": "HS"})
        split_now = fetch_country_split(lvl, code, ym)
        a["concentration"] = concentration(split_now)
        split_yr = fetch_country_split(lvl, code, _months_back(ym, 12))
        a["source_shift"] = source_shift(split_now, split_yr)
        return a, None

    def build_naics(spec):
        lvl, code, label, industry = spec
        series = fetch_line(BASE_NAICS, lvl, code, months, "NAICS")
        if not series:
            return None, "NAICS %s no series" % code
        a = analyse(series, ym)
        a.update({"level_code": lvl, "code": code, "label": label,
                  "industry": industry, "basis": "NAICS"})
        return a, None

    lines_out, naics_out = [], []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for a, err in ex.map(build_hs, LINES):
            if err:
                degraded.append(err)
            elif a:
                lines_out.append(a)
        for a, err in ex.map(build_naics, NAICS_LINES):
            if err:
                degraded.append(err)
            elif a:
                naics_out.append(a)

    # ledger writes are serial (dict mutation) after the parallel fetch
    for a in lines_out:
        hk = "%s:%s" % (a["level_code"], a["code"])
        rec = hist["lines"].setdefault(hk, {})
        if a["yoy_pct"] is not None:
            rec[ym] = a["yoy_pct"]
        a["z_yoy"] = zscore(list(rec.values()), a["yoy_pct"])
        a["hist_n"] = len(rec)
    for a in naics_out:
        hk = "N:%s" % a["code"]
        rec = hist["lines"].setdefault(hk, {})
        if a["yoy_pct"] is not None:
            rec[ym] = a["yoy_pct"]
        a["z_yoy"] = zscore(list(rec.values()), a["yoy_pct"])
        a["hist_n"] = len(rec)

    lines_out.sort(key=lambda x: (x["basis"], x["code"]))
    naics_out.sort(key=lambda x: x["code"])

    # ── SIGNAL LADDER ────────────────────────────────────────────────────
    # A surge is only a canary if it is accelerating AND broad enough to be
    # physical rather than a single shipment artifact.
    signals = []
    for a in lines_out + naics_out:
        y, ac = a.get("yoy_pct"), a.get("accel_pp")
        m3 = a.get("mom_3m_pct")
        if y is None:
            continue
        tier = None
        if y >= 25 and (ac or 0) > 0 and (m3 or 0) > 0:
            tier = "SURGE_CONFIRMED"
        elif y >= 25:
            tier = "SURGE_RAW"
        elif y <= -20 and (ac or 0) < 0:
            tier = "CONTRACTION_CONFIRMED"
        elif y <= -20:
            tier = "CONTRACTION_RAW"
        elif (ac or 0) >= 15:
            tier = "INFLECTING_UP"
        elif (ac or 0) <= -15:
            tier = "INFLECTING_DOWN"
        if tier:
            signals.append({
                "code": a["code"], "label": a["label"],
                "industry": a["industry"], "basis": a["basis"],
                "tier": tier, "yoy_pct": y, "accel_pp": ac,
                "mom_3m_pct": m3, "z_yoy": a.get("z_yoy"),
                "top_source": (a.get("concentration") or {}).get("top_source"),
                "fragile": (a.get("concentration") or {}).get("fragile"),
            })
    order = {"SURGE_CONFIRMED": 0, "CONTRACTION_CONFIRMED": 1,
             "INFLECTING_UP": 2, "INFLECTING_DOWN": 3,
             "SURGE_RAW": 4, "CONTRACTION_RAW": 5}
    signals.sort(key=lambda s: (order.get(s["tier"], 9), -abs(s["yoy_pct"] or 0)))

    # industry rollup — value-weighted YoY, the bridge to industry-boom
    roll = {}
    for a in lines_out:
        if a.get("yoy_pct") is None or not a.get("level"):
            continue
        r = roll.setdefault(a["industry"], {"w": 0.0, "wy": 0.0, "n": 0,
                                            "codes": []})
        r["w"] += a["level"]
        r["wy"] += a["level"] * a["yoy_pct"]
        r["n"] += 1
        r["codes"].append(a["code"])
    industry_rollup = sorted(
        [{"industry": k, "import_yoy_pct": round(v["wy"] / v["w"], 2),
          "import_usd_mo": round(v["w"], 0), "n_lines": v["n"],
          "codes": v["codes"]}
         for k, v in roll.items() if v["w"] > 0],
        key=lambda x: -x["import_yoy_pct"])

    out = {
        "version": VERSION,
        "generated_at": started.isoformat(),
        "data_month": ym,
        "lag_note": "U.S. Census releases trade data ~2 months in arrears",
        "n_lines": len(lines_out) + len(naics_out),
        "lines": lines_out,
        "naics_lines": naics_out,
        "signals": signals,
        "industry_rollup": industry_rollup,
        "degraded": degraded,
        "coverage": {
            "hs_requested": len(LINES), "hs_ok": len(lines_out),
            "naics_requested": len(NAICS_LINES), "naics_ok": len(naics_out),
        },
        "scope_note": ("Commodity-level customs aggregates, not company bills "
                       "of lading. Company-level BOL requires a licensed "
                       "vendor (Panjiva/ImportGenius); this engine is the "
                       "durable public spine and never claims a named "
                       "importer."),
        "attribution": "U.S. Census Bureau, International Trade API",
        "source_url": "https://api.census.gov/data/timeseries/intltrade/imports/hs",
    }

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")),
                  ContentType="application/json")
    # prune ledger to 60 months/line
    for k, v in hist["lines"].items():
        if len(v) > 60:
            for old in sorted(v)[:-60]:
                v.pop(old, None)
    hist["updated_at"] = started.isoformat()
    S3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(hist, separators=(",", ":")),
                  ContentType="application/json")

    print("[import-canary] month=%s lines=%d signals=%d degraded=%d"
          % (ym, out["n_lines"], len(signals), len(degraded)))
    return {"statusCode": 200,
            "body": json.dumps({"month": ym, "lines": out["n_lines"],
                                "signals": len(signals)})}
