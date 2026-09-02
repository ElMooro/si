"""justhodl-cycle-features  v1.1.0 -- feature store for the global business cycle.

v1.1.0 (ops 5102): live-first OECD lanes. The walked DSD_KEI@DF_KEI file is
truncated at the walker's 40MB cap (5 of 22 measures), so KEI is now pulled
live for our 34 countries (22 measures: production, retail, exports, imports,
BCI, CCI, unemployment, long/short rates, money, CLI...), plus DF_BTS (order
books, production expectations, export orders), DF_CS (consumer confidence),
DF_FINMARK (rates fallback), and the BIS API for monthly REER (34/34) and
policy rates (24/34). Every live pull is cached to data/warm/oecd/cycle/ and
data/warm/bis/cycle/; a failed pull falls back to a cache <= 45 days old; the
walked warehouse file remains the last fallback. Requests are paced.

Turns the warehouses the platform already holds into ONE compact document of
per-country, monthly, cycle-ready features that justhodl-global-business-cycle
v3 standardises and fuses. Every feature is a real published series with its
source, period, transform and sign recorded; nothing is filled in.

Sources (ops 5098/5099 audit, 2026-09-02):
  OECD  DSD_STES@DF_CLI   live SDMX (paced, cached)  amplitude-adjusted CLI, 17 majors
        DSD_KEI@DF_KEI    warehouse (34/34)          production/turnover YoY, BCI, CCI,
                                                     exports/imports YoY, rates (curve)
        DSD_LFS@DF_IALFS_UNE_M  warehouse (29/34)    unemployment rate (monthly)
  BIS   WS_TC (credit/GDP -> 4q impulse), WS_SPP (real house price YoY),
        WS_EER (REER 12m %), WS_CREDIT_GAP (gap), WS_DSR (debt service)  33/34
  Eurostat  EI_BSCO_M (consumer conf), EI_BSSI_M_R2 (ESI), EI_BSIN_M_R2
        (industry conf), EI_LMHR_M (unemployment)   EU members
  Fleet  data/asia-leads.json (Korea exports), data/global-sovereign.json
        (10y yield - policy rate: curve slope, latest point)

Output
  data/cycle/features.json.gz        {grid, countries:{ISO3:{features:{name:{values[],...}}}}}
  data/cycle/features-manifest.json  coverage matrix, freshness, source status
  data/warm/oecd/cycle/DF_CLI.csv.gz cache of the live CLI pull (self-healing)

Pillars: survey (leading), activity (coincident), financial (leading),
trade (coincident/leading). `sign` gives the direction in which a higher value
is pro-cyclical (+1) or counter-cyclical (-1); the consumer applies it.
"""
import csv
import gzip
import io
import json
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import boto3

VERSION = "1.1.1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/cycle/features.json.gz"
MANIFEST_KEY = "data/cycle/features-manifest.json"
CLI_CACHE_KEY = "data/warm/oecd/cycle/DF_CLI.csv.gz"
GRID_START = "2001-01"
S3 = boto3.client("s3", region_name="us-east-1")

ISO3 = ["USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "CAN", "BRA", "KOR", "AUS", "ESP", "MEX", "IDN", "NLD",
        "TUR", "CHE", "POL", "BEL", "SWE", "IRL", "AUT", "NOR", "ZAF", "DNK", "FIN", "CZE", "HUN", "CHL", "PRT", "GRC",
        "NZL", "ISR"]
ISO2 = {"USA": "US", "CHN": "CN", "JPN": "JP", "DEU": "DE", "IND": "IN", "GBR": "GB", "FRA": "FR", "ITA": "IT", "CAN": "CA",
        "BRA": "BR", "KOR": "KR", "AUS": "AU", "ESP": "ES", "MEX": "MX", "IDN": "ID", "NLD": "NL", "TUR": "TR", "CHE": "CH",
        "POL": "PL", "BEL": "BE", "SWE": "SE", "IRL": "IE", "AUT": "AT", "NOR": "NO", "ZAF": "ZA", "DNK": "DK", "FIN": "FI",
        "CZE": "CZ", "HUN": "HU", "CHL": "CL", "PRT": "PT", "GRC": "GR", "NZL": "NZ", "ISR": "IL"}
ISO2_TO_3 = {v: k for k, v in ISO2.items()}
ISO2_TO_3.update({"UK": "GBR", "EL": "GRC"})
NAME_TO_ISO3 = {"United States": "USA", "China": "CHN", "Japan": "JPN", "Germany": "DEU", "India": "IND",
                "United Kingdom": "GBR", "France": "FRA", "Italy": "ITA", "Canada": "CAN", "Brazil": "BRA",
                "South Korea": "KOR", "Korea": "KOR", "Australia": "AUS", "Spain": "ESP", "Mexico": "MEX",
                "Indonesia": "IDN", "Netherlands": "NLD", "Turkey": "TUR", "Türkiye": "TUR", "Switzerland": "CHE",
                "Poland": "POL", "Belgium": "BEL", "Sweden": "SWE", "Ireland": "IRL", "Austria": "AUT", "Norway": "NOR",
                "South Africa": "ZAF", "Denmark": "DNK", "Finland": "FIN", "Czech Republic": "CZE", "Czechia": "CZE",
                "Hungary": "HUN", "Chile": "CHL", "Portugal": "PRT", "Greece": "GRC", "New Zealand": "NZL", "Israel": "ISR"}

# feature name -> (pillar, sign, transform label)
FEATURE_META = {
    "cli_oecd":        ("survey",    +1, "OECD amplitude-adjusted CLI, index (100 = long-term trend)"),
    "bci":             ("survey",    +1, "OECD business confidence, level"),
    "cci":             ("survey",    +1, "OECD consumer confidence, level"),
    "esi":             ("survey",    +1, "Eurostat economic sentiment indicator, level"),
    "cons_conf_eu":    ("survey",    +1, "Eurostat consumer confidence balance, SA"),
    "ind_conf_eu":     ("survey",    +1, "Eurostat industry confidence balance, SA"),
    "ip_yoy":          ("activity",  +1, "industrial production, % y/y"),
    "retail_yoy":      ("activity",  +1, "retail trade volume, % y/y"),
    "unemp_12m":       ("activity",  -1, "unemployment rate, 12-month change (pp)"),
    "credit_impulse":  ("financial", +1, "private non-financial credit/GDP, 4-quarter change (pp)"),
    "house_real_yoy":  ("financial", +1, "real residential property prices, % y/y"),
    "reer_12m":        ("financial", -1, "real broad effective exchange rate, 12-month % change"),
    "curve":           ("financial", +1, "long-term minus short-term interest rate (pp)"),
    "money_yoy":       ("financial", +1, "broad money, % y/y"),
    "policy_12m":      ("financial", -1, "central bank policy rate, 12-month change (pp)"),
    "order_books":     ("survey",    +1, "OECD BTS manufacturing order books, balance"),
    "prod_expect":     ("survey",    +1, "OECD BTS production expectations (future tendency), balance"),
    "exports_yoy":     ("trade",     +1, "merchandise exports value, % y/y"),
    "imports_yoy":     ("trade",     +1, "merchandise imports value, % y/y"),
    "export_orders":   ("trade",     +1, "OECD BTS export order books, balance"),
    # side metrics (not in the composite; reported for fragility context)
    "credit_gap":      ("side",      -1, "BIS credit-to-GDP gap (pp)"),
    "dsr":             ("side",      -1, "BIS debt service ratio, private non-financial (%)"),
}
MAX_LAG_MONTHS = {"M": 4, "Q": 7, "D": 2}

LOG = []


def log(msg):
    LOG.append(msg)
    print(f"[cycle-features] {msg}")


# ── period helpers ─────────────────────────────────────────────────────────
def month_grid(start, end):
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def to_month(period):
    """'2026-06' -> '2026-06'; '2026-Q1' -> '2026-03'; '2026-08-04' -> '2026-08'; '2026' -> None."""
    p = (period or "").strip()
    if len(p) >= 7 and p[4] == "-" and p[5:7].isdigit():
        return p[:7]
    if len(p) == 7 and p[5] == "Q":
        q = int(p[6])
        return f"{p[:4]}-{q * 3:02d}"
    if len(p) == 6 and p[4] == "Q":
        q = int(p[5])
        return f"{p[:4]}-{q * 3:02d}"
    return None


def months_between(a, b):
    return (int(b[:4]) - int(a[:4])) * 12 + (int(b[5:7]) - int(a[5:7]))


def shift_month(p, k):
    y, m = int(p[:4]), int(p[5:7]) + k
    while m > 12:
        y, m = y + 1, m - 12
    while m < 1:
        y, m = y - 1, m + 12
    return f"{y:04d}-{m:02d}"


class Series:
    """Sparse monthly series: {month: value}. Daily/quarterly inputs are placed
    on the month (last obs of the month wins; quarter -> its last month)."""

    def __init__(self, freq="M"):
        self.d = {}
        self.freq = freq

    def put(self, period, value):
        """Daily inputs: the last observation of the month wins."""
        mo = to_month(period)
        if mo is None or value is None:
            return
        p = (period or "").strip()
        if len(p) == 10:
            day = p[8:10]
            if not hasattr(self, "_day"):
                self._day = {}
            if day < self._day.get(mo, "00"):
                return
            self._day[mo] = day
        self.d[mo] = value

    def __len__(self):
        return len(self.d)

    def latest(self):
        return max(self.d) if self.d else None

    def yoy(self):
        out = Series(self.freq)
        for mo, v in self.d.items():
            prev = self.d.get(shift_month(mo, -12))
            if prev not in (None, 0) and v is not None:
                out.d[mo] = (v / prev - 1.0) * 100.0
        return out

    def diff(self, k):
        out = Series(self.freq)
        for mo, v in self.d.items():
            prev = self.d.get(shift_month(mo, -k))
            if prev is not None and v is not None:
                out.d[mo] = v - prev
        return out

    def pct(self, k):
        out = Series(self.freq)
        for mo, v in self.d.items():
            prev = self.d.get(shift_month(mo, -k))
            if prev not in (None, 0) and v is not None:
                out.d[mo] = (v / prev - 1.0) * 100.0
        return out

    def minus(self, other):
        out = Series(self.freq)
        for mo, v in self.d.items():
            w = other.d.get(mo)
            if w is not None:
                out.d[mo] = v - w
        return out


# ── S3 / HTTP ──────────────────────────────────────────────────────────────
def get_bytes(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    body = o["Body"].read()
    if key.endswith(".gz") or body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body, o["LastModified"]


def get_json(key):
    try:
        b, _ = get_bytes(key)
        return json.loads(b)
    except Exception as e:  # noqa: BLE001
        log(f"{key}: {str(e)[:80]}")
        return None


def http_get(url, headers=None, timeout=90, retries=3, backoff=(15, 45, 90)):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers or {"User-Agent": "JustHodl.AI cycle-features/1.0 (+https://justhodl.ai)"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read(), r.status
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code in (429, 500, 502, 503, 504) and i < retries - 1:
                time.sleep(backoff[min(i, len(backoff) - 1)])
                continue
            break
        except Exception as e:  # noqa: BLE001
            last = str(e)[:100]
            if i < retries - 1:
                time.sleep(backoff[min(i, len(backoff) - 1)])
    return None, last


def sdmx_rows(body):
    """Yield dict rows from an SDMX-CSV body (OECD csvfile / BIS csv)."""
    text = body.decode("utf-8", "replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    rd = csv.reader(io.StringIO(text))
    hdr = [h.strip() for h in next(rd)]
    for row in rd:
        if len(row) < len(hdr) - 2:
            continue
        yield dict(zip(hdr, row))


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── live lanes with cache ────────────────────────────────────────────────────
CACHE_MAX_AGE_D = 45
AREAS = "+".join(ISO3)
ISO2_LIST = "+".join(ISO2[i] for i in ISO3)
OECD_BASE = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,"
LANES = {
    # name: (url, cache key, fallback warehouse key or None)
    "KEI": (OECD_BASE + f"DSD_KEI@DF_KEI,/{AREAS}.M........?startPeriod=1995-01&format=csvfile",
            "data/warm/oecd/cycle/DF_KEI.csv.gz", "data/warm/oecd/data/DSD_KEI@DF_KEI.dat.gz"),
    "BTS": (OECD_BASE + f"DSD_STES@DF_BTS,/{AREAS}.M.........?startPeriod=1995-01&format=csvfile",
            "data/warm/oecd/cycle/DF_BTS.csv.gz", None),
    "CS": (OECD_BASE + f"DSD_STES@DF_CS,/{AREAS}.M.........?startPeriod=1995-01&format=csvfile",
           "data/warm/oecd/cycle/DF_CS.csv.gz", None),
    "FINMARK": (OECD_BASE + f"DSD_STES@DF_FINMARK,/{AREAS}.M.IRLT+IR3TIB+IRSTCI......?startPeriod=1995-01&format=csvfile",
                "data/warm/oecd/cycle/DF_FINMARK.csv.gz", None),
    "BIS_EER": (f"https://stats.bis.org/api/v2/data/dataflow/BIS/WS_EER/1.0/M.R.B.{ISO2_LIST}?startPeriod=1995-01&format=csv",
                "data/warm/bis/cycle/WS_EER_M.csv.gz", "data/warm/bis/data/WS_EER.dat.gz"),
    "BIS_CBPOL": (f"https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/M.{ISO2_LIST}?startPeriod=1995-01&format=csv",
                  "data/warm/bis/cycle/WS_CBPOL_M.csv.gz", None),
}


def fetch_lane(name, status, pace_s=3):
    """Live pull -> cache; on failure cache (<= 45d) -> warehouse fallback. Returns (body, source_label)."""
    url, cache_key, fallback = LANES[name]
    time.sleep(pace_s)
    body, st = http_get(url, timeout=150)
    if body is not None and len(body) > 1000:
        try:
            S3.put_object(Bucket=BUCKET, Key=cache_key, Body=gzip.compress(body), ContentType="application/gzip",
                          Metadata={"as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"), "url": url[:900]})
        except Exception as e:  # noqa: BLE001
            log(f"{name}: cache write failed {str(e)[:80]}")
        status[f"lane_{name}"] = {"ok": True, "source": "live", "bytes": len(body)}
        return body, "live"
    log(f"{name}: live failed ({st}); trying cache")
    try:
        cached, lm = get_bytes(cache_key)
        age_d = (datetime.now(timezone.utc) - lm).total_seconds() / 86400
        if age_d <= CACHE_MAX_AGE_D:
            status[f"lane_{name}"] = {"ok": True, "source": f"cache {age_d:.0f}d", "bytes": len(cached), "live_error": str(st)[:80]}
            return cached, f"cache {age_d:.0f}d"
        log(f"{name}: cache too old ({age_d:.0f}d)")
    except Exception as e:  # noqa: BLE001
        log(f"{name}: no cache ({str(e)[:60]})")
    if fallback:
        try:
            wb, lm = get_bytes(fallback)
            status[f"lane_{name}"] = {"ok": True, "source": "warehouse fallback", "bytes": len(wb), "live_error": str(st)[:80]}
            return wb, "warehouse"
        except Exception as e:  # noqa: BLE001
            log(f"{name}: warehouse fallback failed {str(e)[:60]}")
    status[f"lane_{name}"] = {"ok": False, "error": f"live {st}; no usable cache/fallback"}
    return None, None


# ── OECD live CLI (paced, cached) ───────────────────────────────────────────
def load_oecd_cli(feat, status):
    url = ("https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.1/"
           ".M.LI...AA...H?startPeriod=1990-01&format=csvfile")
    body, st = http_get(url)
    src = "live"
    if body is None or len(body) < 1000:
        log(f"OECD DF_CLI live failed ({st}); trying cache")
        try:
            body, lm = get_bytes(CLI_CACHE_KEY)
            src = f"cache {lm.date()}"
        except Exception as e:  # noqa: BLE001
            status["oecd_cli"] = {"ok": False, "error": f"live {st}; cache {str(e)[:60]}"}
            return
    else:
        try:
            S3.put_object(Bucket=BUCKET, Key=CLI_CACHE_KEY, Body=gzip.compress(body), ContentType="application/gzip",
                          Metadata={"as_of": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        except Exception as e:  # noqa: BLE001
            log(f"cache write failed: {str(e)[:80]}")
    n = 0
    for row in sdmx_rows(body):
        area = row.get("REF_AREA")
        if area not in feat or row.get("MEASURE") != "LI":
            continue
        v = fnum(row.get("OBS_VALUE"))
        if v is None:
            continue
        feat[area]["cli_oecd"].put(row.get("TIME_PERIOD"), v)
        n += 1
    covered = [a for a in feat if len(feat[a].get("cli_oecd") or [])]
    status["oecd_cli"] = {"ok": True, "source": src, "rows": n, "countries": len(covered),
                          "latest": max((feat[a]["cli_oecd"].latest() or "") for a in covered) if covered else None}
    log(f"OECD DF_CLI ({src}): {n} rows, {len(covered)} countries, latest {status['oecd_cli']['latest']}")


# ── OECD KEI (warehouse) ────────────────────────────────────────────────────
def load_oecd_kei(feat, status):
    body, src = fetch_lane("KEI", status)
    if body is None:
        status["oecd_kei"] = {"ok": False, "error": "no KEI data from any lane"}
        return
    lm = datetime.now(timezone.utc)
    # collect candidate series: (area, measure, activity, adjustment, transformation, unit) -> Series
    cand = defaultdict(lambda: Series("M"))
    measures = defaultdict(int)
    for row in sdmx_rows(body):
        if row.get("FREQ") != "M":
            continue
        area = row.get("REF_AREA")
        if area not in feat:
            continue
        v = fnum(row.get("OBS_VALUE"))
        if v is None:
            continue
        m = row.get("MEASURE")
        measures[m] += 1
        key = (area, m, row.get("ACTIVITY", ""), row.get("ADJUSTMENT", ""), row.get("TRANSFORMATION", ""), row.get("UNIT_MEASURE", ""))
        cand[key].put(row.get("TIME_PERIOD"), v)

    def pick(area, measure, activities, adjustments, transformations, min_obs=36):
        """Freshest qualifying series wins; the preference orders only break ties
        (ops 5102: Germany's PRVM/BTE split ended 2023-12 while PRVM/C runs to 2026-06)."""
        best, best_key = None, None
        for ai, act in enumerate(activities):
            for ji, adj in enumerate(adjustments):
                for ti, tr in enumerate(transformations):
                    for (a, m, ac, ad, t, u), s in cand.items():
                        if a == area and m == measure and (act is None or ac == act) and (adj is None or ad == adj) and (tr is None or t == tr) and len(s) >= min_obs:
                            key = (s.latest() or "", -ai, -ji, -ti, len(s))
                            if best is None or key > best_key:
                                best, best_key = s, key
        return best

    got = defaultdict(int)
    for area in feat:
        # industrial production, % y/y (GY) -- industry ex construction (BTE) first, then manufacturing (C), then total
        s = pick(area, "PRVM", ["BTE", "C", "B-E", "_Z", None], ["Y", "N", None], ["GY"])
        if s is None:
            lvl = pick(area, "PRVM", ["BTE", "C", "B-E", "_Z", None], ["Y", "N", None], ["_Z"])
            s = lvl.yoy() if lvl is not None else None
        if s is not None and len(s):
            feat[area]["ip_yoy"] = s
            got["ip_yoy"] += 1
        s = pick(area, "TOVM", ["G47", "G", "_Z", None], ["Y", "N", None], ["GY"])
        if s is None:
            lvl = pick(area, "TOVM", ["G47", "G", "_Z", None], ["Y", "N", None], ["_Z"])
            s = lvl.yoy() if lvl is not None else None
        if s is not None and len(s):
            feat[area]["retail_yoy"] = s
            got["retail_yoy"] += 1
        for meas, name in (("BCICP", "bci"), ("CCICP", "cci")):
            s = pick(area, meas, [None], ["Y", "N", None], ["_Z", "IX", None])
            if s is not None and len(s):
                feat[area][name] = s
                got[name] += 1
        for meas, name in (("EX", "exports_yoy"), ("IM", "imports_yoy"), ("XTEXVA", "exports_yoy"), ("XTIMVA", "imports_yoy")):
            if name in feat[area] and len(feat[area][name]):
                continue
            s = pick(area, meas, [None], ["Y", "N", "_Z", None], ["GY"])
            if s is None:
                lvl = pick(area, meas, [None], ["Y", "N", "_Z", None], ["_Z"])
                s = lvl.yoy() if lvl is not None else None
            if s is not None and len(s):
                feat[area][name] = s
                got[name] += 1
        s = pick(area, "MABM", [None], ["Y", "N", "_Z", None], ["GY"])
        if s is None:
            lvl = pick(area, "MABM", [None], ["Y", "N", "_Z", None], ["_Z"])
            s = lvl.yoy() if lvl is not None else None
        if s is not None and len(s):
            feat[area]["money_yoy"] = s
            got["money_yoy"] += 1
        u = pick(area, "UNEMP", [None], ["Y", "N", "_Z", None], ["_Z", None])
        if u is not None and len(u) >= 36:
            feat[area]["unemp_12m"] = u.diff(12)
            feat[area]["unemp_12m"].level = u
            feat[area]["unemp_12m"].src_override = "OECD DSD_KEI@DF_KEI UNEMP"
            got["unemp_12m"] += 1
        lt = pick(area, "IRLT", [None], [None], ["_Z", None], min_obs=24)
        stt = None
        for m in ("IR3TIB", "IRSTCI", "IRSTCB", "IRSTPI"):
            stt = pick(area, m, [None], [None], ["_Z", None], min_obs=24)
            if stt is not None:
                break
        if lt is not None and stt is not None:
            cv = lt.minus(stt)
            if len(cv):
                feat[area]["curve"] = cv
                got["curve"] += 1
        li = pick(area, "LI", [None], ["AA", "Y", "_Z", None], ["_Z", "IX", None], min_obs=36)
        if li is not None and not len(feat[area].get("cli_oecd") or []):
            feat[area]["cli_oecd"] = li
            feat[area]["cli_oecd"].src_override = "OECD DSD_KEI@DF_KEI LI"
            got["cli_from_kei"] += 1
    status["oecd_kei"] = {"ok": True, "source": src, "measures_seen": dict(sorted(measures.items(), key=lambda kv: -kv[1])[:40]),
                          "features": dict(got)}
    log(f"OECD KEI: measures {list(measures)[:30]} -> features {dict(got)}")


def load_oecd_unemployment(feat, status):
    try:
        body, lm = get_bytes("data/warm/oecd/data/DSD_LFS@DF_IALFS_UNE_M.dat.gz")
    except Exception as e:  # noqa: BLE001
        status["oecd_lfs"] = {"ok": False, "error": str(e)[:80]}
        return
    cand = defaultdict(lambda: Series("M"))
    for row in sdmx_rows(body):
        if row.get("FREQ") != "M" or row.get("SEX") not in ("_T", "") or row.get("AGE") not in ("Y_GE15", "_T", "Y15T64"):
            continue
        area = row.get("REF_AREA")
        if area not in feat:
            continue
        v = fnum(row.get("OBS_VALUE"))
        if v is None:
            continue
        cand[(area, row.get("ADJUSTMENT", ""), row.get("AGE", ""))].put(row.get("TIME_PERIOD"), v)
    got = 0
    for area in feat:
        best = None
        for adj in ("Y", "N", ""):
            for age in ("Y_GE15", "_T", "Y15T64"):
                s = cand.get((area, adj, age))
                if s is not None and len(s) >= 36 and (best is None or s.latest() > best.latest()):
                    best = s
            if best is not None:
                break
        if best is not None and ("unemp_12m" not in feat[area] or (feat[area]["unemp_12m"].level.latest() or "") < (best.latest() or "")):
            feat[area]["unemp_12m"] = best.diff(12)
            feat[area]["unemp_12m"].level = best
            feat[area]["unemp_12m"].src_override = "OECD DSD_LFS@DF_IALFS_UNE_M"
            got += 1
    status["oecd_lfs"] = {"ok": True, "countries": got, "file_age_h": round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)}
    log(f"OECD LFS unemployment: {got} countries")


# ── OECD business tendency + consumer surveys + FINMARK ───────────────────────
def _stes_candidates(body, feat, keep_measures):
    cand = defaultdict(lambda: Series("M"))
    for row in sdmx_rows(body):
        if row.get("FREQ") != "M":
            continue
        area = row.get("REF_AREA")
        m = row.get("MEASURE")
        if area not in feat or m not in keep_measures:
            continue
        v = fnum(row.get("OBS_VALUE"))
        if v is None:
            continue
        cand[(area, m, row.get("ACTIVITY", ""), row.get("ADJUSTMENT", ""), row.get("TIME_HORIZ", ""))].put(row.get("TIME_PERIOD"), v)
    return cand


def _pick_stes(cand, area, measure, activities, horizons, min_obs=36):
    best = None
    for act in activities:
        for hz in horizons:
            for (a, m, ac, ad, h), s in cand.items():
                if a == area and m == measure and (act is None or ac == act) and (hz is None or h == hz) and len(s) >= min_obs:
                    if best is None or (s.latest() or "") > (best.latest() or ""):
                        best = s
            if best is not None:
                return best
    return best


def load_oecd_bts(feat, status):
    body, src = fetch_lane("BTS", status)
    if body is None:
        return
    cand = _stes_candidates(body, feat, {"BCICP", "OB", "PR", "XR", "EM", "CURT"})
    got = defaultdict(int)
    for area in feat:
        if "bci" not in feat[area]:
            s = _pick_stes(cand, area, "BCICP", ["_Z", "C", None], ["_Z", None])
            if s is not None:
                feat[area]["bci"] = s
                feat[area]["bci"].src_override = "OECD DSD_STES@DF_BTS BCICP"
                got["bci"] += 1
        s = _pick_stes(cand, area, "OB", ["C", None], ["_Z", "C", None])
        if s is not None:
            feat[area]["order_books"] = s
            got["order_books"] += 1
        s = _pick_stes(cand, area, "PR", ["C", None], ["FT", "T", None])
        if s is not None:
            feat[area]["prod_expect"] = s
            got["prod_expect"] += 1
        s = _pick_stes(cand, area, "XR", ["C", None], ["_Z", "C", None])
        if s is not None:
            feat[area]["export_orders"] = s
            got["export_orders"] += 1
    status["oecd_bts"] = {"ok": True, "source": src, "features": dict(got)}
    log(f"OECD BTS ({src}): {dict(got)}")


def load_oecd_cs(feat, status):
    body, src = fetch_lane("CS", status)
    if body is None:
        return
    cand = _stes_candidates(body, feat, {"CCICP"})
    got = 0
    for area in feat:
        if "cci" in feat[area]:
            continue
        s = _pick_stes(cand, area, "CCICP", ["_Z", None], ["_Z", None])
        if s is not None:
            feat[area]["cci"] = s
            feat[area]["cci"].src_override = "OECD DSD_STES@DF_CS CCICP"
            got += 1
    status["oecd_cs"] = {"ok": True, "source": src, "countries_added": got}
    log(f"OECD CS ({src}): cci added for {got}")


def load_oecd_finmark(feat, status):
    need = [a for a in feat if "curve" not in feat[a]]
    if not need:
        status["oecd_finmark"] = {"ok": True, "skipped": "curve complete from KEI"}
        return
    body, src = fetch_lane("FINMARK", status)
    if body is None:
        return
    cand = _stes_candidates(body, feat, {"IRLT", "IR3TIB", "IRSTCI"})
    got = 0
    for area in need:
        lt = _pick_stes(cand, area, "IRLT", [None], [None], min_obs=24)
        st = _pick_stes(cand, area, "IR3TIB", [None], [None], min_obs=24) or _pick_stes(cand, area, "IRSTCI", [None], [None], min_obs=24)
        if lt is not None and st is not None:
            cv = lt.minus(st)
            if len(cv):
                feat[area]["curve"] = cv
                feat[area]["curve"].src_override = "OECD DSD_STES@DF_FINMARK IRLT-IR3TIB"
                got += 1
    status["oecd_finmark"] = {"ok": True, "source": src, "countries_added": got}
    log(f"OECD FINMARK ({src}): curve added for {got}")


def load_bis_live(feat, status):
    body, src = fetch_lane("BIS_EER", status, pace_s=1)
    if body is not None:
        cand = defaultdict(lambda: Series("M"))
        for row in sdmx_rows(body):
            if row.get("FREQ") != "M" or row.get("EER_TYPE") != "R" or row.get("EER_BASKET") != "B":
                continue
            i3 = ISO2_TO_3.get(row.get("REF_AREA"))
            if i3 in feat:
                v = fnum(row.get("OBS_VALUE"))
                if v is not None:
                    cand[i3].put(row.get("TIME_PERIOD"), v)
        got = 0
        for i3, s in cand.items():
            if len(s) >= 36:
                feat[i3]["reer_12m"] = s.pct(12)
                feat[i3]["reer_12m"].level = s
                got += 1
        status["bis_WS_EER"] = {"ok": True, "source": src, "countries": got}
        log(f"BIS EER ({src}): {got} countries")
    body, src = fetch_lane("BIS_CBPOL", status, pace_s=1)
    if body is not None:
        cand = defaultdict(lambda: Series("M"))
        for row in sdmx_rows(body):
            if row.get("FREQ") != "M":
                continue
            i3 = ISO2_TO_3.get(row.get("REF_AREA"))
            if i3 in feat:
                v = fnum(row.get("OBS_VALUE"))
                if v is not None:
                    cand[i3].put(row.get("TIME_PERIOD"), v)
        got = 0
        for i3, s in cand.items():
            if len(s) >= 36:
                feat[i3]["policy_12m"] = s.diff(12)
                feat[i3]["policy_12m"].level = s
                got += 1
        status["bis_WS_CBPOL"] = {"ok": True, "source": src, "countries": got}
        log(f"BIS CBPOL ({src}): {got} countries")


# ── BIS ───────────────────────────────────────────────────────────────────────
def load_bis(feat, status):
    def read(fid):
        try:
            body, lm = get_bytes(f"data/warm/bis/data/{fid}.dat.gz")
            return body, lm
        except Exception as e:  # noqa: BLE001
            status[f"bis_{fid}"] = {"ok": False, "error": str(e)[:80]}
            return None, None

    # total credit -> credit/GDP (private non-fin, all lenders, % of GDP) -> 4q change
    body, lm = read("WS_TC")
    if body:
        cand = defaultdict(lambda: Series("Q"))
        for row in sdmx_rows(body):
            if row.get("TC_BORROWERS") != "P" or row.get("TC_LENDERS") != "A" or row.get("UNIT_TYPE") != "770":
                continue
            i3 = ISO2_TO_3.get(row.get("BORROWERS_CTY"))
            if i3 not in feat:
                continue
            v = fnum(row.get("OBS_VALUE"))
            if v is None:
                continue
            cand[(i3, row.get("VALUATION"), row.get("TC_ADJUST"))].put(row.get("TIME_PERIOD"), v)
        got = 0
        for i3 in feat:
            best = None
            for val in ("M", "N"):
                for adj in ("A", "U"):
                    s = cand.get((i3, val, adj))
                    if s is not None and len(s) >= 20 and (best is None or s.latest() > best.latest()):
                        best = s
                if best is not None:
                    break
            if best is not None:
                imp = best.diff(12)      # 4 quarters == 12 months on the monthly grid
                imp.freq = "Q"
                feat[i3]["credit_impulse"] = imp
                feat[i3]["credit_impulse"].level = best
                got += 1
        status["bis_WS_TC"] = {"ok": True, "countries": got, "file_age_h": round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)}
        log(f"BIS WS_TC credit impulse: {got} countries")
    body, lm = read("WS_SPP")
    if body:
        cand = defaultdict(lambda: Series("Q"))
        for row in sdmx_rows(body):
            if row.get("VALUE") != "R" or row.get("UNIT_MEASURE") != "771":
                continue
            i3 = ISO2_TO_3.get(row.get("REF_AREA"))
            if i3 not in feat:
                continue
            v = fnum(row.get("OBS_VALUE"))
            if v is not None:
                cand[i3].put(row.get("TIME_PERIOD"), v)
        got = 0
        for i3, s in cand.items():
            if len(s) >= 20:
                feat[i3]["house_real_yoy"] = s
                got += 1
        status["bis_WS_SPP"] = {"ok": True, "countries": got}
        log(f"BIS WS_SPP real house prices: {got} countries")
    body, lm = read("WS_EER") if any("reer_12m" not in feat[i] for i in feat) else (None, None)
    if body:
        cand = defaultdict(lambda: Series("M"))
        for row in sdmx_rows(body):
            if row.get("FREQ") != "M" or row.get("EER_TYPE") != "R" or row.get("EER_BASKET") != "B":
                continue
            i3 = ISO2_TO_3.get(row.get("REF_AREA"))
            if i3 not in feat:
                continue
            v = fnum(row.get("OBS_VALUE"))
            if v is not None:
                cand[i3].put(row.get("TIME_PERIOD"), v)
        got = 0
        for i3, s in cand.items():
            if len(s) >= 36 and "reer_12m" not in feat[i3]:
                feat[i3]["reer_12m"] = s.pct(12)
                feat[i3]["reer_12m"].level = s
                got += 1
        status["bis_WS_EER_warehouse"] = {"ok": True, "countries_added": got}
        log(f"BIS WS_EER (warehouse) REER 12m: {got} countries added")
    body, lm = read("WS_CREDIT_GAP")
    if body:
        got = 0
        cand = defaultdict(lambda: Series("Q"))
        for row in sdmx_rows(body):
            if row.get("CG_DTYPE") != "C":
                continue
            i3 = ISO2_TO_3.get(row.get("BORROWERS_CTY"))
            if i3 in feat:
                v = fnum(row.get("OBS_VALUE"))
                if v is not None:
                    cand[i3].put(row.get("TIME_PERIOD"), v)
        for i3, s in cand.items():
            if len(s) >= 8:
                feat[i3]["credit_gap"] = s
                got += 1
        status["bis_WS_CREDIT_GAP"] = {"ok": True, "countries": got}
    body, lm = read("WS_DSR")
    if body:
        got = 0
        cand = defaultdict(lambda: Series("Q"))
        for row in sdmx_rows(body):
            if row.get("DSR_BORROWERS") != "P":
                continue
            i3 = ISO2_TO_3.get(row.get("BORROWERS_CTY"))
            if i3 in feat:
                v = fnum(row.get("OBS_VALUE"))
                if v is not None:
                    cand[i3].put(row.get("TIME_PERIOD"), v)
        for i3, s in cand.items():
            if len(s) >= 8:
                feat[i3]["dsr"] = s
                got += 1
        status["bis_WS_DSR"] = {"ok": True, "countries": got}


# ── Eurostat ──────────────────────────────────────────────────────────────────
def eurostat_tsv(flow):
    body, lm = get_bytes(f"data/warm/eurostat/data/{flow}.dat.gz")
    text = body.decode("utf-8", "replace")
    lines = text.split("\n")
    hdr = lines[0].rstrip("\r").split("\t")
    dims = hdr[0].split("\\")[0].split(",")
    periods = [c.strip() for c in hdr[1:]]
    for ln in lines[1:]:
        ln = ln.rstrip("\r")
        if not ln:
            continue
        cells = ln.split("\t")
        key = dict(zip(dims, cells[0].split(",")))
        yield key, periods, cells[1:], lm


def load_eurostat(feat, status):
    spec = [("EI_BSCO_M", "cons_conf_eu", {"indic": "BS-CSMCI", "s_adj": "SA"}),
            ("EI_BSSI_M_R2", "esi", {"indic": "BS-ESI-I", "s_adj": "SA"}),
            ("EI_BSIN_M_R2", "ind_conf_eu", {"indic": ("BS-ICI-BAL", "BS-ICI", "BS-ICI-BAL-SA"), "s_adj": "SA"}),
            ("EI_LMHR_M", "unemp_eu", {"indic": "LM-UN-T-TOT", "s_adj": "SA", "unit": "PC_ACT"})]
    for flow, name, want in spec:
        try:
            got = 0
            seen_indic = set()
            for key, periods, vals, lm in eurostat_tsv(flow):
                seen_indic.add(key.get("indic"))
                if any((key.get(k) not in v) if isinstance(v, tuple) else (key.get(k) != v) for k, v in want.items()):
                    continue
                i3 = ISO2_TO_3.get(key.get("geo"))
                if i3 not in feat:
                    continue
                s = Series("M")
                for p, v in zip(periods, vals):
                    tok = v.strip().split(" ")[0]
                    if tok in ("", ":"):
                        continue
                    x = fnum(tok)
                    if x is not None:
                        s.put(p, x)
                if len(s) >= 36:
                    if name == "unemp_eu":
                        if "unemp_12m" not in feat[i3] or (feat[i3]["unemp_12m"].latest() or "") < (s.latest() or ""):
                            feat[i3]["unemp_12m"] = s.diff(12)
                            feat[i3]["unemp_12m"].level = s
                            feat[i3]["unemp_12m"].src_override = f"eurostat:{flow}"
                    else:
                        feat[i3][name] = s
                    got += 1
            status[f"eurostat_{flow}"] = {"ok": True, "countries": got, "file_age_h": round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1),
                                          "indic_seen": sorted(x for x in seen_indic if x)[:20]}
            log(f"Eurostat {flow} -> {name}: {got} countries (indic seen {sorted(x for x in seen_indic if x)[:8]})")
        except Exception as e:  # noqa: BLE001
            status[f"eurostat_{flow}"] = {"ok": False, "error": str(e)[:100]}
            log(f"Eurostat {flow} failed: {str(e)[:100]}")


# ── fleet feeds ───────────────────────────────────────────────────────────────
def load_fleet_feeds(feat, status):
    al = get_json("data/asia-leads.json")
    if al and isinstance(al.get("korea_exports"), dict):
        ke = al["korea_exports"]
        hist = ke.get("history_24m") or []
        if hist and "exports_yoy" not in feat["KOR"]:
            lvl = Series("M")
            for h in hist:
                lvl.put(str(h.get("p", ""))[:7], fnum(h.get("v")))
            y = lvl.yoy()
            if len(y):
                feat["KOR"]["exports_yoy"] = y
                status["asia_leads_korea"] = {"ok": True, "latest": y.latest()}
    gs = get_json("data/global-sovereign.json")
    if gs:
        n = 0
        for c in gs.get("countries") or []:
            i3 = NAME_TO_ISO3.get(c.get("country"))
            y10, cb = fnum(c.get("yield_10y_pct")), fnum(c.get("cb_rate_pct"))
            if i3 in feat and y10 is not None and cb is not None:
                cur = feat[i3].get("curve")
                mo = datetime.now(timezone.utc).strftime("%Y-%m")
                if cur is not None and len(cur):
                    if (cur.latest() or "") < mo:
                        cur.d[mo] = y10 - cb          # nowcast point from the fleet's live sovereign desk
                        cur.nowcast_from = "data/global-sovereign.json"
                        n += 1
        status["global_sovereign_curve_nowcast"] = {"ok": True, "countries_extended": n}
        log(f"global-sovereign: extended {n} curve series to the current month")


# ── assemble ──────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    t0 = time.time()
    LOG.clear()
    now = datetime.now(timezone.utc)
    end_month = now.strftime("%Y-%m")
    grid = month_grid(GRID_START, end_month)
    idx = {m: i for i, m in enumerate(grid)}
    feat = {iso: defaultdict(lambda: Series("M")) for iso in ISO3}
    status = {}
    load_oecd_cli(feat, status)
    load_oecd_kei(feat, status)
    load_oecd_unemployment(feat, status)
    load_oecd_bts(feat, status)
    load_oecd_cs(feat, status)
    load_oecd_finmark(feat, status)
    load_bis_live(feat, status)
    load_bis(feat, status)
    load_eurostat(feat, status)
    load_fleet_feeds(feat, status)

    countries = {}
    coverage = {}
    for iso in ISO3:
        fs = {}
        for name, s in feat[iso].items():
            if not len(s) or name not in FEATURE_META:
                continue
            pillar, sign, label = FEATURE_META[name]
            vals = [None] * len(grid)
            n = 0
            for mo, v in s.d.items():
                i = idx.get(mo)
                if i is not None:
                    vals[i] = round(v, 4)
                    n += 1
            if n < 12:
                continue
            latest = s.latest()
            lvl = getattr(s, "level", None)
            fs[name] = {"pillar": pillar, "sign": sign, "label": label, "freq": s.freq, "values": vals, "n_obs": n,
                        "latest_period": latest, "latest_value": round(s.d[latest], 4) if latest else None,
                        "months_stale": months_between(latest, end_month) if latest else None,
                        "max_lag_months": MAX_LAG_MONTHS.get(s.freq, 4),
                        "level_latest": (round(lvl.d[lvl.latest()], 4) if lvl is not None and len(lvl) else None),
                        "source": getattr(s, "src_override", None) or {
                            "cli_oecd": "OECD DSD_STES@DF_CLI (live SDMX, cached)", "bci": "OECD DSD_KEI@DF_KEI BCICP",
                            "cci": "OECD DSD_KEI@DF_KEI CCICP", "ip_yoy": "OECD DSD_KEI@DF_KEI PRVM", "retail_yoy": "OECD DSD_KEI@DF_KEI TOVM",
                            "exports_yoy": "OECD DSD_KEI@DF_KEI XTEXVA", "imports_yoy": "OECD DSD_KEI@DF_KEI XTIMVA",
                            "curve": "OECD DSD_KEI@DF_KEI IRLT-IRST" + (" + global-sovereign nowcast" if getattr(s, "nowcast_from", None) else ""),
                            "unemp_12m": "OECD DSD_LFS@DF_IALFS_UNE_M", "credit_impulse": "BIS WS_TC (P/A, % GDP)",
                            "house_real_yoy": "BIS WS_SPP (real, y/y)", "reer_12m": "BIS WS_EER (real, broad)",
                            "credit_gap": "BIS WS_CREDIT_GAP", "dsr": "BIS WS_DSR", "esi": "Eurostat EI_BSSI_M_R2",
                            "cons_conf_eu": "Eurostat EI_BSCO_M", "ind_conf_eu": "Eurostat EI_BSIN_M_R2",
                            "money_yoy": "OECD DSD_KEI@DF_KEI MABM", "policy_12m": "BIS WS_CBPOL (monthly)",
                            "order_books": "OECD DSD_STES@DF_BTS OB", "prod_expect": "OECD DSD_STES@DF_BTS PR",
                            "export_orders": "OECD DSD_STES@DF_BTS XR"}.get(name, name)}
        countries[iso] = {"features": fs, "n_features": len(fs),
                          "pillars": sorted({v["pillar"] for v in fs.values() if v["pillar"] != "side"})}
        coverage[iso] = {"n_features": len(fs), "pillars": countries[iso]["pillars"],
                         "fresh_features": sum(1 for v in fs.values() if v["months_stale"] is not None and v["months_stale"] <= v["max_lag_months"]),
                         "features": {k: (v["latest_period"], v["months_stale"]) for k, v in fs.items()}}
    doc = {"version": VERSION, "generated_at": now.isoformat(timespec="seconds"), "elapsed_s": round(time.time() - t0, 1),
           "grid": {"start": GRID_START, "end": end_month, "months": grid}, "feature_meta": {k: {"pillar": v[0], "sign": v[1], "label": v[2]} for k, v in FEATURE_META.items()},
           "max_lag_months": MAX_LAG_MONTHS, "countries": countries, "sources": status, "log": LOG[-60:]}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=gzip.compress(json.dumps(doc, default=str).encode()), ContentType="application/json",
                  ContentEncoding="gzip", CacheControl="public, max-age=900")
    manifest = {"version": VERSION, "generated_at": doc["generated_at"], "elapsed_s": doc["elapsed_s"], "features_key": OUT_KEY,
                "n_countries": sum(1 for c in coverage.values() if c["n_features"]), "coverage": coverage, "sources": status,
                "feature_count_by_name": {n: sum(1 for c in countries.values() if n in c["features"]) for n in FEATURE_META}}
    S3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(manifest, default=str).encode(), ContentType="application/json",
                  CacheControl="public, max-age=300")
    log(f"done: {manifest['n_countries']} countries, features by name {manifest['feature_count_by_name']}")
    return {"statusCode": 200, "body": json.dumps({"version": VERSION, "n_countries": manifest["n_countries"],
                                                   "feature_count_by_name": manifest["feature_count_by_name"], "elapsed_s": doc["elapsed_s"]})}
