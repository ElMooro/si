"""justhodl-global-business-cycle  v3.0.1  (multi-pillar composite; equity momentum is one pillar)
═══════════════════════════════════════════════════════════════════════════
The OECD CLI series on FRED stopped updating ~Jan 2024 (28+ months stale at
time of writing). To provide a USEFUL global business cycle map with
≤3-month-old data per country, we now construct a SYNTHETIC composite
leading indicator from real-time inputs:

  Primary input — equity-market momentum (always fresh, leading by 6-12mo):
    • 12-month total return (trend signal)
    • 3-month return       (momentum signal)
    • Current price vs 200-day moving average

  Secondary input (where available within 3 months):
    • OECD Consumer Confidence (USA: USACSCICP02STSAM)
    • OECD Business Confidence (CHN: CHNBSCICP02STSAM)

The synthetic CLI is calibrated to the 0-100+ scale where:
  ≥100 = above trend (expansion territory)
  <100 = below trend (recession territory)
  Rising/falling determined by 3mo return direction

Output schema is BACKWARD COMPATIBLE with v1 — same data/global-business-cycle.json
key, same fields (phase, cli_level, six_month_change, trend, etc.) — so all
downstream consumers (KI, risk, allocator, morning-intel, ai-chat, page)
keep working without modification.

Sources (ordered chain per country — each candidate must clear the same
acceptance gate: >= MIN_BARS daily bars AND last bar within MAX_STALE_DAYS):
  1. Yahoo Finance chart API, national index      https://query1.finance.yahoo.com
  2. Yahoo Finance, equal-weight blue-chip BASKET  (markets whose index symbol
     is dead on Yahoo: Prague, Budapest, Oslo, Athens)
  3. polygon-full warehouse, US-listed country ETF (data/warm/polygon-full/grouped,
     OUR OWN store — no third-party call at run time)
  4. degraded: freshest stale candidate, flagged  (never a fake value)
  • FRED OECD CCI/BCI supplement (USA/CHN) — used ONLY when <= 3 months old and
    ALWAYS as an anomaly from its 100 normal (v2.0 treated the ~100 index level as
    a +-3 anomaly, which pinned USA at the 120 cap every run).

v3.0.1 (ops 5102): each feature's standardised value is carried forward for
  at most its max lag (monthly 4, quarterly 7) so history months hold what a
  nowcast would have seen and the last history row equals the nowcast; a
  component's staleness is measured from its actual last print. Feature
  store v1.1 adds the trade pillar (KEI exports/imports, BTS export orders),
  curve/CCI/unemployment for all countries, money growth, REER 34/34, policy
  rates 24/34, order books and production expectations.
v3.0.0 (ops 5100, 2026-09-02): MULTI-PILLAR COMPOSITE. Reads the feature store
  written by justhodl-cycle-features (data/cycle/features.json.gz -- OECD CLI/
  KEI/labour, BIS credit/property/REER, Eurostat surveys, fleet sovereign desk)
  and fuses five pillars per country: survey 0.35, financial 0.25, activity
  0.20, trade 0.10, equity 0.10 -- each feature standardised on its own history,
  freshness-gated, sign-adjusted (cycle_composite.py). When >= 2 non-equity
  pillars are present the country's cli_level/phase/trend come from the
  composite; otherwise the v2 equity read stands and is flagged `thin`. The
  equity-only fields are kept verbatim as equity_* for every consumer and the
  page. Adds a monthly composite history since 2004 and a calibrated 6-month
  downturn probability (data/global-business-cycle-composite-history.json).
v2.1.2 (ops 5097): supplements are standardised against their OWN trailing
  history (z-score of the latest print vs the prior <= 7y of monthly obs,
  1 z = 1.5 index points, clipped +-3). FRED re-based both series to OECD
  "percentage balance" units (USA prints ~50-60, CHN ~0 +-2), so a fixed
  "normal = 100" transform is wrong and any fixed transform is fragile;
  own-history standardisation is scale-agnostic. Still used only when the
  latest print is <= 3 months old and >= 24 prior obs exist.
v2.1.1 (ops 5096): a supplement whose scale is not recognised (neither ~100
  index nor a small anomaly) is EXCLUDED and reported, never coerced.
v2.1.0 (ops 5094/5095, 2026-09-01):
  - acceptance gate + candidate chain + basket + polygon ETF lane (CZE/HUN/NOR/GRC)
  - supplement anomaly fix (USA no longer saturates the cap)
  - soft clip: CLI = 100 + 20*tanh(0.025*composite) — same 100 boundary, same
    ordering, no saturation plateau at 120 (KOR/USA were pinned)
  - physical layer: last-good carry-forward with provenance when portwatch ships
    an empty ports list (previously every country flipped to UNCONFIRMED)
  - schema additions are strictly additive (source, source_attempts,
    supplement_status, days_stale, cli_transform, engine_version)

Author: JustHodl.AI
"""
import json
import os
import time
import urllib.request
import urllib.error
import urllib.parse
import gzip
import math
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass
try:
    import cycle_composite as CC
except Exception as _cce:  # noqa: BLE001
    CC = None
    print(f"[gbc] cycle_composite unavailable: {_cce}")

ENGINE_VERSION = "3.0.1"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/global-business-cycle.json"
HISTORY_KEY = "data/global-business-cycle-history.json"
COMPOSITE_HISTORY_KEY = "data/global-business-cycle-composite-history.json"
FEATURES_KEY = "data/cycle/features.json.gz"
FEATURES_MAX_AGE_H = 72          # a feature store older than 3 days is not a nowcast
BARS_ROOT = "data/warm/polygon-full/grouped/"

# Acceptance gate for ANY price candidate (index, basket member, ETF)
MIN_BARS = 252            # one full year — enough for ret_12m and the history pass
MAX_STALE_DAYS = 12       # calendar days; a live national index never gaps this long
                          # (CNY/Golden Week <= 9d). Longer = dead symbol on the source.
SUPPLEMENT_MAX_MONTHS = 3         # Khalid's rule: <= 3 months old or not used at all
SUPPLEMENT_POINT_TO_PCT = 5.0     # 1 index point of anomaly ~ 5 composite pct
SUPPLEMENT_WEIGHT = 0.25          # equity composite 75% / survey 25%
SUPPLEMENT_ANOM_CLIP = 3.0        # anomaly in index points is clipped to +-3
SUPPLEMENT_Z_TO_POINTS = 1.5      # OECD normalised CCI/BCI have sd ~1.5 points: 1 z = 1.5 pts
SUPPLEMENT_MIN_HIST = 24          # need >= 24 prior monthly obs to standardise
SUPPLEMENT_HIST_OBS = 90          # ~7.5y of monthly prints pulled from FRED

S3 = boto3.client("s3", region_name="us-east-1")


# ════════════════════════════════════════════════════════════════════════
# COUNTRY MAP — ISO3 → (Yahoo symbol, region, name, GDP weight, ISO2)
# ════════════════════════════════════════════════════════════════════════
COUNTRY_MAP = [
    # iso3, yahoo_sym,    region,           country_name,        weight, iso2
    ("USA", "^GSPC",       "North America", "United States",     25.0,  "US"),
    ("CHN", "000001.SS",   "Asia-Pacific",  "China",             18.0,  "CN"),
    ("JPN", "^N225",       "Asia-Pacific",  "Japan",              4.2,  "JP"),
    ("DEU", "^GDAXI",      "Europe",        "Germany",            4.0,  "DE"),
    ("IND", "^BSESN",      "Asia-Pacific",  "India",              3.6,  "IN"),
    ("GBR", "^FTSE",       "Europe",        "United Kingdom",     3.3,  "GB"),
    ("FRA", "^FCHI",       "Europe",        "France",             2.8,  "FR"),
    ("ITA", "FTSEMIB.MI",  "Europe",        "Italy",              2.1,  "IT"),
    ("CAN", "^GSPTSE",     "North America", "Canada",             2.0,  "CA"),
    ("BRA", "^BVSP",       "Latin America", "Brazil",             1.9,  "BR"),
    ("KOR", "^KS11",       "Asia-Pacific",  "South Korea",        1.6,  "KR"),
    ("AUS", "^AXJO",       "Asia-Pacific",  "Australia",          1.5,  "AU"),
    ("ESP", "^IBEX",       "Europe",        "Spain",              1.5,  "ES"),
    ("MEX", "^MXX",        "Latin America", "Mexico",             1.5,  "MX"),
    ("IDN", "^JKSE",       "Asia-Pacific",  "Indonesia",          1.3,  "ID"),
    ("NLD", "^AEX",        "Europe",        "Netherlands",        1.1,  "NL"),
    ("TUR", "XU100.IS",    "Europe",        "Turkey",             1.0,  "TR"),
    ("CHE", "^SSMI",       "Europe",        "Switzerland",        0.9,  "CH"),
    ("POL", "EPOL",        "Europe",        "Poland",             0.8,  "PL"),  # iShares MSCI Poland (US-listed)
    ("BEL", "^BFX",        "Europe",        "Belgium",            0.7,  "BE"),
    ("SWE", "^OMX",        "Europe",        "Sweden",             0.6,  "SE"),
    ("IRL", "^ISEQ",       "Europe",        "Ireland",            0.6,  "IE"),
    ("AUT", "^ATX",        "Europe",        "Austria",            0.6,  "AT"),
    ("NOR", "^OSEAX",      "Europe",        "Norway",             0.5,  "NO"),
    ("ZAF", "^J203.JO",    "Africa",        "South Africa",       0.5,  "ZA"),
    ("DNK", "^OMXC25",     "Europe",        "Denmark",            0.4,  "DK"),
    ("FIN", "^OMXH25",     "Europe",        "Finland",            0.3,  "FI"),
    ("CZE", "PX.PR",       "Europe",        "Czech Republic",     0.3,  "CZ"),  # Prague suffix format
    ("HUN", "BUX.BD",      "Europe",        "Hungary",            0.2,  "HU"),  # Budapest suffix format
    ("CHL", "ECH",         "Latin America", "Chile",              0.3,  "CL"),  # iShares MSCI Chile (US-listed)
    ("PRT", "PSI20.LS",    "Europe",        "Portugal",           0.3,  "PT"),
    ("GRC", "GD.AT",       "Europe",        "Greece",             0.2,  "GR"),
    ("NZL", "^NZ50",       "Asia-Pacific",  "New Zealand",        0.3,  "NZ"),
    ("ISR", "^TA125.TA",   "Middle East",   "Israel",             0.5,  "IL"),
]

# Countries with fresh supplementary FRED data (≤3mo)
FRED_SUPPLEMENT = {
    "USA": "USACSCICP02STSAM",
    "CHN": "CHNBSCICP02STSAM",
}


# ════════════════════════════════════════════════════════════════════════
# Data fetchers
# ════════════════════════════════════════════════════════════════════════
def yahoo_chart(symbol, range_param="2y", retries=3, fallback_symbols=None):
    """Fetch daily closing prices from Yahoo Finance chart endpoint.
       Returns list of (date_str, close) sorted ascending.
       If primary symbol fails, tries each fallback in order."""
    candidates = [symbol] + (fallback_symbols or [])
    for sym in candidates:
        result = _yahoo_fetch_one(sym, range_param, retries)
        if result:
            if sym != symbol:
                print(f"[gbc] {symbol} fell back to {sym}")
            return result, sym
    return [], symbol


def _yahoo_fetch_one(symbol, range_param, retries):
    """Internal — single-symbol fetch with retry."""
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{urllib.parse.quote(symbol)}?range={range_param}&interval=1d")
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; JustHodlAI-GBC/2.0)",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode("utf-8"))
            result = (data.get("chart") or {}).get("result") or []
            if not result:
                last_err = "empty result"
                time.sleep(0.5 * (attempt + 1))
                continue
            r0 = result[0]
            timestamps = r0.get("timestamp") or []
            indicators = r0.get("indicators") or {}
            quotes = (indicators.get("quote") or [{}])[0]
            adjclose_arr = ((indicators.get("adjclose") or [{}])[0].get("adjclose")
                              if indicators.get("adjclose") else None)
            closes = adjclose_arr if adjclose_arr else quotes.get("close") or []

            out = []
            for ts, c in zip(timestamps, closes):
                if c is None:
                    continue
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                out.append((date_str, float(c)))
            if out:
                return out
            last_err = "no usable bars"
            time.sleep(0.5 * (attempt + 1))
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            time.sleep(0.8 * (attempt + 1))
        except Exception as e:
            last_err = str(e)[:100]
            time.sleep(0.5 * (attempt + 1))
    print(f"[gbc] {symbol} failed: {last_err}")
    return []


# Per-country fallback symbols for known-flaky markets (index-level candidates)
SYMBOL_FALLBACKS = {
    "POL": ["WIG20.WA", "^WIG"],                 # if EPOL fails, try Warsaw direct
    "CZE": ["^PX", "PXTR.PR"],                   # Prague index candidates (PX.PR 404s on Yahoo)
    "HUN": ["^BUX"],                             # BUX.BD 404s on Yahoo
    "CHL": ["^IPSA", "^SPCLXIPSA"],              # if ECH fails, try Santiago direct
    "NOR": ["OBX.OL", "^OBX", "^OSEBX"],         # ^OSEAX stopped printing 2026-07-17
    "GRC": ["^ATG"],                             # GD.AT intermittently stops mid-July
}

# Equal-weight blue-chip BASKETS — the national index reconstructed from its own
# largest constituents when no index symbol clears the gate. A basket needs >= 3
# accepted members; the label records exactly which members were used.
BASKETS = {
    "CZE": ["CEZ.PR", "KOMB.PR", "MONET.PR", "ERBAG.PR", "VIG.PR", "COLT.PR"],
    "HUN": ["OTP.BD", "MOL.BD", "RICHT.BD", "MTELEKOM.BD"],
    "NOR": ["EQNR.OL", "DNB.OL", "TEL.OL", "NHY.OL", "MOWI.OL", "YAR.OL", "ORK.OL", "AKRBP.OL"],
    "GRC": ["OPAP.AT", "ETE.AT", "EUROB.AT", "TPEIR.AT", "ALPHA.AT", "PPC.AT", "MYTIL.AT", "OTE.AT"],
}

# US-listed country ETF for the polygon-full warehouse lane (our own store).
# USD-denominated, so it also carries FX — used as the THIRD rung only and
# labelled as such in `source`. Existence is checked in the warehouse, not assumed.
POLYGON_ETF = {
    "USA": "SPY", "CHN": "MCHI", "JPN": "EWJ", "DEU": "EWG", "IND": "INDA",
    "GBR": "EWU", "FRA": "EWQ", "ITA": "EWI", "CAN": "EWC", "BRA": "EWZ",
    "KOR": "EWY", "AUS": "EWA", "ESP": "EWP", "MEX": "EWW", "IDN": "EIDO",
    "NLD": "EWN", "TUR": "TUR", "CHE": "EWL", "POL": "EPOL", "BEL": "EWK",
    "SWE": "EWD", "IRL": "EIRL", "AUT": "EWO", "NOR": "ENOR", "ZAF": "EZA",
    "DNK": "EDEN", "FIN": "EFNL", "CHL": "ECH", "PRT": "PGAL", "GRC": "GREK",
    "NZL": "ENZL", "ISR": "EIS",
}


def _today():
    return datetime.now(timezone.utc).date()


def acceptable(prices):
    """(ok, reason). A candidate is usable only if it has a full year of bars AND
    is still printing. Anything else is tried-and-rejected with the reason kept."""
    if not prices:
        return False, "no bars"
    if len(prices) < MIN_BARS:
        return False, f"only {len(prices)} bars (< {MIN_BARS})"
    try:
        last = datetime.strptime(prices[-1][0], "%Y-%m-%d").date()
    except Exception:
        return False, "unparseable last date"
    stale = (_today() - last).days
    if stale > MAX_STALE_DAYS:
        return False, f"stale {stale}d (last {prices[-1][0]})"
    return True, f"{len(prices)} bars, last {prices[-1][0]}"


def build_basket(symbols, range_param="5y"):
    """Equal-weight, forward-filled, base-100 basket of accepted members.
    Returns (prices, label, attempts)."""
    members, attempts = {}, []
    for s in symbols:
        px = _yahoo_fetch_one(s, range_param, retries=2)
        ok, why = acceptable(px)
        attempts.append({"candidate": f"yahoo:{s}", "ok": ok, "why": why})
        if ok:
            members[s] = dict(px)
    if len(members) < 3:
        return [], None, attempts
    all_dates = sorted(set().union(*[set(m) for m in members.values()]))
    ff = {}
    for s, m in members.items():
        last, arr = None, {}
        for d in all_dates:
            if d in m:
                last = m[d]
            if last is not None:
                arr[d] = last
        ff[s] = arr
    start = next((d for d in all_dates if all(d in ff[s] for s in ff)), None)
    if start is None:
        return [], None, attempts
    base = {s: ff[s][start] for s in ff}
    out = []
    for d in all_dates:
        if d < start:
            continue
        vals = [ff[s][d] / base[s] for s in ff if d in ff[s] and base[s] > 0]
        if len(vals) < max(3, int(0.6 * len(ff))):
            continue
        out.append((d, 100.0 * sum(vals) / len(vals)))
    return out, "basket:" + "+".join(sorted(members)), attempts


class PolygonETFLane:
    """Lazy reader of our own polygon-full grouped-daily warehouse (one gz doc per
    session, all tickers). Loaded ONCE per run, only for the tickers requested,
    only if at least one country needs it."""

    def __init__(self, years=5, workers=12):
        self.years = years
        self.workers = workers
        self.loaded = False
        self.series = {}          # ticker -> [(date, close)]
        self.n_sessions = 0
        self.error = None

    def _keys(self):
        keys = []
        yr0 = _today().year - self.years
        pag = S3.get_paginator("list_objects_v2")
        for yr in range(yr0, _today().year + 1):
            for page in pag.paginate(Bucket=BUCKET, Prefix=f"{BARS_ROOT}{yr}/"):
                for o in page.get("Contents") or []:
                    if o["Key"].endswith(".json.gz"):
                        keys.append(o["Key"])
        keys.sort()
        cutoff = (_today() - timedelta(days=365 * self.years)).isoformat()
        return [k for k in keys if k.rsplit("/", 1)[1][:10] >= cutoff]

    def _load_one(self, key, keep):
        try:
            body = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            j = json.loads(gzip.decompress(body))
        except Exception as e:  # noqa: BLE001
            return key, None, str(e)[:80]
        d = key.rsplit("/", 1)[1][:10]
        rows = []
        for r in j.get("results") or []:
            t = r.get("T")
            c = r.get("c")
            if t in keep and c is not None and c > 0:
                rows.append((t, float(c)))
        return d, rows, None

    def load(self, tickers):
        if self.loaded:
            return
        self.loaded = True
        keep = set(t for t in tickers if t)
        try:
            keys = self._keys()
        except Exception as e:  # noqa: BLE001
            self.error = f"list failed: {str(e)[:120]}"
            print(f"[polygon-lane] {self.error}")
            return
        if not keys:
            self.error = "no grouped sessions found"
            print(f"[polygon-lane] {self.error}")
            return
        acc = defaultdict(list)
        errs = 0
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            for d, rows, err in ex.map(lambda k: self._load_one(k, keep), keys):
                if rows is None:
                    errs += 1
                    continue
                for t, c in rows:
                    acc[t].append((d, c))
        for t, rows in acc.items():
            rows.sort()
            self.series[t] = rows
        self.n_sessions = len(keys)
        print(f"[polygon-lane] {len(keys)} sessions, {len(self.series)}/{len(keep)} "
              f"tickers present, {errs} unreadable sessions")

    def get(self, ticker):
        return self.series.get(ticker, [])


def resolve_prices(iso3, primary, lane):
    """Walk the source chain for one country. Returns
    (prices, source_label, attempts, degraded_reason_or_None)."""
    attempts = []
    best_stale = None          # (last_date, prices, label) of the freshest rejected-for-staleness candidate
    for sym in [primary] + SYMBOL_FALLBACKS.get(iso3, []):
        px = _yahoo_fetch_one(sym, "5y", retries=2 if sym != primary else 3)
        ok, why = acceptable(px)
        attempts.append({"candidate": f"yahoo:{sym}", "ok": ok, "why": why})
        if ok:
            return px, f"yahoo:{sym}", attempts, None
        if px and len(px) >= MIN_BARS and (best_stale is None or px[-1][0] > best_stale[0]):
            best_stale = (px[-1][0], px, f"yahoo:{sym}")
    if iso3 in BASKETS:
        bpx, blabel, batt = build_basket(BASKETS[iso3])
        attempts.extend(batt)
        ok, why = acceptable(bpx) if bpx else (False, "basket needs >= 3 accepted members")
        attempts.append({"candidate": f"basket:{iso3}", "ok": ok, "why": why})
        if ok:
            return bpx, blabel, attempts, None
    etf = POLYGON_ETF.get(iso3)
    if etf and lane is not None:
        lane.load([POLYGON_ETF[k] for k in POLYGON_ETF])
        epx = lane.get(etf)
        ok, why = acceptable(epx)
        if lane.error:
            why = f"lane: {lane.error}"
        attempts.append({"candidate": f"polygon-etf:{etf}", "ok": ok, "why": why})
        if ok:
            return epx, f"polygon-etf:{etf}", attempts, None
    if best_stale is not None:
        last_date, px, label = best_stale
        stale_days = (_today() - datetime.strptime(last_date, "%Y-%m-%d").date()).days
        return px, label, attempts, f"degraded: freshest candidate is {stale_days}d stale"
    return [], None, attempts, "no candidate cleared the acceptance gate"


def fred_recent(series_id, limit=SUPPLEMENT_HIST_OBS):
    """Most recent `limit` valid observations, ascending [(date, value)]."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    out = []
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-GBC/2.1"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8"))
        for o in data.get("observations", []):
            v = o.get("value")
            if v in (".", "", None):
                continue
            try:
                out.append((o["date"], float(v)))
            except (ValueError, TypeError):
                continue
    except Exception as e:  # noqa: BLE001
        print(f"[gbc] FRED {series_id} failed: {e}")
    out.sort()
    return out


def standardise_supplement(series_id, obs):
    """Scale-agnostic supplement: z-score of the latest print against its own
    prior history, mapped to OECD-style index points and clipped. Returns the
    dict consumed by compute_cli_from_prices (status 'fresh' only when usable)."""
    if not obs:
        return {"series_id": series_id, "status": "unavailable"}
    latest_date, latest = obs[-1]
    mo = months_between(latest_date)
    sup = {"series_id": series_id, "date": latest_date, "value": latest, "months_stale": mo,
           "n_hist": len(obs) - 1, "method": "z-score of latest vs own prior history -> index points (1z=1.5pt, clip +-3)"}
    if mo > SUPPLEMENT_MAX_MONTHS:
        sup["status"] = f"stale {mo}mo (excluded)"
        return sup
    hist = [v for _, v in obs[:-1]]
    if len(hist) < SUPPLEMENT_MIN_HIST:
        sup["status"] = f"only {len(hist)} prior obs (< {SUPPLEMENT_MIN_HIST}) -- excluded"
        return sup
    mean = sum(hist) / len(hist)
    var = sum((v - mean) ** 2 for v in hist) / len(hist)
    std = var ** 0.5
    if std <= 1e-9:
        sup["status"] = "flat history (sd 0) -- excluded"
        return sup
    z = (latest - mean) / std
    anom = max(-SUPPLEMENT_ANOM_CLIP, min(SUPPLEMENT_ANOM_CLIP, z * SUPPLEMENT_Z_TO_POINTS))
    sup.update({"status": "fresh", "hist_mean": round(mean, 3), "hist_std": round(std, 3),
                "z": round(z, 3), "anomaly_points": round(anom, 3)})
    return sup


def fred_latest(series_id):
    """Fetch the most recent valid observation from FRED."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit=10")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-GBC/2.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        for o in data.get("observations", []):
            v = o.get("value")
            if v not in (".", "", None):
                try:
                    return {"date": o["date"], "value": float(v)}
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        print(f"[gbc] FRED {series_id} failed: {e}")
    return None


# ════════════════════════════════════════════════════════════════════════
# Synthetic CLI computation from equity series
# ════════════════════════════════════════════════════════════════════════
def cli_from_composite(composite_pct):
    """100 + 20*tanh(0.025*x): slope 0.5 at the 100 boundary (== v2.0 linear
    map), asymptotes at 80/120 instead of clipping. Monotonic, so phase and
    ordering are preserved; only the saturation plateau is gone."""
    return 100.0 + 20.0 * math.tanh(0.025 * float(composite_pct))


def supplement_anomaly(value):
    """OECD CCI/BCI on FRED are normalised with long-run mean 100 (prints in
    ~[90,110]); a series already expressed as an anomaly prints in (-10,10).
    Anything else (ops 5095 saw USACSCICP02STSAM print 53.26 and
    CHNBSCICP02STSAM print exactly 0.0) is a scale this engine does not
    understand -> None, and the caller EXCLUDES it. A guessed sign is not a
    measurement. Recognised values are clipped to +-SUPPLEMENT_ANOM_CLIP."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if 90.0 <= v <= 110.0:
        anom = v - 100.0
    elif -10.0 < v < 10.0 and v != 0.0:
        anom = v
    else:
        return None
    return max(-SUPPLEMENT_ANOM_CLIP, min(SUPPLEMENT_ANOM_CLIP, anom))


def months_between(date_str):
    """Calendar months between a YYYY-MM-DD date and today (floor)."""
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except Exception:
        return 999
    t = _today()
    return (t.year - d.year) * 12 + (t.month - d.month)


def compute_cli_from_prices(prices, supplement=None):
    """Build a synthetic CLI proxy from a 2y price series.
    Returns dict matching v1 schema for backward compatibility."""
    if not prices or len(prices) < 60:
        return {"phase": "UNKNOWN", "cli_level": None, "latest_date": None,
                "reason": "insufficient_history"}

    latest_date, latest_price = prices[-1]
    n = len(prices)

    def ret(days_back):
        if n <= days_back:
            return None
        prior = prices[-days_back - 1][1]
        if prior <= 0:
            return None
        return (latest_price / prior - 1.0) * 100

    ret_1m  = ret(21)
    ret_3m  = ret(63)
    ret_6m  = ret(126)
    ret_12m = ret(252)

    if n >= 200:
        ma200 = sum(p for _, p in prices[-200:]) / 200
        dist_200ma = (latest_price / ma200 - 1.0) * 100 if ma200 > 0 else 0
    else:
        dist_200ma = 0

    # Composite: weights for the 4 components
    components = []
    if ret_12m is not None: components.append(("ret_12m", ret_12m, 0.35))
    components.append(("dist_200ma", dist_200ma, 0.25))
    if ret_3m is not None: components.append(("ret_3m", ret_3m, 0.25))
    if ret_1m is not None: components.append(("ret_1m", ret_1m, 0.15))

    weighted_sum = sum(v * w for _, v, w in components)
    weight_total = sum(w for _, _, w in components)
    composite_pct = weighted_sum / weight_total if weight_total > 0 else 0

    # Blend with supplement (FRED OECD CCI/BCI) — ONLY when fresh, ALWAYS as an
    # anomaly from the series' normal of 100 (v2.0 fed the ~100 level in as if it
    # were a +-3 anomaly, which pinned USA at the 120 cap on every run).
    equity_composite_pct = composite_pct
    sup_status = "none"
    if supplement and supplement.get("value") is not None:
        sup_status = supplement.get("status") or "unknown"
        if sup_status == "fresh":
            anom = supplement.get("anomaly_points")
            if anom is None:
                anom = supplement_anomaly(supplement["value"])   # legacy caller without standardisation
            if anom is None:
                sup_status = f"scale-unrecognised ({supplement['value']}) -- excluded"
                supplement["status"] = sup_status
            else:
                composite_pct = (composite_pct * (1 - SUPPLEMENT_WEIGHT)
                                 + anom * SUPPLEMENT_POINT_TO_PCT * SUPPLEMENT_WEIGHT)

    # Map composite into a CLI-style score centered at 100 — soft clip (tanh):
    # identical to the v2.0 linear map near 100, bounded (80,120) without the
    # hard plateau that erased ordering among strong markets.
    cli_level = cli_from_composite(composite_pct)

    # Trend (3m momentum direction)
    if ret_3m is None or abs(ret_3m) < 1.0:
        trend = "flat"
    elif ret_3m > 0:
        trend = "rising"
    else:
        trend = "falling"

    # Phase classification
    if cli_level >= 100 and trend == "rising":
        phase = "EXPANSION"
    elif cli_level >= 100 and trend == "falling":
        phase = "AT_RISK"
    elif cli_level < 100 and trend == "falling":
        phase = "RECESSION"
    elif cli_level < 100 and trend == "rising":
        phase = "RECOVERY"
    else:
        phase = "EXPANSION" if cli_level >= 100 else "RECOVERY"

    # z-score of 12m return vs trailing distribution
    z_score = 0.0
    if n >= 252:
        rolling_returns = []
        for i in range(252, n):
            prior = prices[i - 252][1]
            curr = prices[i][1]
            if prior > 0:
                rolling_returns.append((curr / prior - 1.0) * 100)
        if len(rolling_returns) >= 30:
            mean = sum(rolling_returns) / len(rolling_returns)
            var = sum((r - mean) ** 2 for r in rolling_returns) / len(rolling_returns)
            std = var ** 0.5
            if std > 0 and ret_12m is not None:
                z_score = (ret_12m - mean) / std

    return {
        "phase": phase,
        "cli_level": round(cli_level, 2),
        "composite_pct": round(composite_pct, 2),
        "six_month_change": round(ret_6m, 3) if ret_6m is not None else None,
        "mom_change":  round(ret_1m, 3) if ret_1m is not None else None,
        "yoy_change":  round(ret_12m, 3) if ret_12m is not None else None,
        "three_month_change": round(ret_3m, 3) if ret_3m is not None else None,
        "dist_200ma_pct": round(dist_200ma, 2),
        "z_5y": round(z_score, 2),
        "trend": trend,
        "comp_period": "3m_equity_momentum",
        "latest_date": latest_date,
        "history_n": n,
        "supplement_value": supplement.get("value") if supplement else None,
        "supplement_date": supplement.get("date") if supplement else None,
        "supplement_status": sup_status,
        "supplement_z": supplement.get("z") if supplement else None,
        "supplement_anomaly_points": supplement.get("anomaly_points") if supplement else None,
        "equity_composite_pct": round(equity_composite_pct, 2),
        "cli_transform": "soft_clip_tanh_v2.1",
    }


# ════════════════════════════════════════════════════════════════════════
# Regional + global aggregation (unchanged from v1)
# ════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
# PHYSICAL CONFIRMATION — port throughput (ops 3842)
# ═══════════════════════════════════════════════════════════════════════════
# The phase labels above are derived from an EQUITY-MOMENTUM composite. That
# leads, but its documented failure mode is firing in drawdowns that never
# become recessions. Downstream, justhodl-global-recession found 84.3% of its
# GDP-weighted headline resting on exactly these unconfirmed labels.
#
# Port throughput is PHYSICAL and independent of equity prices, so it can
# corroborate or contradict a label at the SOURCE — which fixes every consumer
# at once rather than each one re-deriving it.
#
# STRICTLY ADDITIVE: `phase` is never modified. A new `physical` block is
# attached per country and consumers opt in. Countries need >=2 ports (a single
# port is a facility, not an economy) and unmapped names are reported, never
# silently dropped.

_PORT_ALIASES = {
    "south korea": "KOR", "korea": "KOR", "united states of america": "USA",
    "usa": "USA", "us": "USA", "uk": "GBR", "great britain": "GBR",
    "the netherlands": "NLD", "netherlands (kingdom of the)": "NLD",
    "russian federation": "RUS", "viet nam": "VNM", "czechia": "CZE",
    "czech republic": "CZE", "turkiye": "TUR", "t\u00fcrkiye": "TUR",
    "taiwan province of china": "TWN", "taiwan, province of china": "TWN",
}


def load_port_physical(country_meta):
    """{ISO3: {n_ports, median_yoy_pct, min_yoy_pct, worst_port, disrupted}}"""
    out, unmapped = {}, {}
    try:
        pw = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/portwatch.json")["Body"].read())
    except Exception as e:
        print(f"[physical] portwatch unavailable: {str(e)[:80]}")
        return {}, {}, None
    name_map = dict(_PORT_ALIASES)
    for iso, meta in (country_meta or {}).items():
        nm = (meta or {}).get("name")
        if isinstance(nm, str) and nm.strip():
            name_map.setdefault(nm.strip().lower(), iso.upper())
    agg = {}
    rows_in = pw.get("ports")
    if not isinstance(rows_in, list):
        rows_in = pw.get("rows") if isinstance(pw.get("rows"), list) else []
    for row in rows_in:
        if not isinstance(row, dict):
            continue
        y = row.get("yoy_pct", row.get("yoy"))
        if not isinstance(y, (int, float)):
            continue
        cn = str(row.get("country") or "").strip().lower()
        iso = name_map.get(cn)
        if not iso:
            unmapped[cn] = unmapped.get(cn, 0) + 1
            continue
        agg.setdefault(iso, []).append(row)
    for iso, rows in agg.items():
        ys = sorted(r["yoy_pct"] for r in rows)
        if len(ys) < 2:
            continue
        worst = min(rows, key=lambda r: r["yoy_pct"])
        out[iso] = {
            "n_ports": len(ys),
            "median_yoy_pct": round(ys[len(ys) // 2], 2),
            "min_yoy_pct": round(ys[0], 2),
            "worst_port": worst.get("name"),
            "n_disrupted": sum(1 for r in rows
                               if str(r.get("status", "")).upper() == "DISRUPTED"),
        }
    return out, dict(sorted(unmapped.items(), key=lambda kv: -kv[1])[:20]), \
        pw.get("generated_at")


def confirm_phase(phase, phys):
    """CONFIRMED / DIVERGENT / UNCONFIRMED — never alters `phase` itself."""
    if not phys:
        return {"state": "UNCONFIRMED",
                "note": "no port coverage (>=2 ports required) for this economy"}
    deteriorating = phase in ("RECESSION", "AT_RISK")
    physical_weak = phys["median_yoy_pct"] < 0
    state = "CONFIRMED" if deteriorating == physical_weak else "DIVERGENT"
    return {
        "state": state, "source": "port throughput (physical)",
        **phys,
        "note": ("physical trade volume, independent of the equity-momentum "
                 "composite that produced `phase`"),
    }


def aggregate(by_country):
    PHASES = ["EXPANSION", "AT_RISK", "RECESSION", "RECOVERY", "UNKNOWN"]
    by_region = defaultdict(lambda: {"phase_mix": defaultdict(float), "n_countries": 0,
                                       "total_weight": 0, "classified_weight": 0,
                                       "avg_cli": 0, "countries": []})
    global_mix = defaultdict(float)
    total_weight = 0
    classified_weight = 0
    weighted_cli_sum = 0

    for iso3, info in by_country.items():
        if not info or not info.get("phase"):
            continue
        region = info.get("region")
        weight = info.get("gdp_weight", 0)
        phase = info["phase"]
        cli = info.get("cli_level")

        by_region[region]["n_countries"] += 1
        by_region[region]["total_weight"] += weight
        by_region[region]["phase_mix"][phase] += weight
        by_region[region]["countries"].append(iso3)
        if cli is not None and phase != "UNKNOWN":
            by_region[region]["avg_cli"] += cli * weight
            by_region[region]["classified_weight"] += weight

        global_mix[phase] += weight
        total_weight += weight
        if cli is not None and phase != "UNKNOWN":
            weighted_cli_sum += cli * weight
            classified_weight += weight

    for region, info in by_region.items():
        if info["total_weight"] > 0:
            info["phase_mix_pct"] = {p: round(v / info["total_weight"] * 100, 1)
                                      for p, v in info["phase_mix"].items()}
            if info["classified_weight"] > 0:
                info["avg_cli"] = round(info["avg_cli"] / info["classified_weight"], 2)
            else:
                info["avg_cli"] = None
        else:
            info["phase_mix_pct"] = {}
        info["phase_mix"] = dict(info["phase_mix"])

    global_mix_pct = {p: round(global_mix[p] / total_weight * 100, 1)
                       for p in PHASES if global_mix[p] > 0} if total_weight > 0 else {}
    global_avg_cli = round(weighted_cli_sum / classified_weight, 2) if classified_weight > 0 else None
    classified_pct = (classified_weight / total_weight * 100) if total_weight > 0 else 0

    if classified_weight > 0:
        exp_norm = global_mix.get("EXPANSION", 0) / classified_weight * 100
        rec_norm = global_mix.get("RECOVERY", 0) / classified_weight * 100
        ar_norm = global_mix.get("AT_RISK", 0) / classified_weight * 100
        recession_norm = global_mix.get("RECESSION", 0) / classified_weight * 100
    else:
        exp_norm = rec_norm = ar_norm = recession_norm = 0

    expansion_breadth = exp_norm + rec_norm
    contraction_breadth = ar_norm + recession_norm

    if expansion_breadth > 60:
        global_phase = "GLOBAL_EXPANSION"
    elif contraction_breadth > 60:
        global_phase = "GLOBAL_CONTRACTION"
    elif rec_norm > 35:
        global_phase = "GLOBAL_RECOVERY"
    elif ar_norm > 35:
        global_phase = "GLOBAL_PEAKING"
    else:
        global_phase = "MIXED"

    return {
        "global_phase": global_phase,
        "global_avg_cli": global_avg_cli,
        "global_phase_mix_pct": global_mix_pct,
        "global_phase_mix_weight": dict(global_mix),
        "total_weight_covered": round(total_weight, 1),
        "classified_weight_covered": round(classified_weight, 1),
        "classification_coverage_pct": round(classified_pct, 1),
        "expansion_breadth_pct": round(expansion_breadth, 1),
        "contraction_breadth_pct": round(contraction_breadth, 1),
        "by_region": dict(by_region),
    }


# ════════════════════════════════════════════════════════════════════════
# Interpretation engine (unchanged from v1)
# ════════════════════════════════════════════════════════════════════════
def interpret_global_cycle(agg, by_country):
    global_phase = agg["global_phase"]
    global_cli = agg["global_avg_cli"]
    exp_breadth = agg["expansion_breadth_pct"]
    cont_breadth = agg["contraction_breadth_pct"]

    if global_phase == "GLOBAL_EXPANSION":
        cross_asset = {
            "us_large_equity":  {"signal": +2, "rationale": "Global expansion supports equity multiples"},
            "small_caps":       {"signal": +1, "rationale": "Cyclical exposure benefits in expansion"},
            "international_dm": {"signal": +1, "rationale": "DM expansion broad-based"},
            "emerging_markets": {"signal": +2, "rationale": "EM beta to global growth — overweight"},
            "commodities":      {"signal": +1, "rationale": "Demand-led commodity bid"},
            "high_yield":       {"signal": +1, "rationale": "Risk-on supports HY spreads tight"},
            "long_duration":    {"signal": -1, "rationale": "Expansion = rising rate pressure"},
            "gold":             {"signal":  0, "rationale": "Strategic only in expansion"},
            "dollar":           {"signal": -1, "rationale": "USD softens in global risk-on"},
            "bitcoin":          {"signal": +1, "rationale": "Liquidity + global growth tailwind"},
        }
        decisive = (f"GLOBAL EXPANSION · {exp_breadth:.0f}% of world GDP in expansion+recovery "
                    f"(avg CLI {global_cli}). Maximum cyclical risk exposure justified — overweight "
                    f"equity (especially EM + small caps), commodities, and HY credit. Underweight "
                    f"long duration and USD.")
    elif global_phase == "GLOBAL_PEAKING":
        cross_asset = {
            "us_large_equity":  {"signal":  0, "rationale": "Late-cycle — maintain core, watch escalation"},
            "small_caps":       {"signal": -1, "rationale": "Cyclicals weaken as cycle rolls over"},
            "international_dm": {"signal": -1, "rationale": "DM peaking — reduce broad exposure"},
            "emerging_markets": {"signal": -1, "rationale": "EM rolls over with DM lag"},
            "commodities":      {"signal":  0, "rationale": "Mixed — late-cycle inflationary bid"},
            "high_yield":       {"signal": -1, "rationale": "Spreads start widening late-cycle"},
            "long_duration":    {"signal": +1, "rationale": "Cycle rolling — duration starts working"},
            "gold":             {"signal": +1, "rationale": "Hedge as cycle peaks"},
            "dollar":           {"signal": +1, "rationale": "Defensive USD bid emerging"},
            "bitcoin":          {"signal":  0, "rationale": "Volatile around cycle peak"},
        }
        decisive = (f"GLOBAL PEAKING · expansion breadth {exp_breadth:.0f}% but rolling over "
                    f"(avg CLI {global_cli}, contraction breadth rising to {cont_breadth:.0f}%). "
                    f"Shift to quality — trim small caps + EM + HY by 30%, build duration + gold "
                    f"+ USD as defensive bid emerges.")
    elif global_phase == "GLOBAL_CONTRACTION":
        cross_asset = {
            "us_large_equity":  {"signal": -2, "rationale": "Global contraction — multiple compression severe"},
            "small_caps":       {"signal": -2, "rationale": "Small caps + cyclicals worst hit in contraction"},
            "international_dm": {"signal": -2, "rationale": "DM contracting — equity drawdown risk"},
            "emerging_markets": {"signal": -2, "rationale": "EM gets hit hardest"},
            "commodities":      {"signal": -1, "rationale": "Demand destruction in contraction"},
            "high_yield":       {"signal": -2, "rationale": "HY default cycle activates"},
            "long_duration":    {"signal": +2, "rationale": "Recession bid + cuts expected"},
            "gold":             {"signal": +2, "rationale": "Defensive store of value"},
            "dollar":           {"signal": +1, "rationale": "Initial safe-haven"},
            "bitcoin":          {"signal": -1, "rationale": "Risk-off in contraction"},
        }
        decisive = (f"GLOBAL CONTRACTION · {cont_breadth:.0f}% of world GDP in recession+at-risk "
                    f"(avg CLI {global_cli}). Defensive posture — minimize equity, maximize "
                    f"TLT + GLD + cash. Re-entry signal will be CLI improvement to trend.")
    elif global_phase == "GLOBAL_RECOVERY":
        cross_asset = {
            "us_large_equity":  {"signal": +2, "rationale": "Recovery = strongest equity returns historically"},
            "small_caps":       {"signal": +2, "rationale": "Small caps explode out of recovery"},
            "international_dm": {"signal": +1, "rationale": "DM recovery follows"},
            "emerging_markets": {"signal": +2, "rationale": "EM disproportionately benefits from recovery"},
            "commodities":      {"signal": +1, "rationale": "Demand bid returning"},
            "high_yield":       {"signal": +2, "rationale": "HY snapback in recovery"},
            "long_duration":    {"signal":  0, "rationale": "Mixed — rate expectations stabilize"},
            "gold":             {"signal":  0, "rationale": "Tactical only — risk-on resumes"},
            "dollar":           {"signal": -1, "rationale": "USD weakens as global risk-on resumes"},
            "bitcoin":          {"signal": +2, "rationale": "Liquidity tailwind + risk-on"},
        }
        decisive = (f"GLOBAL RECOVERY · recovery breadth {agg['global_phase_mix_pct'].get('RECOVERY', 0):.0f}% "
                    f"(avg CLI {global_cli}). Buy aggressively — small caps, EM, HY, BTC. Historically "
                    f"the highest-return regime.")
    else:
        cross_asset = {
            "us_large_equity":  {"signal":  0, "rationale": "Mixed cycle — maintain core"},
            "small_caps":       {"signal":  0, "rationale": "No clear cyclical signal"},
            "international_dm": {"signal":  0, "rationale": "Mixed — bottom-up selection"},
            "emerging_markets": {"signal":  0, "rationale": "Country-by-country in mixed cycle"},
            "commodities":      {"signal":  0, "rationale": "Tactical only"},
            "high_yield":       {"signal":  0, "rationale": "Spread carry but selective"},
            "long_duration":    {"signal":  0, "rationale": "No clear trend"},
            "gold":             {"signal":  0, "rationale": "Strategic 5%"},
            "dollar":           {"signal":  0, "rationale": "Range-bound"},
            "bitcoin":          {"signal":  0, "rationale": "Liquidity-driven"},
        }
        decisive = (f"MIXED CYCLE · no dominant phase (avg CLI {global_cli}, expansion {exp_breadth:.0f}% / "
                    f"contraction {cont_breadth:.0f}%). Bottom-up selection over top-down beta.")

    country_tilts = {}
    for iso3, info in by_country.items():
        if not info or not info.get("phase"):
            continue
        phase = info["phase"]
        country_tilts[iso3] = {
            "phase": phase,
            "tilt": (+2 if phase == "EXPANSION" else
                     +1 if phase == "RECOVERY" else
                     -1 if phase == "AT_RISK" else
                     -2 if phase == "RECESSION" else 0)
        }

    return {
        "decisive_call": decisive,
        "cross_asset": cross_asset,
        "country_tilts": country_tilts,
    }


# ════════════════════════════════════════════════════════════════════════
# Historical CLI series — weekly rolling computation across full price array
# ════════════════════════════════════════════════════════════════════════
def compute_history_series(prices, frequency_days=5):
    """Given daily prices [(date, close), ...], compute weekly rolling CLI
    history. Each output point is the CLI as it would have been published
    at that historical date — i.e. ret_12m = price / price 252 days prior.
    Returns list of dicts: {date, cli, phase, ret_12m, ret_3m, ret_1m, dist_200ma}.

    `frequency_days` controls sampling. 5 = ~weekly, 1 = daily."""
    if not prices or len(prices) < 252:
        return []
    out = []
    n = len(prices)
    # Need 252 days backward for ret_12m, so first computable index is 252.
    for i in range(252, n, frequency_days):
        date_i, p = prices[i]

        # Lookback returns
        prior_12m = prices[i - 252][1]
        ret_12m = (p / prior_12m - 1.0) * 100 if prior_12m > 0 else None

        ret_3m = None
        if i >= 63:
            prior_3m = prices[i - 63][1]
            if prior_3m > 0:
                ret_3m = (p / prior_3m - 1.0) * 100

        ret_1m = None
        if i >= 21:
            prior_1m = prices[i - 21][1]
            if prior_1m > 0:
                ret_1m = (p / prior_1m - 1.0) * 100

        # 200-day MA at this point
        if i >= 200:
            ma200 = sum(pp for _, pp in prices[i - 200:i]) / 200
            dist_200ma = (p / ma200 - 1.0) * 100 if ma200 > 0 else 0
        else:
            dist_200ma = 0

        # Composite (same weighting as live engine)
        components = []
        if ret_12m is not None: components.append((ret_12m, 0.35))
        components.append((dist_200ma, 0.25))
        if ret_3m is not None: components.append((ret_3m, 0.25))
        if ret_1m is not None: components.append((ret_1m, 0.15))
        weighted_sum = sum(v * w for v, w in components)
        weight_total = sum(w for _, w in components)
        composite_pct = weighted_sum / weight_total if weight_total > 0 else 0
        cli = cli_from_composite(composite_pct)

        # Phase (same logic as live engine)
        if ret_3m is None or abs(ret_3m) < 1.0:
            trend = "flat"
        elif ret_3m > 0:
            trend = "rising"
        else:
            trend = "falling"
        if cli >= 100 and trend == "rising":
            phase = "EXPANSION"
        elif cli >= 100 and trend == "falling":
            phase = "AT_RISK"
        elif cli < 100 and trend == "falling":
            phase = "RECESSION"
        elif cli < 100 and trend == "rising":
            phase = "RECOVERY"
        else:
            phase = "EXPANSION" if cli >= 100 else "RECOVERY"

        out.append({
            "date": date_i,
            "cli": round(cli, 2),
            "phase": phase,
            "ret_12m": round(ret_12m, 2) if ret_12m is not None else None,
            "ret_3m":  round(ret_3m, 2)  if ret_3m  is not None else None,
            "ret_1m":  round(ret_1m, 2)  if ret_1m  is not None else None,
            "dist_200ma": round(dist_200ma, 2),
        })
    return out


def aggregate_global_history(history_by_country, country_weights, country_regions):
    """For each unique date across all countries, compute GDP-weighted
    aggregate (global_phase, global_avg_cli, expansion_breadth_pct,
    contraction_breadth_pct, phase_mix). Returns list of per-date dicts.

    Country histories may have slightly different date sets (different
    market holidays). We forward-fill per-country to a unified weekly grid."""
    # Collect all unique dates
    all_dates = set()
    for h in history_by_country.values():
        for pt in h:
            all_dates.add(pt["date"])
    sorted_dates = sorted(all_dates)
    if not sorted_dates:
        return []

    # Per-country lookup: date → point. Plus track latest seen point for
    # forward-fill behavior on missing dates.
    country_lookups = {iso3: {pt["date"]: pt for pt in h}
                        for iso3, h in history_by_country.items()}

    aggregate_series = []
    last_seen = {iso3: None for iso3 in country_lookups}
    for d in sorted_dates:
        global_mix = {"EXPANSION": 0.0, "AT_RISK": 0.0,
                       "RECESSION": 0.0, "RECOVERY": 0.0}
        weighted_cli_sum = 0.0
        classified_weight = 0.0
        total_weight = 0.0

        for iso3, lookup in country_lookups.items():
            point = lookup.get(d) or last_seen[iso3]
            if point:
                last_seen[iso3] = point
                phase = point.get("phase")
                cli = point.get("cli")
                w = country_weights.get(iso3, 0)
                total_weight += w
                if phase and phase != "UNKNOWN":
                    global_mix[phase] = global_mix.get(phase, 0) + w
                if cli is not None and phase != "UNKNOWN":
                    weighted_cli_sum += cli * w
                    classified_weight += w

        if classified_weight <= 0:
            continue

        # Phase mix as % of classified weight
        phase_mix_pct = {p: round(global_mix[p] / classified_weight * 100, 2)
                          for p in global_mix}
        avg_cli = round(weighted_cli_sum / classified_weight, 2)
        exp_norm = phase_mix_pct["EXPANSION"]
        rec_norm = phase_mix_pct["RECOVERY"]
        ar_norm = phase_mix_pct["AT_RISK"]
        recession_norm = phase_mix_pct["RECESSION"]
        expansion_breadth = exp_norm + rec_norm
        contraction_breadth = ar_norm + recession_norm

        if expansion_breadth > 60:
            global_phase = "GLOBAL_EXPANSION"
        elif contraction_breadth > 60:
            global_phase = "GLOBAL_CONTRACTION"
        elif rec_norm > 35:
            global_phase = "GLOBAL_RECOVERY"
        elif ar_norm > 35:
            global_phase = "GLOBAL_PEAKING"
        else:
            global_phase = "MIXED"

        aggregate_series.append({
            "date": d,
            "global_phase": global_phase,
            "global_avg_cli": avg_cli,
            "phase_mix_pct": phase_mix_pct,
            "expansion_breadth_pct": round(expansion_breadth, 2),
            "contraction_breadth_pct": round(contraction_breadth, 2),
            "classified_weight": round(classified_weight, 1),
        })
    return aggregate_series


def detect_transitions(aggregate_series, min_dwell=3):
    """Detect confirmed global-phase transitions in the aggregate history.

    A transition is recorded only when the new phase PERSISTS for at least
    `min_dwell` consecutive weekly points — otherwise it's treated as noise
    (e.g. one-week flip back to MIXED before settling). The recorded date
    is the FIRST week of the new phase.

    Returns list of dicts:
      { date, from_phase, to_phase, cli_at_transition,
        breadth_at_transition, weeks_persisted }
    where weeks_persisted = how many consecutive weeks the new phase held
    (until the next transition or end of series)."""
    if not aggregate_series or len(aggregate_series) < min_dwell:
        return []

    transitions = []
    confirmed_phase = aggregate_series[0]["global_phase"]
    i = 1
    while i < len(aggregate_series):
        curr = aggregate_series[i]
        if curr["global_phase"] != confirmed_phase:
            new_phase = curr["global_phase"]
            persists = 1
            j = i + 1
            while j < len(aggregate_series) and aggregate_series[j]["global_phase"] == new_phase:
                persists += 1
                j += 1
            if persists >= min_dwell:
                transitions.append({
                    "date": curr["date"],
                    "from_phase": confirmed_phase,
                    "to_phase": new_phase,
                    "cli_at_transition": curr["global_avg_cli"],
                    "breadth_at_transition": curr.get("expansion_breadth_pct"),
                    "contraction_at_transition": curr.get("contraction_breadth_pct"),
                    "weeks_persisted": persists,
                })
                confirmed_phase = new_phase
                i = j
                continue
        i += 1
    return transitions


# ════════════════════════════════════════════════════════════════════════
# Cross-correlation lead/lag analysis
# ════════════════════════════════════════════════════════════════════════
def pearson_at_lag(x_series, y_series, lag):
    """Pearson correlation between x and y where y is shifted by `lag` weeks.

    Convention: positive lag means x LEADS y by `lag` weeks.
      lag > 0  →  corr(x[t], y[t+lag])    pair x[:n-lag] with y[lag:]
      lag < 0  →  corr(x[t+|lag|], y[t])  pair x[|lag|:] with y[:n-|lag|]
      lag = 0  →  standard correlation
    Returns None if insufficient overlap (<10 paired points)."""
    n = len(x_series)
    if n != len(y_series) or n < 10:
        return None
    if lag > 0:
        x_use = x_series[:n - lag]
        y_use = y_series[lag:]
    elif lag < 0:
        L = -lag
        x_use = x_series[L:]
        y_use = y_series[:n - L]
    else:
        x_use = x_series
        y_use = y_series
    if len(x_use) < 10 or len(x_use) != len(y_use):
        return None
    m = len(x_use)
    mean_x = sum(x_use) / m
    mean_y = sum(y_use) / m
    num = sum((x_use[i] - mean_x) * (y_use[i] - mean_y) for i in range(m))
    var_x = sum((x_use[i] - mean_x) ** 2 for i in range(m))
    var_y = sum((y_use[i] - mean_y) ** 2 for i in range(m))
    if var_x <= 0 or var_y <= 0:
        return None
    return num / (var_x * var_y) ** 0.5


def compute_lead_lag(iso3, history_by_country, aggregate_series, max_lag_weeks=26):
    """Cross-correlate one country's CLI series against the global aggregate.

    Returns dict with:
      iso3, country_name, region
      peak_lag_weeks   : lag (positive = country leads, negative = lags)
      peak_correlation : Pearson at peak_lag (always in [-1, 1])
      curve_lags / curve_corrs : full sweep across [-max_lag, +max_lag]
      n_overlap        : how many paired weekly points contributed
      caveat           : flag if peak is at boundary (lead/lag may be longer)
    Returns None if country has too little history to correlate."""
    if iso3 not in history_by_country:
        return None
    country_info = history_by_country[iso3]
    country_h = country_info.get("history") or []
    if len(country_h) < max_lag_weeks * 2 + 10:
        return None

    # Forward-fill country CLI onto the global aggregate's weekly grid
    country_lookup = {pt["date"]: pt["cli"] for pt in country_h}
    last_seen = None
    x = []   # country
    y = []   # global
    for agg_pt in aggregate_series:
        d = agg_pt["date"]
        if d in country_lookup:
            last_seen = country_lookup[d]
        if last_seen is not None:
            x.append(last_seen)
            y.append(agg_pt["global_avg_cli"])

    if len(x) < max_lag_weeks * 2 + 10:
        return None

    # Scan correlation across the lag window
    best_lag = 0
    best_corr = -2.0
    curve_lags = []
    curve_corrs = []
    for lag in range(-max_lag_weeks, max_lag_weeks + 1):
        c = pearson_at_lag(x, y, lag)
        if c is None:
            continue
        curve_lags.append(lag)
        curve_corrs.append(round(c, 3))
        if c > best_corr:
            best_corr = c
            best_lag = lag

    if best_corr < -1.5:
        return None

    boundary_flag = (best_lag == -max_lag_weeks or best_lag == max_lag_weeks)
    return {
        "iso3": iso3,
        "country_name": country_info.get("country_name"),
        "region": country_info.get("region"),
        "gdp_weight": country_info.get("gdp_weight"),
        "peak_lag_weeks": best_lag,
        "peak_correlation": round(best_corr, 3),
        "curve_lags": curve_lags,
        "curve_corrs": curve_corrs,
        "n_overlap": len(x),
        "boundary_flag": boundary_flag,
    }


def rank_lead_lag(history_by_country, aggregate_series, max_lag_weeks=26):
    """Compute lead/lag for every country, return list sorted by peak_lag descending
    (most-leading at top, most-lagging at bottom)."""
    ranking = []
    for iso3 in history_by_country.keys():
        ll = compute_lead_lag(iso3, history_by_country, aggregate_series, max_lag_weeks)
        if ll:
            ranking.append(ll)
    ranking.sort(key=lambda r: (-r["peak_lag_weeks"], -r["peak_correlation"]))
    return ranking


# ════════════════════════════════════════════════════════════════════════
# Phase-conditional forward-return distribution analysis
# ════════════════════════════════════════════════════════════════════════
def compute_histogram_bins(returns, n_bins=20):
    """Equal-width histogram bins. Returns list of {lo, hi, center, count}."""
    if not returns:
        return []
    lo = min(returns)
    hi = max(returns)
    if hi - lo < 1e-9:
        return [{"lo": round(lo, 2), "hi": round(hi, 2),
                  "center": round(lo, 2), "count": len(returns)}]
    width = (hi - lo) / n_bins
    bins = []
    for i in range(n_bins):
        b_lo = lo + i * width
        b_hi = lo + (i + 1) * width
        if i == n_bins - 1:
            count = sum(1 for r in returns if b_lo <= r <= b_hi)
        else:
            count = sum(1 for r in returns if b_lo <= r < b_hi)
        bins.append({
            "lo": round(b_lo, 2),
            "hi": round(b_hi, 2),
            "center": round((b_lo + b_hi) / 2, 2),
            "count": count,
        })
    return bins


def compute_distribution_stats(returns):
    """Summary stats + histogram for a list of returns (%)."""
    if not returns:
        return {"n": 0}
    sorted_returns = sorted(returns)
    n = len(returns)
    mean = sum(returns) / n
    var = sum((r - mean) ** 2 for r in returns) / n
    stdev = var ** 0.5

    def percentile(p):
        idx = int(round(p / 100.0 * (n - 1)))
        idx = max(0, min(n - 1, idx))
        return sorted_returns[idx]

    hit_rate = sum(1 for r in returns if r > 0) / n
    return {
        "n": n,
        "mean": round(mean, 2),
        "median": round(percentile(50), 2),
        "stdev": round(stdev, 2),
        "p10": round(percentile(10), 2),
        "p25": round(percentile(25), 2),
        "p75": round(percentile(75), 2),
        "p90": round(percentile(90), 2),
        "min": round(min(returns), 2),
        "max": round(max(returns), 2),
        "hit_rate": round(hit_rate, 3),
        "bins": compute_histogram_bins(returns, n_bins=20),
    }


def compute_phase_returns(prices, history, forward_days=63):
    """For each weekly history point, compute the forward N-trading-day equity
    return and bucket by the phase active at that time.

    Returns dict: phase -> distribution stats.
    Default forward window: 63 trading days ≈ 3 months."""
    PHASES = ["EXPANSION", "AT_RISK", "RECESSION", "RECOVERY"]
    if not prices or not history:
        return {p: {"n": 0} for p in PHASES}

    # Build date → daily-index lookup
    date_to_idx = {d: i for i, (d, _) in enumerate(prices)}
    buckets = {p: [] for p in PHASES}
    n_prices = len(prices)

    for pt in history:
        phase = pt.get("phase")
        if phase not in buckets:
            continue
        idx = date_to_idx.get(pt["date"])
        if idx is None or idx + forward_days >= n_prices:
            continue
        p_now = prices[idx][1]
        p_fwd = prices[idx + forward_days][1]
        if p_now <= 0:
            continue
        fwd_ret = (p_fwd / p_now - 1.0) * 100
        buckets[phase].append(fwd_ret)

    return {phase: compute_distribution_stats(returns)
            for phase, returns in buckets.items()}


def build_phase_summary(phase_returns_by_country, country_meta):
    """For each phase, return a list of countries sorted by mean forward return.
    Each entry: {iso3, country_name, region, gdp_weight, n, mean, median, hit_rate, p10, p90}.
    Skips entries with n < 5 (insufficient sample)."""
    PHASES = ["EXPANSION", "AT_RISK", "RECESSION", "RECOVERY"]
    summary = {p: [] for p in PHASES}
    for iso3, by_phase in phase_returns_by_country.items():
        meta = country_meta.get(iso3, {})
        for phase in PHASES:
            stats = by_phase.get(phase, {})
            n = stats.get("n", 0)
            if n < 5:
                continue
            summary[phase].append({
                "iso3": iso3,
                "country_name": meta.get("name"),
                "region": meta.get("region"),
                "gdp_weight": meta.get("weight"),
                "n": n,
                "mean": stats.get("mean"),
                "median": stats.get("median"),
                "hit_rate": stats.get("hit_rate"),
                "p10": stats.get("p10"),
                "p90": stats.get("p90"),
                "stdev": stats.get("stdev"),
            })
        # Sort each phase by mean descending (best forward return first)
    for phase in PHASES:
        summary[phase].sort(key=lambda r: -(r["mean"] or -999))
    return summary


# ════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════
def equity_monthly_composite(prices, grid):
    """The v2 equity composite (0.35*ret12m + 0.25*dist200 + 0.25*ret3m + 0.15*ret1m)
    evaluated at each month-end of the grid -> list aligned to grid (None where
    < 252 bars of history)."""
    out = [None] * len(grid)
    if not prices or len(prices) < 260:
        return out
    gi = {m: i for i, m in enumerate(grid)}
    last_idx = {}
    for i, (d, _) in enumerate(prices):
        last_idx[d[:7]] = i
    for mo, i in last_idx.items():
        g = gi.get(mo)
        if g is None or i < 252:
            continue
        p = prices[i][1]
        r12 = (p / prices[i - 252][1] - 1) * 100 if prices[i - 252][1] > 0 else None
        r3 = (p / prices[i - 63][1] - 1) * 100 if prices[i - 63][1] > 0 else None
        r1 = (p / prices[i - 21][1] - 1) * 100 if prices[i - 21][1] > 0 else None
        ma = sum(pp for _, pp in prices[i - 199:i + 1]) / 200
        d200 = (p / ma - 1) * 100 if ma > 0 else 0.0
        comps = [(r12, 0.35), (d200, 0.25), (r3, 0.25), (r1, 0.15)]
        comps = [(v, w) for v, w in comps if v is not None]
        wt = sum(w for _, w in comps)
        out[g] = round(sum(v * w for v, w in comps) / wt, 3) if wt else None
    return out


def _load_features():
    try:
        o = S3.get_object(Bucket=BUCKET, Key=FEATURES_KEY)
        body = o["Body"].read()
        if body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)
        doc = json.loads(body)
        age_h = (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600
        doc["_age_h"] = round(age_h, 1)
        return doc
    except Exception as e:  # noqa: BLE001
        print(f"[gbc] feature store unavailable: {str(e)[:100]}")
        return None


def _load_previous_output():
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read())
    except Exception as e:  # noqa: BLE001
        print(f"[gbc] previous output unavailable: {str(e)[:80]}")
        return None


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[gbc] v{ENGINE_VERSION} start, {len(COUNTRY_MAP)} countries (equity-momentum-based)")
    previous_output = _load_previous_output()
    lane = PolygonETFLane()

    by_country = {}
    prices_by_country = {}    # iso3 → list of (date, close)  for history pass
    fresh_count = 0
    sources_used = defaultdict(int)
    supplement_log = {}
    for iso3, yahoo_sym, region, name, weight, iso2 in COUNTRY_MAP:
        prices, source, attempts, degraded = resolve_prices(iso3, yahoo_sym, lane)
        prices_by_country[iso3] = prices
        supplement = None
        if iso3 in FRED_SUPPLEMENT:
            supplement = standardise_supplement(FRED_SUPPLEMENT[iso3], fred_recent(FRED_SUPPLEMENT[iso3]))
            supplement_log[iso3] = supplement

        phase_info = compute_cli_from_prices(prices, supplement=supplement)
        if degraded and phase_info.get("phase") != "UNKNOWN":
            phase_info["reason"] = degraded
        if not prices and attempts:
            phase_info["reason"] = degraded or "no candidate cleared the acceptance gate"

        latest_date = phase_info.get("latest_date")
        months_old, days_old = 999, None
        if latest_date:
            try:
                d = datetime.strptime(latest_date, "%Y-%m-%d").date()
                days_old = (_today() - d).days
                months_old = max(0, days_old // 30)
                if months_old <= 3:
                    fresh_count += 1
            except Exception:
                pass
        source_kind = (source or "none").split(":", 1)[0]
        sources_used[source_kind if not degraded else "degraded"] += 1

        by_country[iso3] = {
            "iso3": iso3,
            "iso2": iso2,
            "yahoo_symbol": (source.split(":", 1)[1] if source and source.startswith("yahoo:") else source),
            "yahoo_symbol_primary": yahoo_sym,
            "source": source,
            "source_kind": source_kind,
            "source_attempts": attempts,
            "source_degraded": degraded,
            "country_name": name,
            "region": region,
            "gdp_weight": weight,
            "months_stale": months_old,
            "days_stale": days_old,
            **phase_info,
        }
        print(f"[gbc] {iso3:<4} {(phase_info.get('phase') or 'UNK'):<10} "
              f"CLI={phase_info.get('cli_level')} 3m={phase_info.get('three_month_change')} "
              f"latest={latest_date} ({days_old}d) src={source} "
              f"{'DEGRADED: ' + degraded if degraded else ''}")
    print(f"[gbc] sources used: {dict(sources_used)} · supplements: "
          f"{ {k: v.get('status') for k, v in supplement_log.items()} }")

    # ── v3 MULTI-PILLAR COMPOSITE ─────────────────────────────────────────
    features = _load_features() if CC is not None else None
    composite_summary = {"available": False}
    composite_hist = {}
    grid = []
    _fage = features.get("_age_h") if features else None
    if features and (_fage if _fage is not None else 999) <= FEATURES_MAX_AGE_H:
        grid = features["grid"]["months"]
        fc = features.get("countries") or {}
        n_multi, n_thin, pillar_counts = 0, 0, defaultdict(int)
        for iso3, row in by_country.items():
            eq = equity_monthly_composite(prices_by_country.get(iso3) or [], grid)
            built = CC.build_country((fc.get(iso3) or {}).get("features"), grid, equity_monthly=eq)
            nc = built.get("nowcast")
            row["equity_cli_level"] = row.get("cli_level")
            row["equity_phase"] = row.get("phase")
            row["equity_trend"] = row.get("trend")
            if nc and nc["basis"] == "multi-pillar":
                row["cli_level"] = nc["cli"]
                row["phase"] = nc["phase"]
                row["trend"] = nc["trend"]
                row["phase_basis"] = "multi-pillar"
                n_multi += 1
                for p in nc["pillars"]:
                    pillar_counts[p] += 1
            else:
                row["phase_basis"] = "equity-only" if not nc else "thin"
                n_thin += 1
            row["composite"] = nc
            row["components"] = built.get("components")
            composite_hist[iso3] = built.get("history") or []
            print(f"[gbc-v3] {iso3} {row['phase_basis']:<12} composite cli={row.get('cli_level')} phase={row.get('phase')} "
                  f"pillars={list((nc or {}).get('pillars', {}).keys())} conf={(nc or {}).get('confidence')}")
        composite_summary = {"available": True, "features_generated_at": features.get("generated_at"),
                             "features_age_h": features.get("_age_h"), "features_version": features.get("version"),
                             "countries_multi_pillar": n_multi, "countries_thin_or_equity": n_thin,
                             "pillar_counts": dict(pillar_counts), "pillar_weights": CC.PILLAR_WEIGHTS,
                             "method": ("own-history z (120 obs, min 36, clip 3) per feature, sign-adjusted; pillar = mean; "
                                        "composite = weighted mean over present pillars; CLI = 100 + 20*tanh(z/2)")}
    else:
        for row in by_country.values():
            row["phase_basis"] = "equity-only"
        composite_summary = {"available": False, "reason": ("feature store missing" if not features else
                             f"feature store stale ({features.get('_age_h')}h > {FEATURES_MAX_AGE_H}h)")}
    print(f"[gbc-v3] composite summary: {json.dumps(composite_summary)[:300]}")

    agg = aggregate(by_country)
    interp = interpret_global_cycle(agg, by_country)

    # ── PHYSICAL CONFIRMATION (ops 3842) — strictly additive, `phase` untouched
    try:
        _pmeta = {iso: {"name": r.get("country_name")}
                  for iso, r in by_country.items() if isinstance(r, dict)}
        _phys, _unmapped, _pw_gen = load_port_physical(_pmeta)
        _phys_carried = None
        if not _phys and previous_output:
            # ops 5095: portwatch fills `ports` inside a try/except around the IMF
            # ArcGIS call, so an upstream hiccup ships an EMPTY list and every
            # country used to flip to UNCONFIRMED. Carry the previous run's
            # physical read forward WITH provenance instead of erasing it.
            _prev = previous_output.get("physical_confirmation") or {}
            if (_prev.get("countries_with_ports") or 0) > 0:
                _pw_gen = _prev.get("portwatch_generated_at")
                _phys_carried = previous_output.get("generated_at")
                for iso, row in by_country.items():
                    pb = ((previous_output.get("by_country") or {}).get(iso) or {}).get("physical") or {}
                    if pb.get("n_ports"):
                        _phys[iso] = {k: pb[k] for k in ("n_ports", "median_yoy_pct", "min_yoy_pct",
                                                          "worst_port", "n_disrupted") if k in pb}
                print(f"[physical] portwatch ports list empty — carried {len(_phys)} "
                      f"countries from previous run {_phys_carried}")
        _cs = {"CONFIRMED": 0, "DIVERGENT": 0, "UNCONFIRMED": 0}
        for iso, row in by_country.items():
            if not isinstance(row, dict):
                continue
            blk = confirm_phase(row.get("phase"), _phys.get(iso.upper()))
            if _phys_carried and blk.get("n_ports"):
                blk["stale"] = True
                blk["carried_from_run"] = _phys_carried
            row["physical"] = blk
            _cs[blk["state"]] = _cs.get(blk["state"], 0) + 1
        _phys_summary = {
            "source": "data/portwatch.json",
            "portwatch_generated_at": _pw_gen,
            "carried_from_previous_run": _phys_carried,
            "countries_with_ports": len(_phys),
            "counts": _cs,
            "unmapped_port_countries": _unmapped,
            "method": ("median YoY port throughput per country (>=2 ports). "
                       "CONFIRMED when physical direction agrees with the "
                       "equity-momentum phase, DIVERGENT when it contradicts."),
            "why": ("`phase` is derived from equity momentum, whose known failure "
                    "mode is firing in drawdowns that never become recessions. "
                    "Port volumes are physical and independent, so they "
                    "corroborate or contradict the label at the SOURCE — every "
                    "downstream consumer inherits the check instead of "
                    "re-deriving it."),
            "limits": ("Port throughput is a TRADE proxy: it under-reads "
                       "service-led and domestic-demand economies, and a "
                       "commodity exporter can show falling volumes for "
                       "supply-side reasons unrelated to its business cycle. "
                       "Corroboration, not proof."),
        }
        print(f"[physical] {len(_phys)} countries · {_cs}")
    except Exception as _pe:  # noqa: BLE001
        _phys_summary = {"error": str(_pe)[:200]}
        print(f"[physical] failed: {str(_pe)[:120]}")

    _sup_used = [k for k, v in supplement_log.items() if v.get("status") == "fresh"]
    global_comp_hist, calibration = [], {"ok": False, "reason": "composite unavailable"}
    if composite_summary.get("available") and composite_hist:
        weights = {iso3: w for iso3, _, _, _, w, _ in COUNTRY_MAP}
        global_comp_hist = CC.aggregate_history(composite_hist, weights, grid)
        # target: GDP-weighted industrial production y/y from the feature store
        target = {}
        fc = features.get("countries") or {}
        for i, mo in enumerate(grid):
            num = den = 0.0
            for iso3, w in weights.items():
                f = ((fc.get(iso3) or {}).get("features") or {}).get("ip_yoy")
                v = (f or {}).get("values", [None] * len(grid))[i] if f else None
                if v is not None:
                    num += w * v
                    den += w
            if den >= 40:                       # at least ~40% of world GDP reporting
                target[mo] = num / den
        calibration = CC.calibrate_downturn(global_comp_hist, target, grid) if global_comp_hist else calibration
        calibration["target_series_months"] = len(target)
        calibration["target_latest"] = max(target) if target else None
    output = {
        "schema_version": "3.0",
        "engine_version": ENGINE_VERSION,
        "engine_type": "synthetic_equity_momentum",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 1),
        "countries_with_fresh_data": fresh_count,
        "countries_total": len(COUNTRY_MAP),
        "sources_used": dict(sources_used),
        "supplements": supplement_log,
        "polygon_lane": {"loaded": lane.loaded, "sessions": lane.n_sessions,
                         "tickers_present": len(lane.series), "error": lane.error},
        "acceptance_gate": {"min_bars": MIN_BARS, "max_stale_days": MAX_STALE_DAYS},
        "composite": composite_summary,
        "downturn_probability_6m": calibration,
        "global_composite_latest": (global_comp_hist[-1] if global_comp_hist else None),
        "methodology": {
            "primary_source": "Yahoo Finance national equity indices (real-time, ~0-1 day stale)",
            "fallback_chain": ("index candidates -> equal-weight blue-chip basket (Yahoo) -> "
                               "US-listed country ETF from the polygon-full warehouse (own store) "
                               "-> degraded stale candidate (flagged) -> UNKNOWN; every candidate "
                               f"must clear >= {MIN_BARS} bars and a last bar <= {MAX_STALE_DAYS} days old"),
            "secondary_source": ("FRED OECD CCI/BCI for USA + CHN standardised against their own "
                                 "trailing history (z-score -> index points, 1z=1.5pt, clip +-3), used "
                                 f"only when <= {SUPPLEMENT_MAX_MONTHS} months old with >= "
                                 f"{SUPPLEMENT_MIN_HIST} prior obs; used this run for: {_sup_used or 'none'}"),
            "rationale": ("OECD's Composite Leading Indicator series on FRED stopped "
                          "updating in Jan 2024 (28+ months stale). Equity-market "
                          "momentum is a well-established leading indicator (typically "
                          "leads economic activity by 6-12 months) and is updated daily, "
                          "so it serves as a reliable real-time proxy for the global "
                          "business cycle."),
            "composite_formula": ("CLI = 100 + 20*tanh(0.025*composite_pct) — slope 0.5 at 100 "
                                   "(the v2.0 linear map), soft-bounded (80,120). composite_pct = "
                                   "0.35*ret12m + 0.25*dist_200ma + 0.25*ret3m + 0.15*ret1m. "
                                   "Fresh OECD CCI/BCI standardised vs own history (z*1.5 pts, "
                                   "clipped +-3, x5) blended 75/25 for USA+CHN."),
            "phase_classification": {
                "EXPANSION": "CLI >= 100 AND 3-month return rising",
                "AT_RISK":   "CLI >= 100 AND 3-month return falling (peak passing)",
                "RECESSION": "CLI < 100 AND 3-month return falling (deepening)",
                "RECOVERY":  "CLI < 100 AND 3-month return rising (trough passed)",
            },
            "country_count": len(COUNTRY_MAP),
        },
        "by_country": by_country,
        "physical_confirmation": _phys_summary,
        "aggregate": agg,
        "interpretation": interp,
    }

    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=3600, s-maxage=3600",
    )

    if composite_summary.get("available"):
        comp_doc = {"schema_version": "3.0", "engine_version": ENGINE_VERSION, "generated_at": output["generated_at"],
                    "frequency": "monthly", "grid": {"start": grid[0], "end": grid[-1]},
                    "features_generated_at": features.get("generated_at"), "pillar_weights": CC.PILLAR_WEIGHTS,
                    "by_country": {iso3: {"country_name": by_country[iso3].get("country_name"), "region": by_country[iso3].get("region"),
                                          "gdp_weight": by_country[iso3].get("gdp_weight"), "n_points": len(h),
                                          "first_period": h[0]["period"] if h else None, "last_period": h[-1]["period"] if h else None,
                                          "history": h, "nowcast": by_country[iso3].get("composite")}
                                   for iso3, h in composite_hist.items()},
                    "global": global_comp_hist, "downturn_probability_6m": calibration,
                    "methodology": composite_summary.get("method")}
        S3.put_object(Bucket=BUCKET, Key=COMPOSITE_HISTORY_KEY, Body=json.dumps(comp_doc, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=3600, s-maxage=3600")
        print(f"[gbc-v3] composite history written: {len(composite_hist)} countries, global points {len(global_comp_hist)}, "
              f"downturn p={calibration.get('probability_now')} auc={calibration.get('in_sample_auc')}")

    # ─────────────────────────────────────────────────────────────────────
    # SECOND PASS: weekly rolling CLI history for each country
    # Used by /global-cycle/ history page. Separate S3 key, separate
    # consumers — schema may evolve independently of the live JSON.
    # ─────────────────────────────────────────────────────────────────────
    history_started = time.time()
    print(f"[gbc-history] computing weekly history for {len(prices_by_country)} countries")
    history_by_country = {}
    country_weights = {}
    country_regions = {}
    for iso3, yahoo_sym, region, name, weight, iso2 in COUNTRY_MAP:
        prices = prices_by_country.get(iso3, [])
        if not prices:
            continue
        country_weights[iso3] = weight
        country_regions[iso3] = region
        series = compute_history_series(prices, frequency_days=5)
        if series:
            history_by_country[iso3] = {
                "iso3": iso3,
                "iso2": iso2,
                "country_name": name,
                "region": region,
                "gdp_weight": weight,
                "yahoo_symbol": by_country.get(iso3, {}).get("yahoo_symbol"),
                "n_points": len(series),
                "first_date": series[0]["date"] if series else None,
                "last_date": series[-1]["date"] if series else None,
                "history": series,
            }
            print(f"[gbc-history] {iso3} {len(series)} weekly points "
                  f"({series[0]['date']} → {series[-1]['date']})")

    # GDP-weighted aggregate across countries by date
    aggregate_history = aggregate_global_history(
        {iso3: info["history"] for iso3, info in history_by_country.items()},
        country_weights, country_regions
    )
    print(f"[gbc-history] aggregate has {len(aggregate_history)} dates "
          f"({aggregate_history[0]['date'] if aggregate_history else '—'} → "
          f"{aggregate_history[-1]['date'] if aggregate_history else '—'})")

    # Detect confirmed phase transitions (≥5-week dwell filter to reduce noise)
    transitions = detect_transitions(aggregate_history, min_dwell=5)
    print(f"[gbc-history] detected {len(transitions)} confirmed transitions")
    for tr in transitions:
        print(f"[gbc-history]   {tr['date']} {tr['from_phase']} → {tr['to_phase']} "
              f"CLI {tr['cli_at_transition']} · persisted {tr['weeks_persisted']}w")

    # Cross-correlation lead/lag ranking
    lead_lag_started = time.time()
    lead_lag_ranking = rank_lead_lag(history_by_country, aggregate_history, max_lag_weeks=26)
    print(f"[gbc-history] computed lead/lag for {len(lead_lag_ranking)} countries "
          f"in {time.time() - lead_lag_started:.2f}s")
    for i, r in enumerate(lead_lag_ranking[:10]):
        print(f"[gbc-history]   #{i+1} {r['iso3']} lag={r['peak_lag_weeks']:+}w "
              f"corr={r['peak_correlation']}")

    # Also attach per-country into history_by_country for easy frontend lookup
    for r in lead_lag_ranking:
        if r["iso3"] in history_by_country:
            history_by_country[r["iso3"]]["lead_lag"] = {
                "peak_lag_weeks": r["peak_lag_weeks"],
                "peak_correlation": r["peak_correlation"],
                "boundary_flag": r["boundary_flag"],
            }

    # Phase-conditional forward return distributions per country
    pcr_started = time.time()
    country_meta = {}
    phase_returns_by_country = {}
    for iso3, yahoo_sym, region, name, weight, iso2 in COUNTRY_MAP:
        country_meta[iso3] = {"name": name, "region": region, "weight": weight, "iso2": iso2}
        if iso3 not in history_by_country:
            continue
        prices = prices_by_country.get(iso3) or []
        hist = history_by_country[iso3].get("history") or []
        phase_returns_by_country[iso3] = compute_phase_returns(prices, hist, forward_days=63)

    phase_summary = build_phase_summary(phase_returns_by_country, country_meta)
    print(f"[gbc-history] computed phase-conditional returns for "
          f"{len(phase_returns_by_country)} countries in {time.time() - pcr_started:.2f}s")
    for phase, top in phase_summary.items():
        if top:
            t1 = top[0]
            print(f"[gbc-history]   {phase}: top → {t1['iso3']} "
                  f"mean {t1['mean']}% hit {t1['hit_rate']} n={t1['n']}")

    # Also attach per-country into history_by_country for the per-country drilldown view
    for iso3, by_phase in phase_returns_by_country.items():
        if iso3 in history_by_country:
            history_by_country[iso3]["phase_returns"] = by_phase

    history_output = {
        "schema_version": "2.4",
        "engine_version": ENGINE_VERSION,
        "cli_transform": "soft_clip_tanh_v2.1",
        "engine_type": "synthetic_equity_momentum_history",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "frequency": "weekly_5d",
        "history_elapsed_sec": round(time.time() - history_started, 1),
        "countries_count": len(history_by_country),
        "transitions_count": len(transitions),
        "lead_lag_count": len(lead_lag_ranking),
        "lead_lag_max_lag_weeks": 26,
        "lead_lag_methodology": (
            "For each country, compute Pearson correlation between its weekly CLI "
            "and the GDP-weighted global aggregate CLI across lags in [-26, +26] weeks. "
            "Positive lag = country leads global (its CLI moves first); negative = lags. "
            "Each country is itself a component of the aggregate (USA ~25% weight), "
            "which creates artificial self-correlation at lag 0 — interpret near-zero "
            "lags with weight in mind. boundary_flag=true means peak was at edge of "
            "scan window; true lead/lag may be longer than ±26 weeks."
        ),
        "phase_returns_methodology": (
            "For each country at each weekly history point, compute the forward "
            "63-trading-day (~3 month) total return of the equity index and bucket "
            "by the phase active at that historical moment. Each phase therefore has "
            "a distribution of forward returns conditional on the cycle state. "
            "CAVEAT: weekly sampling with 63-day forward windows creates overlapping "
            "observations — sample means are unbiased but standard deviations and "
            "hit-rates are optimistic relative to non-overlapping draws. Phases with "
            "n<5 are excluded from the summary."
        ),
        "by_country": history_by_country,
        "aggregate": aggregate_history,
        "transitions": transitions,
        "lead_lag_ranking": lead_lag_ranking,
        "phase_summary": phase_summary,
    }

    S3.put_object(
        Bucket=BUCKET, Key=HISTORY_KEY,
        Body=json.dumps(history_output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=3600, s-maxage=3600",
    )

    print(f"[gbc] done. global_phase={agg['global_phase']} avg_cli={agg['global_avg_cli']} "
          f"fresh={fresh_count}/{len(COUNTRY_MAP)}")

    return {"statusCode": 200, "body": json.dumps({
        "engine_version": ENGINE_VERSION,
        "composite": {k: composite_summary.get(k) for k in ("available", "countries_multi_pillar", "pillar_counts")},
        "downturn_probability_6m": calibration.get("probability_now"),
        "global_phase": agg["global_phase"],
        "global_avg_cli": agg["global_avg_cli"],
        "n_countries": len(by_country),
        "fresh_count": fresh_count,
        "sources_used": dict(sources_used),
        "elapsed_sec": output["elapsed_sec"],
    })}
