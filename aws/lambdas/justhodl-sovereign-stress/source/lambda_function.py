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
VERSION = "2.1.0"
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
WGB_EXTRA = {"chile": "chile", "peru": "peru", "netherlands": "netherlands"}
WGB_REGION = {"south_korea": "Asia-Pacific", "singapore": "Asia-Pacific",
              "hong_kong": "Asia-Pacific", "taiwan": "Asia-Pacific",
              "chile": "Latin America", "peru": "Latin America",
              "netherlands": "Europe"}
COUNTRY_ETF = {"south_korea": "EWY", "singapore": "EWS", "hong_kong": "EWH",
               "taiwan": "EWT", "chile": "ECH", "peru": "EPU",
               "netherlands": "EWN", "germany": "EWG", "france": "EWQ",
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
                      "chile": "IRLTLT01CLM156N"}
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
