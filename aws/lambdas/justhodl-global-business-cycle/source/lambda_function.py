"""justhodl-global-business-cycle  v2.0  (real-time, equity-momentum-based)
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

Sources:
  • Yahoo Finance chart API (free, real-time)  https://query1.finance.yahoo.com
  • FRED (where fresh, supplementary)            https://api.stlouisfed.org

Author: JustHodl.AI
"""
import json
import os
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from collections import defaultdict
import boto3

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/global-business-cycle.json"

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
                date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
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


# Per-country fallback symbols for known-flaky markets
SYMBOL_FALLBACKS = {
    "POL": ["WIG20.WA", "^WIG"],          # if EPOL fails, try Warsaw direct
    "CZE": ["^PX", "PXTR.PR", "CEZP.PR", "KOMB.PR"],  # try generic ^PX, then ČEZ utility, then Komerční banka
    "HUN": ["^BUX", "OTP.BD"],             # if BUX.BD fails, try generic ^BUX or OTP Bank as proxy
    "CHL": ["^IPSA", "^SPCLXIPSA"],       # if ECH fails, try Santiago direct
}


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

    # Blend with supplement (FRED CCI/BCI) if provided (75/25 blend)
    if supplement and supplement.get("value") is not None:
        sup_val = supplement["value"]
        composite_pct = composite_pct * 0.75 + sup_val * 10 * 0.25

    # Map composite percentage into CLI-style 0-200 score centered at 100
    cli_level = 100 + composite_pct * 0.5
    cli_level = max(80, min(120, cli_level))

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
    }


# ════════════════════════════════════════════════════════════════════════
# Regional + global aggregation (unchanged from v1)
# ════════════════════════════════════════════════════════════════════════
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
        cli = 100 + composite_pct * 0.5
        cli = max(80, min(120, cli))

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
# Main
# ════════════════════════════════════════════════════════════════════════
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[gbc] v2.0 start, {len(COUNTRY_MAP)} countries (equity-momentum-based)")

    by_country = {}
    prices_by_country = {}    # iso3 → list of (date, close)  for history pass
    fresh_count = 0
    for iso3, yahoo_sym, region, name, weight, iso2 in COUNTRY_MAP:
        fallbacks = SYMBOL_FALLBACKS.get(iso3, [])
        prices, used_symbol = yahoo_chart(yahoo_sym, range_param="5y",
                                            fallback_symbols=fallbacks)
        prices_by_country[iso3] = prices
        supplement = None
        if iso3 in FRED_SUPPLEMENT:
            supplement = fred_latest(FRED_SUPPLEMENT[iso3])

        phase_info = compute_cli_from_prices(prices, supplement=supplement)

        latest_date = phase_info.get("latest_date")
        months_old = 999
        if latest_date:
            try:
                d = datetime.strptime(latest_date, "%Y-%m-%d")
                now = datetime.utcnow()
                months_old = (now.year - d.year) * 12 + (now.month - d.month)
                if months_old <= 3:
                    fresh_count += 1
            except Exception:
                pass

        by_country[iso3] = {
            "iso3": iso3,
            "iso2": iso2,
            "yahoo_symbol": used_symbol,
            "yahoo_symbol_primary": yahoo_sym,
            "country_name": name,
            "region": region,
            "gdp_weight": weight,
            "months_stale": months_old,
            **phase_info,
        }
        print(f"[gbc] {iso3:<4} {(phase_info.get('phase') or 'UNK'):<10} "
              f"CLI={phase_info.get('cli_level')} 3m={phase_info.get('three_month_change')} "
              f"latest={latest_date} ({months_old}mo)")

    agg = aggregate(by_country)
    interp = interpret_global_cycle(agg, by_country)

    output = {
        "schema_version": "2.0",
        "engine_type": "synthetic_equity_momentum",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 1),
        "countries_with_fresh_data": fresh_count,
        "countries_total": len(COUNTRY_MAP),
        "methodology": {
            "primary_source": "Yahoo Finance equity indices (real-time, ~0-1 day stale)",
            "secondary_source": "FRED OECD CCI/BCI for USA + CHN (~1-2 mo stale)",
            "rationale": ("OECD's Composite Leading Indicator series on FRED stopped "
                          "updating in Jan 2024 (28+ months stale). Equity-market "
                          "momentum is a well-established leading indicator (typically "
                          "leads economic activity by 6-12 months) and is updated daily, "
                          "so it serves as a reliable real-time proxy for the global "
                          "business cycle."),
            "composite_formula": ("CLI = 100 + composite_pct * 0.5, capped [80,120]. "
                                   "composite_pct = 0.35*ret12m + 0.25*dist_200ma + "
                                   "0.25*ret3m + 0.15*ret1m. For USA+CHN blended 75/25 "
                                   "with OECD CCI/BCI."),
            "phase_classification": {
                "EXPANSION": "CLI >= 100 AND 3-month return rising",
                "AT_RISK":   "CLI >= 100 AND 3-month return falling (peak passing)",
                "RECESSION": "CLI < 100 AND 3-month return falling (deepening)",
                "RECOVERY":  "CLI < 100 AND 3-month return rising (trough passed)",
            },
            "country_count": len(COUNTRY_MAP),
        },
        "by_country": by_country,
        "aggregate": agg,
        "interpretation": interp,
    }

    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=3600, s-maxage=3600",
    )

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

    # Detect confirmed phase transitions (≥3-week dwell filter)
    transitions = detect_transitions(aggregate_history, min_dwell=3)
    print(f"[gbc-history] detected {len(transitions)} confirmed transitions")
    for tr in transitions:
        print(f"[gbc-history]   {tr['date']} {tr['from_phase']} → {tr['to_phase']} "
              f"CLI {tr['cli_at_transition']} · persisted {tr['weeks_persisted']}w")

    history_output = {
        "schema_version": "2.1",
        "engine_type": "synthetic_equity_momentum_history",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "frequency": "weekly_5d",
        "history_elapsed_sec": round(time.time() - history_started, 1),
        "countries_count": len(history_by_country),
        "transitions_count": len(transitions),
        "by_country": history_by_country,
        "aggregate": aggregate_history,
        "transitions": transitions,
    }

    S3.put_object(
        Bucket=BUCKET, Key="data/global-business-cycle-history.json",
        Body=json.dumps(history_output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=3600, s-maxage=3600",
    )

    print(f"[gbc] done. global_phase={agg['global_phase']} avg_cli={agg['global_avg_cli']} "
          f"fresh={fresh_count}/{len(COUNTRY_MAP)}")

    return {"statusCode": 200, "body": json.dumps({
        "global_phase": agg["global_phase"],
        "global_avg_cli": agg["global_avg_cli"],
        "n_countries": len(by_country),
        "fresh_count": fresh_count,
        "elapsed_sec": output["elapsed_sec"],
    })}
