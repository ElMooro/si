"""justhodl-global-business-cycle
═══════════════════════════════════════════════════════════════════════
Pulls OECD Composite Leading Indicator (CLI) for 25+ major economies from
FRED. Classifies each country into a 4-phase business cycle (Expansion /
At Risk / Recession / Recovery) using the standard OECD framework:

  Phase            CLI level vs 100   6-month trend
  ──────────────────────────────────────────────────
  EXPANSION        ≥ 100              ↑ rising
  AT_RISK          ≥ 100              ↓ falling   (peak passed)
  RECESSION        < 100              ↓ falling   (deepening)
  RECOVERY         < 100              ↑ rising    (trough passed)

Aggregates to regional and global phase mix. Output baked into S3 JSON
at data/global-business-cycle.json. Schedule: daily (CLI is monthly data).

Khalid integration: phase mix feeds the Khalid Index, risk dashboard
recession subscore, allocator country tilts, morning brief, and ai-chat.

Author: JustHodl.AI
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from collections import defaultdict
import boto3

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/global-business-cycle.json"

S3 = boto3.client("s3", region_name="us-east-1")


# ════════════════════════════════════════════════════════════════════════
# COUNTRY MAP — ISO3 → (FRED CLI series ID, region, country name, weight)
# Weights ≈ global GDP share (rough; used for global aggregate calc)
# ════════════════════════════════════════════════════════════════════════
COUNTRY_MAP = [
    # iso3, fred_id, region,           country_name,      gdp_weight, iso2 (for flags)
    ("USA", "USALOLITONOSTSAM", "North America",  "United States",     25.0,  "US"),
    ("CHN", "CHNLOLITONOSTSAM", "Asia-Pacific",    "China",             18.0,  "CN"),
    ("JPN", "JPNLOLITONOSTSAM", "Asia-Pacific",    "Japan",              4.2,  "JP"),
    ("DEU", "DEULOLITONOSTSAM", "Europe",          "Germany",            4.0,  "DE"),
    ("IND", "INDLOLITONOSTSAM", "Asia-Pacific",    "India",              3.6,  "IN"),
    ("GBR", "GBRLOLITONOSTSAM", "Europe",          "United Kingdom",     3.3,  "GB"),
    ("FRA", "FRALOLITONOSTSAM", "Europe",          "France",             2.8,  "FR"),
    ("ITA", "ITALOLITONOSTSAM", "Europe",          "Italy",              2.1,  "IT"),
    ("CAN", "CANLOLITONOSTSAM", "North America",  "Canada",             2.0,  "CA"),
    ("BRA", "BRALOLITONOSTSAM", "Latin America",   "Brazil",             1.9,  "BR"),
    ("KOR", "KORLOLITONOSTSAM", "Asia-Pacific",    "South Korea",        1.6,  "KR"),
    ("AUS", "AUSLOLITONOSTSAM", "Asia-Pacific",    "Australia",          1.5,  "AU"),
    ("ESP", "ESPLOLITONOSTSAM", "Europe",          "Spain",              1.5,  "ES"),
    ("MEX", "MEXLOLITONOSTSAM", "Latin America",   "Mexico",             1.5,  "MX"),
    ("IDN", "IDNLOLITONOSTSAM", "Asia-Pacific",    "Indonesia",          1.3,  "ID"),
    ("NLD", "NLDLOLITONOSTSAM", "Europe",          "Netherlands",        1.1,  "NL"),
    ("TUR", "TURLOLITONOSTSAM", "Europe",          "Turkey",             1.0,  "TR"),
    ("CHE", "CHELOLITONOSTSAM", "Europe",          "Switzerland",        0.9,  "CH"),
    ("POL", "POLLOLITONOSTSAM", "Europe",          "Poland",             0.8,  "PL"),
    ("BEL", "BELLOLITONOSTSAM", "Europe",          "Belgium",            0.7,  "BE"),
    ("SWE", "SWELOLITONOSTSAM", "Europe",          "Sweden",             0.6,  "SE"),
    ("IRL", "IRLLOLITONOSTSAM", "Europe",          "Ireland",            0.6,  "IE"),
    ("AUT", "AUTLOLITONOSTSAM", "Europe",          "Austria",            0.6,  "AT"),
    ("NOR", "NORLOLITONOSTSAM", "Europe",          "Norway",             0.5,  "NO"),
    ("ZAF", "ZAFLOLITONOSTSAM", "Africa",          "South Africa",       0.5,  "ZA"),
    ("DNK", "DNKLOLITONOSTSAM", "Europe",          "Denmark",            0.4,  "DK"),
    ("FIN", "FINLOLITONOSTSAM", "Europe",          "Finland",            0.3,  "FI"),
    ("CZE", "CZELOLITONOSTSAM", "Europe",          "Czech Republic",     0.3,  "CZ"),
    ("HUN", "HUNLOLITONOSTSAM", "Europe",          "Hungary",            0.2,  "HU"),
    ("CHL", "CHLLOLITONOSTSAM", "Latin America",   "Chile",              0.3,  "CL"),
    ("COL", "COLLOLITONOSTSAM", "Latin America",   "Colombia",           0.3,  "CO"),
    ("PRT", "PRTLOLITONOSTSAM", "Europe",          "Portugal",           0.3,  "PT"),
    ("GRC", "GRCLOLITONOSTSAM", "Europe",          "Greece",             0.2,  "GR"),
    ("NZL", "NZLLOLITONOSTSAM", "Asia-Pacific",    "New Zealand",        0.3,  "NZ"),
    ("ISR", "ISRLOLITONOSTSAM", "Middle East",     "Israel",             0.5,  "IL"),
]

# ════════════════════════════════════════════════════════════════════════
# FRED helpers
# ════════════════════════════════════════════════════════════════════════
def fred_observations(series_id, limit=120):
    """Pull last N observations (default 10y) sorted asc by date."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-GBC/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8"))
        obs = []
        for o in data.get("observations", []):
            v = o.get("value")
            if v in (".", "", None):
                continue
            try:
                obs.append({"date": o["date"], "value": float(v)})
            except (ValueError, TypeError):
                continue
        obs.sort(key=lambda o: o["date"])
        return obs
    except Exception as e:
        print(f"[gbc] {series_id} fetch failed: {e}")
        return []


# ════════════════════════════════════════════════════════════════════════
# Phase classification
# ════════════════════════════════════════════════════════════════════════
def classify_phase(cli_series):
    """OECD 4-phase classification from CLI series.

    Returns dict with: phase, cli_level, six_month_change, trend, latest_date
    """
    if not cli_series or len(cli_series) < 7:
        return {"phase": "UNKNOWN", "cli_level": None, "six_month_change": None,
                "trend": None, "latest_date": None}

    latest = cli_series[-1]
    latest_value = latest["value"]
    latest_date = latest["date"]

    # 6-month change: latest minus value 6 months ago
    six_months_back = cli_series[-7]["value"] if len(cli_series) >= 7 else None
    six_month_change = (latest_value - six_months_back) if six_months_back else None
    trend = "rising" if six_month_change is not None and six_month_change > 0 else \
            "falling" if six_month_change is not None and six_month_change < 0 else \
            "flat"

    # Phase logic
    if latest_value >= 100 and trend == "rising":
        phase = "EXPANSION"
    elif latest_value >= 100 and trend == "falling":
        phase = "AT_RISK"
    elif latest_value < 100 and trend == "falling":
        phase = "RECESSION"
    elif latest_value < 100 and trend == "rising":
        phase = "RECOVERY"
    else:
        # Flat trend — classify by level only
        phase = "EXPANSION" if latest_value >= 100 else "RECESSION"

    # Month-over-month change
    mom_change = latest_value - cli_series[-2]["value"] if len(cli_series) >= 2 else None
    yoy_change = (latest_value - cli_series[-13]["value"]) if len(cli_series) >= 13 else None

    # z-score (5y)
    five_y = cli_series[-60:] if len(cli_series) >= 60 else cli_series
    vals = [o["value"] for o in five_y]
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = var ** 0.5
    z = (latest_value - mean) / std if std > 0 else 0

    return {
        "phase": phase,
        "cli_level": round(latest_value, 2),
        "mom_change": round(mom_change, 3) if mom_change is not None else None,
        "six_month_change": round(six_month_change, 3) if six_month_change is not None else None,
        "yoy_change": round(yoy_change, 3) if yoy_change is not None else None,
        "z_5y": round(z, 2),
        "trend": trend,
        "latest_date": latest_date,
        "history_n": len(cli_series),
    }


# ════════════════════════════════════════════════════════════════════════
# Regional + global aggregation
# ════════════════════════════════════════════════════════════════════════
def aggregate(by_country):
    """Compute regional + global phase mix (GDP-weighted)."""
    PHASES = ["EXPANSION", "AT_RISK", "RECESSION", "RECOVERY", "UNKNOWN"]

    # Regional aggregation
    by_region = defaultdict(lambda: {"phase_mix": defaultdict(float), "n_countries": 0,
                                       "total_weight": 0, "avg_cli": 0, "countries": []})
    global_mix = defaultdict(float)
    total_weight = 0
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
        if cli is not None:
            by_region[region]["avg_cli"] += cli * weight

        global_mix[phase] += weight
        total_weight += weight
        if cli is not None:
            weighted_cli_sum += cli * weight

    # Normalize regional phase shares (as % of region GDP)
    for region, info in by_region.items():
        if info["total_weight"] > 0:
            info["phase_mix_pct"] = {p: round(v / info["total_weight"] * 100, 1)
                                      for p, v in info["phase_mix"].items()}
            info["avg_cli"] = round(info["avg_cli"] / info["total_weight"], 2)
        else:
            info["phase_mix_pct"] = {}
        info["phase_mix"] = dict(info["phase_mix"])

    # Normalize global phase shares (as % of total weight)
    global_mix_pct = {p: round(global_mix[p] / total_weight * 100, 1)
                       for p in PHASES if global_mix[p] > 0} if total_weight > 0 else {}
    global_avg_cli = round(weighted_cli_sum / total_weight, 2) if total_weight > 0 else None

    # Determine dominant phase + net direction
    expansion_pct = global_mix_pct.get("EXPANSION", 0) + global_mix_pct.get("RECOVERY", 0)
    contraction_pct = global_mix_pct.get("AT_RISK", 0) + global_mix_pct.get("RECESSION", 0)
    if expansion_pct > 60:
        global_phase = "GLOBAL_EXPANSION"
    elif contraction_pct > 60:
        global_phase = "GLOBAL_CONTRACTION"
    elif global_mix_pct.get("RECOVERY", 0) > 35:
        global_phase = "GLOBAL_RECOVERY"
    elif global_mix_pct.get("AT_RISK", 0) > 35:
        global_phase = "GLOBAL_PEAKING"
    else:
        global_phase = "MIXED"

    return {
        "global_phase": global_phase,
        "global_avg_cli": global_avg_cli,
        "global_phase_mix_pct": global_mix_pct,
        "global_phase_mix_weight": dict(global_mix),
        "total_weight_covered": round(total_weight, 1),
        "expansion_breadth_pct": round(expansion_pct, 1),
        "contraction_breadth_pct": round(contraction_pct, 1),
        "by_region": dict(by_region),
    }


# ════════════════════════════════════════════════════════════════════════
# Interpretation engine — translate global cycle into portfolio implications
# ════════════════════════════════════════════════════════════════════════
def interpret_global_cycle(agg, by_country):
    """Produce decisive interpretation + cross-asset implications from
       the global business cycle state."""
    global_phase = agg["global_phase"]
    global_cli = agg["global_avg_cli"]
    exp_breadth = agg["expansion_breadth_pct"]
    cont_breadth = agg["contraction_breadth_pct"]

    # Get key country signals
    us = by_country.get("USA", {}) or {}
    cn = by_country.get("CHN", {}) or {}
    de = by_country.get("DEU", {}) or {}
    jp = by_country.get("JPN", {}) or {}

    # Cross-asset signals
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
            "dollar":           {"signal": +1, "rationale": "Initial safe-haven, then weakens on cuts"},
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
                    f"the highest-return regime. Sell duration into rate normalization.")
    else:  # MIXED
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
                    f"contraction {cont_breadth:.0f}%). Bottom-up selection over top-down beta. Watch for "
                    f"break to expansion or contraction.")

    # Country tilts (for allocator)
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
# Main
# ════════════════════════════════════════════════════════════════════════
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[gbc] start, {len(COUNTRY_MAP)} countries")

    by_country = {}
    for iso3, fred_id, region, name, weight, iso2 in COUNTRY_MAP:
        obs = fred_observations(fred_id, limit=120)
        phase_info = classify_phase(obs)
        by_country[iso3] = {
            "iso3": iso3,
            "iso2": iso2,
            "fred_id": fred_id,
            "country_name": name,
            "region": region,
            "gdp_weight": weight,
            "n_observations": len(obs),
            **phase_info,
        }
        print(f"[gbc] {iso3:<4} {phase_info.get('phase'):<10} CLI={phase_info.get('cli_level')} "
              f"6m={phase_info.get('six_month_change')}")

    agg = aggregate(by_country)
    interp = interpret_global_cycle(agg, by_country)

    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 1),
        "methodology": {
            "data_source": "OECD Composite Leading Indicator (CLI) via FRED",
            "classification": {
                "EXPANSION": "CLI ≥ 100 AND 6-month change rising",
                "AT_RISK":   "CLI ≥ 100 AND 6-month change falling (peak passing)",
                "RECESSION": "CLI < 100 AND 6-month change falling (deepening)",
                "RECOVERY":  "CLI < 100 AND 6-month change rising (trough passed)",
            },
            "release_cadence": "Monthly (typically with 1-2 month lag)",
            "country_count": len(COUNTRY_MAP),
            "fred_series_pattern": "{ISO3}LOLITONOSTSAM (OECD normalized, smoothed)",
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

    print(f"[gbc] done. global_phase={agg['global_phase']} avg_cli={agg['global_avg_cli']} "
          f"covered={agg['total_weight_covered']}%")

    return {"statusCode": 200, "body": json.dumps({
        "global_phase": agg["global_phase"],
        "global_avg_cli": agg["global_avg_cli"],
        "n_countries": len(by_country),
        "elapsed_sec": output["elapsed_sec"],
    })}
