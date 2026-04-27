"""
justhodl-oecd-cli — OECD Composite Leading Indicators

The OECD's Composite Leading Indicator (CLI) is designed to anticipate
turning points in business cycles 6-9 months ahead. Calibrated against
historical recession dates across 38 OECD + non-OECD economies.

Above 100 = expansion phase (growth above trend)
Below 100 = slowdown phase (growth below trend)
Inflection points (CLI turning) precede recessions/recoveries by ~6-9 months.

For US specifically, CLI has anticipated every recession since 1960 except
1980 (which was Volcker-induced, hard to predict from leading indicators).

Endpoint:
  https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.0/...
  (free; SDMX format; returns CSV/XML/JSON)

Output (data/oecd-cli.json):
  {
    "generated_at": ...,
    "as_of_period": "2026-02",
    "global_avg_cli": 100.4,
    "by_country": {
      "USA": {"cli": 100.2, "trend": "+0.3 vs prior", "phase": "expansion"},
      "DEU": {"cli": 99.4,  "trend": "-0.1 vs prior", "phase": "slowdown"},
      ...
    },
    "regime_signals": {
      "us": "expansion_strengthening",
      "eu": "slowdown_stabilizing",
      "global": "neutral_diverging",
    },
    "interpretation": "<plain English>"
  }
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/oecd-cli.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

FRED_OECD_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")

# OECD CLI series available via FRED (more reliable than direct OECD SDMX which
# changes URL patterns frequently). Each is OECD's amplitude-adjusted CLI for
# that country, normalized so 100 = long-term trend.
FRED_CLI_SERIES = {
    "USA": "USALOLITONOSTSAM",     # United States
    "CHN": "CHNLOLITONOSTSAM",     # China
    "JPN": "JPNLOLITONOSTSAM",     # Japan
    "DEU": "DEULOLITONOSTSAM",     # Germany
    "GBR": "GBRLOLITONOSTSAM",     # United Kingdom
    "FRA": "FRALOLITONOSTSAM",     # France
    "ITA": "ITALOLITONOSTSAM",     # Italy
    "CAN": "CANLOLITONOSTSAM",     # Canada
    "ESP": "ESPLOLITONOSTSAM",     # Spain
    "KOR": "KORLOLITONOSTSAM",     # Korea
    "MEX": "MEXLOLITONOSTSAM",     # Mexico
    "IND": "INDLOLITONOSTSAM",     # India
    "TUR": "TURLOLITONOSTSAM",     # Turkey
    "AUS": "AUSLOLITONOSTSAM",     # Australia
    "OECD": "OECDLOLITONOSTSAM",   # OECD Total
}

# Country code → human label
COUNTRY_LABELS = {
    "USA": "United States", "CHN": "China", "JPN": "Japan",
    "DEU": "Germany", "GBR": "United Kingdom", "FRA": "France",
    "ITA": "Italy", "CAN": "Canada", "ESP": "Spain",
    "KOR": "Korea", "MEX": "Mexico", "IND": "India",
    "TUR": "Turkey", "AUS": "Australia",
    "OECD": "OECD Total",
}


def _fetch_json(url: str, timeout: int = 20):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fetch_country_series(country_code: str, fred_id: str) -> list:
    """Return last 24 monthly CLI observations for one country via FRED."""
    url = (f"{FRED_OECD_BASE}?series_id={fred_id}&api_key={FRED_KEY}"
           f"&file_type=json&sort_order=desc&limit=24")
    try:
        data = _fetch_json(url)
        obs = []
        for o in data.get("observations", []):
            v = o.get("value")
            if v in (".", "", None):
                continue
            try:
                obs.append({"period": o["date"], "value": float(v)})
            except (ValueError, KeyError):
                continue
        # FRED returns desc; we want chronological asc
        obs.sort(key=lambda x: x["period"])
        return obs
    except Exception as e:
        print(f"FRED fetch fail {country_code}/{fred_id}: {e}")
        return []


def _classify_phase(cli: float, prior_cli: float = None) -> str:
    """Map CLI level + change to a phase descriptor."""
    if cli is None:
        return "unknown"
    direction = None
    if prior_cli is not None:
        delta = cli - prior_cli
        if delta > 0.05: direction = "rising"
        elif delta < -0.05: direction = "falling"
        else: direction = "flat"

    if cli >= 100.5:
        base = "expansion"
    elif cli >= 99.5:
        base = "neutral"
    else:
        base = "slowdown"

    if direction == "rising" and base == "slowdown":
        return "slowdown_recovering"
    if direction == "falling" and base == "expansion":
        return "expansion_weakening"
    if direction == "rising" and base == "expansion":
        return "expansion_strengthening"
    if direction == "falling" and base == "slowdown":
        return "slowdown_deepening"
    return base


def aggregate_signals(by_country):
    """Roll into latest readings + phase + trend."""
    latest_period = ""
    out = {}
    for country, series in by_country.items():
        if not series:
            continue
        latest = series[-1]
        prior = series[-2] if len(series) > 1 else None
        cli = latest["value"]
        prior_cli = prior["value"] if prior else None
        phase = _classify_phase(cli, prior_cli)

        if latest["period"] > latest_period:
            latest_period = latest["period"]

        trend = "—"
        if prior:
            delta = cli - prior_cli
            trend = f"{delta:+.2f} vs prior"

        out[country] = {
            "country": COUNTRY_LABELS.get(country, country),
            "cli": round(cli, 2),
            "prior_cli": round(prior_cli, 2) if prior_cli else None,
            "trend": trend,
            "phase": phase,
        }
    return out, latest_period


def interpret(by_country: dict) -> str:
    us = by_country.get("USA", {})
    oecd = by_country.get("OECD", {})

    parts = []
    if us:
        if us["phase"].startswith("expansion"):
            parts.append(f"US CLI at {us['cli']} signals expansion phase")
        elif us["phase"].startswith("slowdown"):
            parts.append(f"US CLI at {us['cli']} signals slowdown phase")
        if "_strengthening" in us["phase"]:
            parts[-1] += " and strengthening (positive 6-9mo outlook)"
        elif "_weakening" in us["phase"]:
            parts[-1] += " but weakening (deterioration ahead)"
        elif "_recovering" in us["phase"]:
            parts[-1] += " but recovering (turn ahead)"

    if oecd:
        if oecd["cli"] > 100.5 and us.get("cli", 100) > 100.5:
            parts.append("Global + US growth synchronized above trend")
        elif oecd["cli"] < 99.5 and us.get("cli", 100) < 99.5:
            parts.append("Global + US slowdown synchronized — recession risk elevated")
        elif (oecd["cli"] > 100) != (us.get("cli", 100) > 100):
            parts.append("US and global cycles diverging")

    if not parts:
        return "OECD CLI data fetched; awaiting calibration."
    return ". ".join(parts) + "."


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    # Fetch per-country series via FRED
    by_country_raw = {}
    fetch_errors = []
    for country, fred_id in FRED_CLI_SERIES.items():
        series = fetch_country_series(country, fred_id)
        if series:
            by_country_raw[country] = series
        else:
            fetch_errors.append(country)
        # FRED rate limit is generous but be polite
        time.sleep(0.1)

    if not by_country_raw:
        return {"statusCode": 502,
                "body": json.dumps({"error": f"All FRED CLI fetches failed: {fetch_errors}"})}

    by_country, latest_period = aggregate_signals(by_country_raw)

    # Compute global average CLI
    cli_values = [v["cli"] for v in by_country.values() if v.get("cli") is not None]
    global_avg = round(sum(cli_values) / len(cli_values), 2) if cli_values else None

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "as_of_period": latest_period,
        "global_avg_cli": global_avg,
        "by_country": by_country,
        "interpretation": interpret(by_country),
        "fetch_errors": fetch_errors,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"OECD CLI {latest_period} | global avg {global_avg} | {len(by_country)} countries | {len(fetch_errors)} errors")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "period": latest_period,
                            "global_avg": global_avg,
                            "us_cli": by_country.get("USA", {}).get("cli")}),
    }
