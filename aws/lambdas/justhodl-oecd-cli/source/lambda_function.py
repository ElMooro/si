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
import csv
import io
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/oecd-cli.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

# OECD SDMX endpoint for CLI (Amplitude Adjusted Leading Indicator)
# Returns CSV with columns: TIME_PERIOD, REF_AREA, MEASURE, OBS_VALUE
OECD_CLI_URL = (
    "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.0/"
    "USA+CHN+JPN+DEU+GBR+FRA+ITA+CAN+ESP+KOR+MEX+IND+IDN+TUR+AUS+OECD.M.LI.IX.AA....?"
    "startPeriod=2024-01&format=csvfilewithlabels"
)

# Country code → human label
COUNTRY_LABELS = {
    "USA": "United States", "CHN": "China", "JPN": "Japan",
    "DEU": "Germany", "GBR": "United Kingdom", "FRA": "France",
    "ITA": "Italy", "CAN": "Canada", "ESP": "Spain",
    "KOR": "Korea", "MEX": "Mexico", "IND": "India",
    "IDN": "Indonesia", "TUR": "Turkey", "AUS": "Australia",
    "OECD": "OECD Total",
}


def _fetch(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/csv,application/json,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


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


def parse_oecd_csv(raw: str):
    """Parse OECD's CSV response into per-country time series."""
    reader = csv.DictReader(io.StringIO(raw))
    by_country = {}
    for row in reader:
        country = row.get("REF_AREA", "")
        period = row.get("TIME_PERIOD", "")
        try:
            value = float(row.get("OBS_VALUE", ""))
        except ValueError:
            continue
        if country not in by_country:
            by_country[country] = []
        by_country[country].append({"period": period, "value": value})
    # Sort each by period
    for c in by_country:
        by_country[c].sort(key=lambda x: x["period"])
    return by_country


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

    try:
        raw = _fetch(OECD_CLI_URL).decode("utf-8", errors="ignore")
    except Exception as e:
        return {"statusCode": 502, "body": json.dumps({"error": f"OECD fetch failed: {e}"})}

    by_country_raw = parse_oecd_csv(raw)
    if not by_country_raw:
        return {"statusCode": 502, "body": json.dumps({"error": "No data parsed from OECD CSV"})}

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
        "fetch_duration_s": round(time.time() - started, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"OECD CLI {latest_period} | global avg {global_avg} | {len(by_country)} countries")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "period": latest_period,
                            "global_avg": global_avg,
                            "us_cli": by_country.get("USA", {}).get("cli")}),
    }
