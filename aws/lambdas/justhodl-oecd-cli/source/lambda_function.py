"""
justhodl-oecd-cli v2.0.0 — OECD Composite Leading Indicators, from the source.

v1 read the OECD CLI through FRED (USALOLITONOSTSAM...), which stopped in
January 2024; from then on the engine republished a 2024-01 reading every day
(ops 5098 audit: "zombie feed"), and justhodl-global-recession correctly
refused to use it (oecd_usable=false, age 32 months).

v2 reads the OECD's own SDMX pull that justhodl-cycle-features makes daily
(DSD_STES@DF_CLI, amplitude-adjusted CLI, cached verbatim at
data/warm/oecd/cycle/DF_CLI.csv.gz -- 22 reference areas including the OECD
total, G7, euro area, Major-5-Asia and NAFTA aggregates), falling back to the
feature store document (data/cycle/features.json.gz, 17 countries) when the
raw cache is missing. Output schema is unchanged so every consumer
(global-recession hard leg 1, morning-intelligence, ai-chat, signals.html)
keeps working; per-country rows additionally carry the v3 composite read
(`composite_cli`, `composite_phase`) from data/global-business-cycle.json.

Above 100 = growth above trend; below 100 = below trend; the CLI's turning
points lead activity by ~6-9 months.
"""
import csv
import gzip
import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "2.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/oecd-cli.json")
CLI_CACHE_KEY = "data/warm/oecd/cycle/DF_CLI.csv.gz"
FEATURES_KEY = "data/cycle/features.json.gz"
GBC_KEY = "data/global-business-cycle.json"
MAX_CACHE_AGE_D = 45

COUNTRY_LABELS = {
    "USA": "United States", "CHN": "China", "JPN": "Japan", "DEU": "Germany", "GBR": "United Kingdom",
    "FRA": "France", "ITA": "Italy", "CAN": "Canada", "ESP": "Spain", "KOR": "Korea", "MEX": "Mexico",
    "IND": "India", "TUR": "Turkey", "AUS": "Australia", "BRA": "Brazil", "IDN": "Indonesia",
    "ZAF": "South Africa", "OECD": "OECD Total", "G7": "G7", "EA20": "Euro area (20)", "EA": "Euro area",
    "A5M": "Major five Asia", "NAFTA": "NAFTA", "G20": "G20", "OECDE": "OECD Europe", "EU27_2020": "European Union",
}
AGGREGATES = {"OECD", "G7", "EA20", "EA", "A5M", "NAFTA", "G20", "OECDE", "EU27_2020"}
s3 = boto3.client("s3")


def _get(key):
    o = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = o["Body"].read()
    if key.endswith(".gz") or body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body, o["LastModified"]


def _classify_phase(cli, prior_cli=None):
    if cli is None:
        return "unknown"
    direction = None
    if prior_cli is not None:
        delta = cli - prior_cli
        if delta > 0.05:
            direction = "rising"
        elif delta < -0.05:
            direction = "falling"
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


def series_from_cache():
    """{area: [{period, value}...]} from the raw OECD csvfile cache (all areas)."""
    body, lm = _get(CLI_CACHE_KEY)
    age_d = (datetime.now(timezone.utc) - lm).total_seconds() / 86400
    if age_d > MAX_CACHE_AGE_D:
        raise RuntimeError(f"CLI cache is {age_d:.0f} days old")
    text = body.decode("utf-8", "replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    rd = csv.DictReader(io.StringIO(text))
    out = defaultdict(dict)
    for row in rd:
        if row.get("MEASURE") != "LI" or row.get("FREQ", "M") != "M":
            continue
        try:
            out[row["REF_AREA"]][row["TIME_PERIOD"][:7]] = float(row["OBS_VALUE"])
        except (KeyError, ValueError, TypeError):
            continue
    return ({a: [{"period": p, "value": v} for p, v in sorted(d.items())][-24:] for a, d in out.items() if len(d) >= 2},
            f"OECD DSD_STES@DF_CLI via cycle-features cache ({lm.date()})")


def series_from_features():
    body, lm = _get(FEATURES_KEY)
    doc = json.loads(body)
    months = doc["grid"]["months"]
    out = {}
    for iso, c in (doc.get("countries") or {}).items():
        f = (c.get("features") or {}).get("cli_oecd")
        if not f:
            continue
        pts = [{"period": m, "value": v} for m, v in zip(months, f["values"]) if v is not None]
        if len(pts) >= 2:
            out[iso] = pts[-24:]
    return out, f"OECD DSD_STES@DF_CLI via data/cycle/features.json.gz ({doc.get('generated_at')})"


def aggregate_signals(by_area):
    latest_period = ""
    out = {}
    for area, series in by_area.items():
        latest, prior = series[-1], (series[-2] if len(series) > 1 else None)
        cli, prior_cli = latest["value"], (prior["value"] if prior else None)
        if latest["period"] > latest_period:
            latest_period = latest["period"]
        out[area] = {"country": COUNTRY_LABELS.get(area, area), "cli": round(cli, 2),
                     "prior_cli": round(prior_cli, 2) if prior_cli is not None else None,
                     "trend": (f"{cli - prior_cli:+.2f} vs prior" if prior_cli is not None else "—"),
                     "phase": _classify_phase(cli, prior_cli), "period": latest["period"],
                     "history_24m": [{"p": s["period"], "v": round(s["value"], 3)} for s in series],
                     "aggregate": area in AGGREGATES}
    return out, latest_period


def interpret(by_country):
    us, oecd = by_country.get("USA", {}), by_country.get("OECD", {})
    parts = []
    if us:
        if us["phase"].startswith("expansion"):
            parts.append(f"US CLI at {us['cli']} signals expansion phase")
        elif us["phase"].startswith("slowdown"):
            parts.append(f"US CLI at {us['cli']} signals slowdown phase")
        else:
            parts.append(f"US CLI at {us['cli']} is at trend")
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
        else:
            parts.append(f"OECD total CLI {oecd['cli']} ({oecd['phase'].replace('_', ' ')})")
    return ". ".join(parts) + "." if parts else "OECD CLI read; no US/OECD rows."


def lambda_handler(event, context):
    started = time.time()
    fetch_errors = []
    by_area, source = {}, None
    try:
        by_area, source = series_from_cache()
        print(f"[oecd-cli] cache: {len(by_area)} areas")
    except Exception as e:  # noqa: BLE001
        fetch_errors.append(f"cache: {str(e)[:120]}")
        print(f"[oecd-cli] cache unavailable ({str(e)[:100]}); falling back to feature store")
        try:
            by_area, source = series_from_features()
        except Exception as e2:  # noqa: BLE001
            fetch_errors.append(f"features: {str(e2)[:120]}")
    if not by_area:
        # never overwrite the last good document with nothing
        return {"statusCode": 502, "body": json.dumps({"error": "no OECD CLI source available", "errors": fetch_errors})}
    by_country, latest_period = aggregate_signals(by_area)
    try:
        gbc = json.loads(_get(GBC_KEY)[0])
        for iso, row in (gbc.get("by_country") or {}).items():
            if iso in by_country:
                by_country[iso]["composite_cli"] = row.get("cli_level")
                by_country[iso]["composite_phase"] = row.get("phase")
                by_country[iso]["composite_basis"] = row.get("phase_basis")
        composite_note = {"engine_version": gbc.get("engine_version"), "generated_at": gbc.get("generated_at")}
    except Exception as e:  # noqa: BLE001
        composite_note = {"error": str(e)[:100]}
    countries = {k: v for k, v in by_country.items() if not v.get("aggregate")}
    cli_values = [v["cli"] for v in countries.values()]
    global_avg = round(sum(cli_values) / len(cli_values), 2) if cli_values else None
    y, m = int(latest_period[:4]), int(latest_period[5:7])
    now = datetime.now(timezone.utc)
    age_months = (now.year - y) * 12 + (now.month - m)
    output = {
        "engine_version": ENGINE_VERSION,
        "generated_at": now.isoformat(timespec="seconds"),
        "as_of_period": f"{latest_period}-01",
        "as_of_age_months": age_months,
        "source": source,
        "global_avg_cli": global_avg,
        "oecd_total_cli": (by_country.get("OECD") or {}).get("cli"),
        "n_countries": len(countries),
        "n_aggregates": len(by_country) - len(countries),
        "by_country": by_country,
        "interpretation": interpret(by_country),
        "composite": composite_note,
        "fetch_errors": fetch_errors,
        "fetch_duration_s": round(time.time() - started, 1),
        "note": ("Amplitude-adjusted OECD CLI (DSD_STES@DF_CLI). The FRED mirror of these series stopped in January 2024; "
                 "v2 reads the OECD's own SDMX pull made daily by justhodl-cycle-features."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=json.dumps(output).encode(), ContentType="application/json", CacheControl="no-cache")
    print(f"[oecd-cli] v{ENGINE_VERSION} {latest_period} ({age_months}mo) | avg {global_avg} | {len(countries)} countries + {len(by_country) - len(countries)} aggregates | errors {fetch_errors}")
    return {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "period": latest_period, "global_avg": global_avg, "us_cli": (by_country.get("USA") or {}).get("cli"),
                                "oecd_cli": (by_country.get("OECD") or {}).get("cli"), "n_countries": len(countries)})}
