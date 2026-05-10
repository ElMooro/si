#!/usr/bin/env python3
"""Step 393 — Probe sources for FRESH (≤3mo) OECD CLI data.

Tests in order:
 1. OECD new SDMX REST API (sdmx.oecd.org) — should be the canonical source
 2. OECD old SDMX-JSON (stats.oecd.org) — legacy
 3. FRED alternative naming patterns we haven't tried (post-2024 series)

For each source, fetches latest date for USA/CHN/DEU/JPN/GBR and reports
freshness (months stale) — so we can pick the source that returns 2026 data.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/393_oecd_freshness_probe.json"
NAME = "justhodl-tmp-oecd-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = r'''
import json, urllib.request, urllib.parse
from datetime import datetime

FRED_KEY = "2f057499936072679d8843d7fce99989"
COUNTRIES = ["USA","CHN","DEU","JPN","GBR","FRA","ITA","BRA","IND","KOR"]
NOW = datetime.utcnow()


def months_stale(date_str):
    """Return months between given YYYY-MM(-DD) date and now."""
    try:
        if len(date_str) == 7:
            date_str += "-01"
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (NOW.year - d.year) * 12 + (NOW.month - d.month)
    except Exception:
        return -1


def fetch(url, headers=None, timeout=15):
    try:
        req = urllib.request.Request(url, headers=headers or {"User-Agent":"JH-probe/1.0","Accept":"application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace"), r.status
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}: {e.reason}", e.code
    except Exception as e:
        return f"ERR: {e}"[:300], None


def probe_oecd_new_sdmx(iso3):
    """OECD's new SDMX REST API at sdmx.oecd.org.
    Dataset DSD_STES@DF_CLI v4.0. CLI measure = IX (index), reference area = country.
    Try multiple key formats since structure dimensions vary by dataset version."""
    base = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.0"
    # Try wide-open key with country first
    for key in [f"{iso3}........", f"{iso3}.M.LI.....", f".{iso3}.....", iso3]:
        url = f"{base}/{key}?startPeriod=2025-01&dimensionAtObservation=AllDimensions&format=jsondata"
        body, code = fetch(url)
        if code == 200 and body and body.lstrip().startswith("{"):
            try:
                d = json.loads(body)
                # SDMX-JSON: data > dataSets > [0] > observations > {idx: [value, ...]}
                # AND structures > dimensions has dimension keys
                ds = (d.get("data") or {}).get("dataSets") or []
                if ds:
                    obs = ds[0].get("observations", {})
                    if obs:
                        # Find the latest TIME_PERIOD
                        struct = (d.get("data") or {}).get("structures") or [d.get("structure", {})]
                        time_dim_values = []
                        if struct:
                            dims = struct[0].get("dimensions", {}).get("observation") or []
                            for dim in dims:
                                if dim.get("id") in ("TIME_PERIOD","TIME"):
                                    time_dim_values = [v.get("id") for v in dim.get("values", [])]
                        if time_dim_values:
                            latest = sorted(time_dim_values)[-1]
                            return {"source":"oecd_new", "key":key, "latest_date":latest,
                                     "months_stale":months_stale(latest), "n_obs":len(obs)}
            except Exception:
                pass
    return {"source":"oecd_new", "error": "no_match_or_unparseable"}


def probe_oecd_legacy(iso3):
    """OECD legacy SDMX-JSON at stats.oecd.org (may be deprecated but still serve data)."""
    # Format: /sdmx-json/data/MEI_CLI/{indicator}.{country}.{frequency}/all
    url = f"https://stats.oecd.org/sdmx-json/data/MEI_CLI/LOLITONOSTSAM.{iso3}.M/all?startTime=2025"
    body, code = fetch(url, timeout=20)
    if code == 200 and body.lstrip().startswith("{"):
        try:
            d = json.loads(body)
            struct = (d.get("structure") or {})
            time_dim = None
            for dim in (struct.get("dimensions", {}).get("observation") or []):
                if dim.get("id") in ("TIME_PERIOD","TIME"):
                    vals = [v.get("id") for v in dim.get("values", [])]
                    if vals:
                        time_dim = sorted(vals)[-1]
                        break
            ds = (d.get("dataSets") or [])
            n_obs = len(ds[0].get("observations", {})) if ds else 0
            return {"source":"oecd_legacy", "latest_date":time_dim,
                     "months_stale":months_stale(time_dim) if time_dim else -1, "n_obs":n_obs}
        except Exception as e:
            return {"source":"oecd_legacy", "error": str(e)[:120]}
    return {"source":"oecd_legacy", "http_status":code, "snippet":body[:140]}


def probe_oecd_csv(iso3):
    """OECD CSV endpoint — sometimes more reliable than SDMX-JSON."""
    # The CSV endpoint uses different host and path
    url = f"https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.0/{iso3}........?startPeriod=2025-01&format=csvfile"
    body, code = fetch(url, headers={"User-Agent":"JH-probe/1.0","Accept":"text/csv"}, timeout=20)
    if code == 200 and body and "," in body:
        lines = [l for l in body.strip().split("\n") if l]
        if len(lines) > 1:
            # Parse header — find TIME_PERIOD column
            hdr = lines[0].split(",")
            try:
                time_col = next(i for i, h in enumerate(hdr) if "TIME" in h.upper())
                value_col = next(i for i, h in enumerate(hdr) if "VALUE" in h.upper() or h == "OBS_VALUE")
                dates = []
                for line in lines[1:]:
                    cells = line.split(",")
                    if len(cells) > max(time_col, value_col):
                        date = cells[time_col].strip('"')
                        val = cells[value_col].strip('"')
                        try:
                            float(val)
                            dates.append(date)
                        except: pass
                if dates:
                    latest = sorted(dates)[-1]
                    return {"source":"oecd_csv", "latest_date":latest,
                             "months_stale":months_stale(latest), "n_obs":len(dates),
                             "first_3_lines": lines[:3]}
            except Exception as e:
                return {"source":"oecd_csv", "error": str(e)[:120], "snippet":body[:200]}
    return {"source":"oecd_csv", "http_status":code, "snippet":body[:200]}


def probe_fred_alt_patterns(iso3):
    """Try newer FRED naming patterns we haven't tested yet."""
    patterns_to_try = [
        f"{iso3}LOCOBSNOSTSAM",   # business situation indicator
        f"{iso3}LRHUTTTTM156S",   # labor market
        # OECD published a new CLI v2 in 2024; FRED may have:
        f"{iso3}LOCOABCITONONST", # newer alt
        f"OECDCLIO{iso3}",          # alt prefix
        f"{iso3}LOCOSITOSTSAM",   # situation indicator
    ]
    found = []
    for p in patterns_to_try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={p}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=3"
        body, code = fetch(url)
        if code == 200 and body.lstrip().startswith("{"):
            try:
                d = json.loads(body)
                obs = d.get("observations", [])
                valid = [o for o in obs if o.get("value") not in (".","",None)]
                if valid:
                    latest = valid[0]
                    found.append({"id":p, "latest":latest["date"], "value":latest["value"],
                                    "months_stale":months_stale(latest["date"])})
            except Exception: pass
    return {"source":"fred_alt_patterns", "found":found}


def probe_fred_current_cli(iso3):
    """Confirm what FRED currently returns for the standard series with limit=1."""
    sid = f"{iso3}LOLITONOSTSAM"
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=1"
    body, code = fetch(url)
    if code == 200 and body.lstrip().startswith("{"):
        try:
            d = json.loads(body)
            obs = d.get("observations", [])
            if obs:
                o = obs[0]
                return {"source":"fred_standard", "id":sid, "latest_date":o.get("date"),
                         "value":o.get("value"), "months_stale":months_stale(o.get("date") or "")}
        except: pass
    return {"source":"fred_standard", "id":sid, "error":"failed"}


def lambda_handler(event, context):
    out = {"now": NOW.isoformat(), "by_country": {}}
    for iso3 in COUNTRIES:
        out["by_country"][iso3] = {
            "fred_standard": probe_fred_current_cli(iso3),
            "oecd_legacy":   probe_oecd_legacy(iso3),
            "oecd_new_sdmx": probe_oecd_new_sdmx(iso3),
            "oecd_csv":      probe_oecd_csv(iso3),
            "fred_alt":      probe_fred_alt_patterns(iso3),
        }
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
