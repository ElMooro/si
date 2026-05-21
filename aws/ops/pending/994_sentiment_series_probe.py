"""
ops 994 - Sentiment series probe (FRED).

ops 993 confirmed:
- FRED key works (VIXCLS returns data)
- FRED CBOEEQUITYPCRATIO + PUTCALL DEAD (400 - series retired)
- FMP options endpoints all 404
- Polygon already known stocks-only

Pivot: institutional-grade Sentiment Extreme Composite.
This ops probes candidate FRED sentiment series:
  - AAIIBULL, AAIIBEAR, AAIIBULLBEARSPREAD (retail sentiment)
  - UMCSENT (UMich Consumer Sentiment, monthly)
  - MICH (alt UMich series)
  - NAAIMNUMBER (NAAIM active managers, may not be in FRED)
  - VIXCLS (sanity)
  - USEPUINDXD (Economic Policy Uncertainty daily)
  - SENT_NEG (Daily News Sentiment if exists)

Returns: which series work + their cadence + sample.
ops 995 will use winners to build the new composite engine.
"""

import io
import json
import sys
import time
import traceback
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
DONOR_FN = "justhodl-cross-asset-rv"
PROBE_FN = "justhodl-ops-994-sentiment-probe"

cfg = Config(read_timeout=120, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)


PROBE_SOURCE = r"""
import json
import os
import urllib.request

FRED_KEY = os.environ.get("FRED_KEY", "")


def _fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore"), r.status


def probe(series_id, limit=20):
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}"
           f"&file_type=json&sort_order=desc&limit={limit}")
    try:
        body, status = _fetch(url)
        d = json.loads(body)
        if "error_message" in d:
            return {"ok": False, "series": series_id, "status": status,
                    "error": d["error_message"][:200]}
        obs = d.get("observations", [])
        values = [(o.get("date"), o.get("value")) for o in obs
                  if o.get("value") and o.get("value") != "."]
        if not values:
            return {"ok": False, "series": series_id,
                    "error": "no_values", "status": status}
        # Latest 5 + cadence detect
        dates = [v[0] for v in values[:10]]
        return {"ok": True, "series": series_id, "n": len(values),
                "latest_date": values[0][0],
                "latest_value": values[0][1],
                "sample_dates": dates,
                "values_recent": [float(v[1]) for v in values[:5]]}
    except Exception as e:
        return {"ok": False, "series": series_id, "error": str(e)[:200]}


def lambda_handler(event, context):
    if not FRED_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "no_fred_key"})}
    series = [
        # AAII Investor Sentiment
        "AAIIBULL", "AAIIBEAR", "AAIIBULLBEARSPREAD", "AAIINEU",
        # UMich Consumer Sentiment
        "UMCSENT", "MICH",
        # NAAIM
        "NAAIMNUMBER",
        # VIX + alt vol
        "VIXCLS", "VXNCLS", "VXOCLS",
        # Econ Policy Uncertainty
        "USEPUINDXD", "EPUNYTODAY",
        # Daily News Sentiment Index (FRBSF)
        "STLFSI4",
        # Citigroup Economic Surprise (not in FRED but try)
        "CESI",
        # NFIB Small Business Optimism
        "NFIB",
        # Conf Board Consumer Conf
        "CCICONFTOT",
    ]
    out = {s: probe(s) for s in series}
    return {"statusCode": 200, "body": json.dumps({"ok": True, "series": out})}
"""


def build_zip(source):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", source)
    return buf.getvalue()


def wait_for_settled(fn, max_wait=120):
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            cf = lam.get_function_configuration(FunctionName=fn)
            state = cf.get("State")
            lst = cf.get("LastUpdateStatus")
            if state == "Active" and lst in (None, "Successful"):
                return {"ok": True, "state": state, "last_status": lst}
            if lst == "Failed":
                return {"ok": False, "reason": cf.get("LastUpdateStatusReason")}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        time.sleep(3)
    return {"ok": False, "error": "timeout"}


def main():
    started = datetime.now(timezone.utc).isoformat()
    report = {"ops": 994, "started_at": started, "donor": DONOR_FN}
    try:
        cf = lam.get_function_configuration(FunctionName=DONOR_FN)
        env = (cf.get("Environment") or {}).get("Variables") or {}
        keep = {k: env[k] for k in ("FRED_KEY",) if k in env}
        report["env_keys_used"] = sorted(keep.keys())

        z = build_zip(PROBE_SOURCE)
        try:
            lam.get_function(FunctionName=PROBE_FN)
            exists = True
        except lam.exceptions.ResourceNotFoundException:
            exists = False
        if exists:
            lam.update_function_code(FunctionName=PROBE_FN, ZipFile=z)
            wait_for_settled(PROBE_FN)
            lam.update_function_configuration(
                FunctionName=PROBE_FN, Timeout=90, MemorySize=256,
                Environment={"Variables": keep})
        else:
            lam.create_function(
                FunctionName=PROBE_FN, Runtime="python3.12", Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": z}, Timeout=90, MemorySize=256,
                Environment={"Variables": keep},
                Description="ops 994 sentiment series probe (temp)")
        report["deploy"] = wait_for_settled(PROBE_FN)

        r = lam.invoke(FunctionName=PROBE_FN, Payload=b"{}")
        payload = json.loads(r["Payload"].read().decode("utf-8"))
        if payload.get("statusCode") == 200:
            body = json.loads(payload["body"])
            series_results = body.get("series", {})
            working = {s: r for s, r in series_results.items() if r.get("ok")}
            dead = {s: r.get("error", "unknown")
                    for s, r in series_results.items() if not r.get("ok")}
            report["working_series"] = working
            report["dead_series"] = dead
            report["n_working"] = len(working)
            report["n_dead"] = len(dead)
        else:
            report["invoke_error"] = payload

        try:
            lam.delete_function(FunctionName=PROBE_FN)
            report["delete_probe_ok"] = True
        except Exception:
            report["delete_probe_ok"] = False

    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        (out_dir / "994.json").write_text(json.dumps(report, indent=2, default=str))
        print(f"Report: aws/ops/reports/994.json")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
