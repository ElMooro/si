"""
ops 992 - Put-Call Extreme data-source probe (RE-RUN, fixed report path).

ops 991 ran the probe but wrote the report to S3 instead of the local
filesystem - so GH Actions auto-commit had nothing to add. This version
mirrors the proven ops 989 pattern: try/finally + write to
REPO_ROOT/aws/ops/reports/NNN.json.

Probes 11 candidate sources for CBOE equity put/call ratio:
  - FRED: CBOEEQUITYPCRATIO, PUTCALL, VIXCLS (sanity)
  - Yahoo: ^CPC + ^CPCE x {simple UA, browser UA}, dollar variants
  - CBOE direct CSV (3 URL candidates)
  - AlphaVantage HISTORICAL_OPTIONS

Returns which sources work + sample data + creates basis for ops 993
to wire winning source(s) into put-call-extreme.
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
DONOR_FN = "justhodl-earnings-pead"
PROBE_FN = "justhodl-ops-992-pc-probe"

cfg = Config(read_timeout=120, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)


PROBE_SOURCE = r"""
import json
import os
import urllib.parse
import urllib.request

FRED_KEY = os.environ.get("FRED_KEY", "")
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
SIMPLE_UA = "justhodl/1.0"


def _fetch(url, ua, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore"), r.status


def probe_fred(series_id):
    if not FRED_KEY:
        return {"ok": False, "error": "no_fred_key"}
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}"
           f"&file_type=json&sort_order=desc&limit=50")
    try:
        body, status = _fetch(url, SIMPLE_UA)
        d = json.loads(body)
        obs = d.get("observations", [])
        values = [float(o["value"]) for o in obs
                  if o.get("value") and o["value"] != "."]
        sample = values[:5] if values else []
        return {"ok": len(values) >= 10, "n_values": len(values),
                "sample": sample, "status": status,
                "latest_date": (obs[0].get("date") if obs else None)}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def probe_yahoo(symbol_raw, ua_label):
    ua = BROWSER_UA if ua_label == "browser" else SIMPLE_UA
    sym_enc = urllib.parse.quote(symbol_raw)
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_enc}"
           f"?range=2y&interval=1d")
    try:
        body, status = _fetch(url, ua, timeout=15)
        d = json.loads(body)
        result = (d.get("chart") or {}).get("result", [])
        if not result:
            err = (d.get("chart") or {}).get("error", {})
            return {"ok": False, "n_values": 0, "status": status,
                    "yahoo_err": str(err)[:200]}
        r = result[0]
        closes = (r.get("indicators", {})
                  .get("quote", [{}])[0].get("close") or [])
        valid = [c for c in closes if c is not None]
        sample = valid[-5:] if valid else []
        return {"ok": len(valid) >= 30, "n_values": len(valid),
                "sample": sample, "status": status}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def probe_cboe_csv():
    candidates = [
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/CPCE_History.csv",
        "https://cdn.cboe.com/data/us/options/market_statistics/daily/_market_statistics.csv",
        "https://www.cboe.com/us/options/market_statistics/historical_data/equitypc.csv",
    ]
    out = []
    for url in candidates:
        try:
            body, status = _fetch(url, BROWSER_UA, timeout=10)
            lines = [ln for ln in body.split("\n") if ln.strip()]
            preview = lines[:3] if lines else []
            out.append({"url": url, "ok": status == 200 and len(lines) > 10,
                        "status": status, "n_lines": len(lines),
                        "preview": preview})
        except Exception as e:
            out.append({"url": url, "ok": False, "error": str(e)[:200]})
    return out


def probe_alphavantage_options():
    if not ALPHA_VANTAGE_KEY:
        return {"ok": False, "error": "no_av_key"}
    url = (f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS"
           f"&symbol=SPY&date=2025-01-15&apikey={ALPHA_VANTAGE_KEY}")
    try:
        body, status = _fetch(url, SIMPLE_UA, timeout=15)
        d = json.loads(body)
        if "Information" in d or "Note" in d:
            return {"ok": False, "status": status,
                    "msg": (d.get("Information") or d.get("Note") or "")[:200]}
        data = d.get("data") or []
        return {"ok": len(data) > 0, "status": status, "n_records": len(data)}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def lambda_handler(event, context):
    out = {
        "FRED_CBOEEQUITYPCRATIO": probe_fred("CBOEEQUITYPCRATIO"),
        "FRED_PUTCALL":           probe_fred("PUTCALL"),
        "FRED_VIXCLS":            probe_fred("VIXCLS"),
        "Yahoo_CPC_simple":       probe_yahoo("^CPC",  "simple"),
        "Yahoo_CPC_browser":      probe_yahoo("^CPC",  "browser"),
        "Yahoo_CPCE_simple":      probe_yahoo("^CPCE", "simple"),
        "Yahoo_CPCE_browser":     probe_yahoo("^CPCE", "browser"),
        "Yahoo_dollar_CPC":       probe_yahoo("$CPC",  "browser"),
        "Yahoo_dollar_CPCE":      probe_yahoo("$CPCE", "browser"),
        "CBOE_direct_CSV":        probe_cboe_csv(),
        "AlphaVantage_options":   probe_alphavantage_options(),
    }
    return {"statusCode": 200, "body": json.dumps({"ok": True, "probes": out})}
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
                return {"ok": False, "state": state, "last_status": lst,
                        "reason": cf.get("LastUpdateStatusReason")}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        time.sleep(3)
    return {"ok": False, "error": "timeout"}


def get_donor_env(fn):
    cf = lam.get_function_configuration(FunctionName=fn)
    return (cf.get("Environment") or {}).get("Variables") or {}


def create_or_update_probe(env_vars):
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
            FunctionName=PROBE_FN, Timeout=120, MemorySize=256,
            Environment={"Variables": env_vars})
    else:
        lam.create_function(
            FunctionName=PROBE_FN,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": z},
            Timeout=120, MemorySize=256,
            Environment={"Variables": env_vars},
            Description="ops 992 PC source probe (temp, delete after)")
    return wait_for_settled(PROBE_FN)


def invoke_probe():
    r = lam.invoke(FunctionName=PROBE_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    raw = r["Payload"].read()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        return {"ok": False, "raw": raw[:2000].decode("utf-8", "ignore")}
    if payload.get("statusCode") != 200:
        return {"ok": False, "payload": payload}
    try:
        body = json.loads(payload["body"])
    except Exception:
        return {"ok": False, "payload": payload}
    return {"ok": True, "probes": body.get("probes", {})}


def delete_probe():
    try:
        lam.delete_function(FunctionName=PROBE_FN)
        return True
    except Exception:
        return False


def main():
    started = datetime.now(timezone.utc).isoformat()
    report = {"ops": 992, "started_at": started, "donor": DONOR_FN}

    try:
        env = get_donor_env(DONOR_FN)
        keep = {k: env[k] for k in ("FRED_KEY", "ALPHA_VANTAGE_KEY",
                                    "FMP_KEY") if k in env}
        report["env_keys_present"] = sorted(keep.keys())

        report["deploy"] = create_or_update_probe(keep)
        report["invoke"] = invoke_probe()

        probes = report["invoke"].get("probes", {})
        winners = {}
        for name, p in probes.items():
            if isinstance(p, dict) and p.get("ok"):
                winners[name] = {k: p.get(k) for k in
                                 ("n_values", "n_records", "status",
                                  "sample", "latest_date") if p.get(k) is not None}
            elif isinstance(p, list):
                wins = [x for x in p if x.get("ok")]
                if wins:
                    winners[name] = wins
        report["working_sources"] = winners
        report["n_working"] = len(winners)

        report["delete_probe_ok"] = delete_probe()

    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()

    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "992.json"
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        try:
            out_path.write_text(json.dumps(report, indent=2, default=str))
            print(f"Report: {out_path.relative_to(REPO_ROOT)}")
        except Exception as wex:
            print(f"Report write FAILED: {wex}")
        print("\n=== FULL REPORT JSON ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
