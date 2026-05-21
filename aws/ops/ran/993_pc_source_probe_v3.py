"""
ops 993 - PC source probe v3: correct donor + synthetic P/C from FMP.

ops 992 findings:
- env donor justhodl-earnings-pead only has FMP_KEY (no FRED, no AV)
- Yahoo Finance 404 for ALL P/C symbols (^CPC, ^CPCE, $CPC, $CPCE)
- CBOE direct CSV 403/404 across all 3 URL patterns
- => Public free CBOE P/C feeds may be genuinely dead

This v3:
1. Uses correct donor justhodl-cross-asset-rv (has FRED_KEY + FMP_KEY)
   to actually test FRED CBOEEQUITYPCRATIO
2. Tests institutional-grade pivot: synthetic P/C from FMP options data
   - Aggregate put volume / call volume across SPY major-strike options
   - This is what professional shops do (they compute their own P/C
     metric from raw options data, not relying on CBOE's published
     number which has known sample biases)
3. Tests FMP option-chain endpoint shape

If FRED works -> simple fix (rewire donor in put-call-extreme engine).
If FRED dead too -> pivot to synthetic P/C (more institutional anyway).
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
DONOR_FN = "justhodl-cross-asset-rv"  # has FRED_KEY + FMP_KEY confirmed
PROBE_FN = "justhodl-ops-993-pc-probe"

cfg = Config(read_timeout=180, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)


PROBE_SOURCE = r"""
import json
import os
import urllib.parse
import urllib.request

FRED_KEY = os.environ.get("FRED_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "")

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
SIMPLE_UA = "justhodl/1.0"


def _fetch(url, ua=SIMPLE_UA, timeout=15):
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
        body, status = _fetch(url)
        d = json.loads(body)
        if "error_message" in d:
            return {"ok": False, "error": d["error_message"], "status": status}
        obs = d.get("observations", [])
        values = [float(o["value"]) for o in obs
                  if o.get("value") and o["value"] != "."]
        return {"ok": len(values) >= 10, "n_values": len(values),
                "sample": values[:5], "status": status,
                "latest_date": (obs[0].get("date") if obs else None)}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def probe_fmp_endpoint(endpoint_path):
    # Test if an FMP /stable endpoint returns data for SPY options
    if not FMP_KEY:
        return {"ok": False, "error": "no_fmp_key"}
    url = f"https://financialmodelingprep.com/stable/{endpoint_path}&apikey={FMP_KEY}"
    try:
        body, status = _fetch(url, timeout=20)
        # Try JSON
        try:
            d = json.loads(body)
        except Exception:
            return {"ok": False, "status": status, "raw_head": body[:300]}
        # Check shape
        if isinstance(d, dict) and ("Error Message" in d or "error" in d):
            return {"ok": False, "status": status,
                    "msg": str(d)[:300]}
        if isinstance(d, list):
            return {"ok": len(d) > 0, "status": status, "n_records": len(d),
                    "sample_keys": (list(d[0].keys())[:15] if d else []),
                    "first_record": (d[0] if d else None)}
        if isinstance(d, dict):
            return {"ok": True, "status": status, "shape": "dict",
                    "top_keys": list(d.keys())[:20], "sample": str(d)[:500]}
        return {"ok": False, "status": status, "shape": "other"}
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": f"HTTP {e.code}", "url_tail": endpoint_path}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def compute_synthetic_pc(symbol="SPY"):
    # Real synthetic P/C: pull SPY options chain, sum put-vol/call-vol
    if not FMP_KEY:
        return {"ok": False, "error": "no_fmp_key"}
    # Try several FMP option endpoints (FMP moved options into /stable/)
    endpoints = [
        f"options-chain?symbol={symbol}&apikey={FMP_KEY}",
        f"options/chain?symbol={symbol}&apikey={FMP_KEY}",
        f"historical/options-chain?symbol={symbol}&apikey={FMP_KEY}",
        f"options-historical-data?symbol={symbol}&apikey={FMP_KEY}",
    ]
    results = []
    for ep in endpoints:
        url = f"https://financialmodelingprep.com/stable/{ep}"
        try:
            body, status = _fetch(url, timeout=30)
            try:
                d = json.loads(body)
            except Exception:
                results.append({"ep": ep[:40], "status": status,
                                "raw_head": body[:200]})
                continue
            if isinstance(d, dict) and ("Error Message" in d):
                results.append({"ep": ep[:40], "status": status,
                                "err": d.get("Error Message")[:200]})
                continue
            if isinstance(d, list) and len(d) > 0:
                # Try to derive P/C from this
                puts_vol = 0
                calls_vol = 0
                puts_oi = 0
                calls_oi = 0
                for row in d:
                    # FMP options row schema varies - look for type/side + volume
                    typ = str(row.get("type") or row.get("optionType") or
                              row.get("side") or "").upper()
                    vol = (row.get("volume") or row.get("totalVolume") or
                           row.get("dailyVolume") or 0) or 0
                    oi = (row.get("openInterest") or row.get("oi") or 0) or 0
                    if typ.startswith("P") or "PUT" in typ:
                        puts_vol += float(vol or 0)
                        puts_oi += float(oi or 0)
                    elif typ.startswith("C") or "CALL" in typ:
                        calls_vol += float(vol or 0)
                        calls_oi += float(oi or 0)
                pc_vol = (puts_vol / calls_vol) if calls_vol > 0 else None
                pc_oi = (puts_oi / calls_oi) if calls_oi > 0 else None
                results.append({"ep": ep[:40], "status": status,
                                "n_rows": len(d),
                                "puts_vol": puts_vol, "calls_vol": calls_vol,
                                "pc_volume_ratio": pc_vol,
                                "pc_oi_ratio": pc_oi,
                                "sample_row": d[0] if d else None,
                                "ok": True})
                return results  # short-circuit on success
        except Exception as e:
            results.append({"ep": ep[:40], "error": str(e)[:150]})
    return results


def lambda_handler(event, context):
    out = {
        "env_keys": {"FRED": bool(FRED_KEY), "FMP": bool(FMP_KEY)},
        "FRED_CBOEEQUITYPCRATIO": probe_fred("CBOEEQUITYPCRATIO"),
        "FRED_PUTCALL": probe_fred("PUTCALL"),
        "FRED_VIXCLS_sanity": probe_fred("VIXCLS"),
        "FMP_synthetic_pc": compute_synthetic_pc("SPY"),
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
            FunctionName=PROBE_FN, Timeout=180, MemorySize=256,
            Environment={"Variables": env_vars})
    else:
        lam.create_function(
            FunctionName=PROBE_FN, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": z}, Timeout=180, MemorySize=256,
            Environment={"Variables": env_vars},
            Description="ops 993 PC source probe v3 (temp)")
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
    report = {"ops": 993, "started_at": started, "donor": DONOR_FN}
    try:
        env = get_donor_env(DONOR_FN)
        report["donor_env_keys_present"] = sorted(env.keys())
        keep = {k: env[k] for k in ("FRED_KEY", "FMP_KEY") if k in env}
        report["env_keys_used"] = sorted(keep.keys())
        report["deploy"] = create_or_update_probe(keep)
        report["invoke"] = invoke_probe()
        probes = report["invoke"].get("probes", {})
        report["FRED_works"] = (probes.get("FRED_CBOEEQUITYPCRATIO", {})
                                .get("ok", False))
        report["FRED_sanity_works"] = (probes.get("FRED_VIXCLS_sanity", {})
                                       .get("ok", False))
        synth = probes.get("FMP_synthetic_pc", [])
        if isinstance(synth, list):
            wins = [x for x in synth if x.get("ok")]
            report["FMP_synthetic_works"] = len(wins) > 0
            report["FMP_synthetic_winners"] = wins
        else:
            report["FMP_synthetic_works"] = bool(synth.get("ok"))
        report["delete_probe_ok"] = delete_probe()
    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "993.json"
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
