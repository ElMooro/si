"""
ops 1000 - Pro Pack v3 Phase B kickoff.

Three actions:
1. Quick re-verify justhodl-gf-value (v1.0.1 fixes for EV/EBIT join + MoS bounds)
   - ops 999 failed only on CI wait race; deploy did complete successfully
   - Just invoke + fetch S3 + check sample tickers
2. Probe FMP /stable/ endpoints needed for StarMine (#4):
   - analyst-estimates, earnings, grades, grades-consensus,
     price-target-consensus, analyst-stock-recommendations
3. Probe MOVE / bond-vol data availability for #5:
   - FMP /stable/quote on ^MOVE (likely 403)
   - Polygon previous-day for I:MOVE (likely 403 - no index access)
   - FRED candidates for bond vol proxies (DGS10, DGS2, T10Y2Y daily realized vol)

All three probes use a single temp Lambda for the FMP/Polygon calls so the
sandbox stays clean. Writes single ops/reports/1000.json.
"""
import json, sys, time, traceback, zipfile, io
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
GF_FN = "justhodl-gf-value"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)
iam_role = "arn:aws:iam::857687956942:role/lambda-execution-role"


def invoke(fn, payload=None):
    p = json.dumps(payload or {}).encode("utf-8")
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=p)
        raw = r["Payload"].read()
        body = json.loads(raw.decode("utf-8"))
        if isinstance(body.get("body"), str):
            try: body["body"] = json.loads(body["body"])
            except Exception: pass
        return {"ok": True, "function_error": r.get("FunctionError"), "payload": body}
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}


def fetch_s3(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return {"ok": True, "data": json.loads(obj["Body"].read().decode("utf-8")),
                "last_modified": obj["LastModified"].isoformat()}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


# ---------- Action 1: re-verify gf-value v1.0.1 ----------
def verify_gf_value():
    out = {"started_at": datetime.now(timezone.utc).isoformat()}
    iv = invoke(GF_FN, {})
    out["invoke"] = {"ok": iv["ok"], "function_error": iv.get("function_error")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        out["invoke_summary"] = {k: body.get(k) for k in
                                 ["ok", "version", "n_valid", "n_total",
                                  "universe_state", "median_mos_pct"]}
    s = fetch_s3("justhodl-dashboard-live", "gf-value/data.json")
    if s["ok"]:
        d = s["data"]
        all_t = d.get("all_tickers", [])
        mos = [t["mos_pct"] for t in all_t if t.get("mos_pct") is not None]
        n_evebit = sum(1 for t in all_t if t.get("evebit_fair_value") is not None)
        # Outlier-corruption check: |gf/price| or |price/gf| > 20
        n_outlier = sum(1 for t in all_t
                        if t.get("gf_value") and t.get("price")
                        and max(t["gf_value"], t["price"]) /
                           max(0.01, min(t["gf_value"], t["price"])) > 20)
        out["s3"] = {
            "version": d.get("version"),
            "generated_at": d.get("generated_at"),
            "universe_state": d.get("universe_state"),
            "n_valid": d.get("n_valid"),
            "n_total": d.get("n_total"),
            "counts": d.get("counts"),
            "min_mos_pct": min(mos) if mos else None,
            "max_mos_pct": max(mos) if mos else None,
            "n_mos_outside_95": sum(1 for m in mos if m < -95 or m > 95),
            "n_outlier_gt_20x": n_outlier,
            "n_evebit_populated": n_evebit,
            "n_evebit_pct": round(100 * n_evebit / max(1, len(all_t)), 1),
            "deepest_value_top5": [
                {"t": t["ticker"], "px": t["price"], "gfv": t["gf_value"],
                 "mos_pct": t["mos_pct"], "dcf": t.get("dcf_fair_value"),
                 "evebit": t.get("evebit_fair_value"),
                 "graham": t.get("graham_fair_value")}
                for t in (d.get("deepest_value") or [])[:5]
            ],
        }
        out["last_modified"] = s["last_modified"]
        # Scorecard
        sc = {
            "version_1_0_1": d.get("version") == "1.0.1",
            "mos_within_95": out["s3"]["n_mos_outside_95"] == 0,
            "no_corrupt_20x_outliers": out["s3"]["n_outlier_gt_20x"] == 0,
            "evebit_lens_majority_populated": out["s3"]["n_evebit_pct"] >= 60.0,
            "n_valid_min_300": (d.get("n_valid") or 0) >= 300,
            "universe_state_real": d.get("universe_state", "").startswith("MARKET_"),
            "deepest_value_25": len(d.get("deepest_value") or []) == 25,
            "invoke_ok": iv["ok"] and not iv.get("function_error"),
        }
        sc["all_pass"] = all(sc.values())
        out["scorecard"] = sc
    else:
        out["s3"] = s
    return out


# ---------- Actions 2+3: temp probe Lambda ----------
PROBE_LAMBDA_CODE = r'''
import os, json, urllib.request, urllib.error, time
FMP_KEY = os.environ.get("FMP_KEY", "")
POLY_KEY = os.environ.get("POLYGON_KEY", "")
FRED_KEY = os.environ.get("FRED_KEY", "")

FMP_ENDPOINTS = [
    "analyst-estimates?symbol=AAPL&limit=4",
    "earnings?symbol=AAPL&limit=8",
    "grades?symbol=AAPL&limit=20",
    "grades-consensus?symbol=AAPL",
    "price-target-consensus?symbol=AAPL",
    "price-target-latest-news?page=0&limit=5",
    "analyst-stock-recommendations?symbol=AAPL",
    "earnings-surprises?symbol=AAPL",
    "quote?symbol=%5EMOVE",   # MOVE Index
    "quote?symbol=%5EVIX",
    "quote?symbol=%5EVXN",
    "quote?symbol=%5ETNX",   # 10Y Treasury yield
]
POLYGON_PROBES = [
    "v2/aggs/ticker/I:MOVE/prev",
    "v2/aggs/ticker/I:VIX/prev",
    "v2/aggs/ticker/TLT/prev",
]
FRED_SERIES = [
    "DGS10", "DGS2", "T10Y2Y", "BAMLH0A0HYM2",
    "BAMLC0A4CBBB", "AAA10Y", "MORTGAGE30US",
    "ICE_BAML_MOVE",  # speculative
    "BOGZ1FL072051001",  # also speculative
    "DCOILWTICO",
    "WALCL",
]

def http_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", errors="ignore")
            try: body = json.loads(raw)
            except Exception: body = raw[:300]
            return {"ok": True, "status": r.status, "body_kind": type(body).__name__,
                    "body_size": len(raw),
                    "sample": (body if isinstance(body, dict) else
                               (body[:2] if isinstance(body, list) else str(body)[:300]))}
    except urllib.error.HTTPError as e:
        msg = ""
        try: msg = e.read().decode("utf-8", errors="ignore")[:200]
        except Exception: pass
        return {"ok": False, "status": e.code, "error": msg}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}

def lambda_handler(event, context):
    out = {"fmp": {}, "polygon": {}, "fred": {}}
    if not FMP_KEY:
        out["fmp"] = {"error": "FMP_KEY not set"}
    else:
        for ep in FMP_ENDPOINTS:
            sep = "&" if "?" in ep else "?"
            url = f"https://financialmodelingprep.com/stable/{ep}{sep}apikey={FMP_KEY}"
            out["fmp"][ep] = http_json(url)
            time.sleep(0.25)
    if not POLY_KEY:
        out["polygon"] = {"error": "POLYGON_KEY not set"}
    else:
        for ep in POLYGON_PROBES:
            url = f"https://api.polygon.io/{ep}?apiKey={POLY_KEY}"
            out["polygon"][ep] = http_json(url)
            time.sleep(0.25)
    if not FRED_KEY:
        out["fred"] = {"error": "FRED_KEY not set"}
    else:
        for s in FRED_SERIES:
            url = (f"https://api.stlouisfed.org/fred/series?series_id={s}"
                   f"&api_key={FRED_KEY}&file_type=json")
            out["fred"][s] = http_json(url)
            time.sleep(0.2)
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def build_probe_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o755 << 16
        zf.writestr(zi, PROBE_LAMBDA_CODE)
    return buf.getvalue()


def deploy_and_run_probe():
    fn = "justhodl-tmp-v3-probe-1000"
    zip_bytes = build_probe_zip()
    # Inherit FMP/Polygon/FRED keys from a working donor (justhodl-earnings-quality)
    donor = lam.get_function_configuration(FunctionName="justhodl-earnings-quality")
    env_donor = donor.get("Environment", {}).get("Variables", {}) or {}
    # also pull FRED from cross-asset-rv
    try:
        donor2 = lam.get_function_configuration(FunctionName="justhodl-cross-asset-rv")
        env2 = donor2.get("Environment", {}).get("Variables", {}) or {}
    except Exception:
        env2 = {}
    env_out = {}
    for src in (env_donor, env2):
        for k in ("FMP_KEY", "POLYGON_KEY", "FRED_KEY"):
            v = src.get(k)
            if v and not env_out.get(k):
                env_out[k] = v
    # create or update
    try:
        lam.create_function(
            FunctionName=fn, Runtime="python3.12",
            Role=iam_role, Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes}, Timeout=300, MemorySize=512,
            Environment={"Variables": env_out},
            Description="ops 1000 temp probe for StarMine + MOVE",
        )
        # wait active
        for _ in range(20):
            s = lam.get_function(FunctionName=fn)["Configuration"]["State"]
            if s == "Active": break
            time.sleep(2)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes)
        time.sleep(2)
        lam.update_function_configuration(
            FunctionName=fn, Environment={"Variables": env_out})
        time.sleep(2)
    r = invoke(fn, {})
    try:
        lam.delete_function(FunctionName=fn)
    except Exception:
        pass
    return r


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    try:
        report["gf_value_v101"] = verify_gf_value()
    except Exception as e:
        report["gf_value_v101"] = {"error": str(e)[:400],
                                    "trace": traceback.format_exc()[:1200]}
    try:
        probe = deploy_and_run_probe()
        if probe.get("ok"):
            body = probe.get("payload", {}).get("body")
            if isinstance(body, str):
                try: body = json.loads(body)
                except Exception: pass
            # Summarize each section to keep report under 100KB
            def summarize(section_name, section_data):
                if not isinstance(section_data, dict):
                    return section_data
                out = {}
                for k, v in section_data.items():
                    if isinstance(v, dict):
                        out[k] = {
                            "ok": v.get("ok"),
                            "status": v.get("status"),
                            "body_kind": v.get("body_kind"),
                            "body_size": v.get("body_size"),
                            "error": v.get("error"),
                            "sample_preview": str(v.get("sample"))[:500]
                                if v.get("sample") is not None else None,
                        }
                    else:
                        out[k] = v
                return out
            report["fmp_probes"] = summarize("fmp", body.get("fmp", {}))
            report["polygon_probes"] = summarize("polygon", body.get("polygon", {}))
            report["fred_probes"] = summarize("fred", body.get("fred", {}))
        else:
            report["probe_error"] = probe.get("error")
    except Exception as e:
        report["probe_error"] = str(e)[:400]
        report["probe_trace"] = traceback.format_exc()[:1200]

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1000.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1000] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps({
        "gf_value_all_pass": report.get("gf_value_v101", {})
            .get("scorecard", {}).get("all_pass"),
        "fmp_probes_n": len(report.get("fmp_probes", {})),
        "polygon_probes_n": len(report.get("polygon_probes", {})),
        "fred_probes_n": len(report.get("fred_probes", {})),
    }, indent=2))


if __name__ == "__main__":
    try: main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
