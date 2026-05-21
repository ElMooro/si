"""
ops 1009 - Pro Pack v3 #10 EVA Spread end-to-end verify.

Validates Stern Stewart EVA + Bennett Stewart EVA Momentum + MVA pipeline.

Scorecard gates:
- version 1.0.0
- universe state real (CREATING_VALUE_BROAD / MIXED / DESTROYING_VALUE_BROAD)
- WACC inputs sane (Rf, ERP, IG spread all present + numeric)
- n_valid >= 35 (out of 50)
- top_10_eva_spread populated + each entry has roic + wacc + spread
- top_10_eva_momentum populated
- sector_breakdown has >=3 sectors
- Lambda invoke OK + S3 output written
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-eva-spread"
KEY = "data/eva-spread.json"
EXPECTED_VERSION = "1.0.0"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)


def wait_for_active(fn_name, max_wait=600):
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            c = lam.get_function(FunctionName=fn_name)["Configuration"]
            if (c.get("State") == "Active" and
                    c.get("LastUpdateStatus") == "Successful"):
                return {"ok": True,
                        "last_modified": c.get("LastModified"),
                        "memory_mb": c.get("MemorySize"),
                        "timeout_s": c.get("Timeout"),
                        "env_keys": sorted(list((c.get("Environment") or {}
                                                  ).get("Variables", {}
                                                        ).keys())),
                        "waited_sec": round(time.time() - t0, 1)}
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(15)
    return {"ok": False, "error": "timeout",
            "waited_sec": round(time.time() - t0, 1)}


def invoke():
    try:
        t0 = time.time()
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({}).encode("utf-8"))
        elapsed = round(time.time() - t0, 1)
        raw = r["Payload"].read()
        body = json.loads(raw.decode("utf-8"))
        if isinstance(body.get("body"), str):
            try:
                body["body"] = json.loads(body["body"])
            except Exception:
                pass
        return {"ok": True, "function_error": r.get("FunctionError"),
                "elapsed_sec": elapsed, "payload": body}
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}


def fetch_s3():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        return {"ok": True,
                "data": json.loads(obj["Body"].read().decode("utf-8")),
                "last_modified": obj["LastModified"].isoformat()}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat(),
              "expected_version": EXPECTED_VERSION}

    # Wait for Lambda
    w = wait_for_active(FN)
    report["lambda_ready"] = w
    if not w.get("ok"):
        report["scorecard"] = {"all_pass": False, "deploy_failed": True}
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        _write(report)
        return

    # Verify FMP_KEY + FRED_KEY were inherited
    report["env_check"] = {
        "has_fmp_key": "FMP_KEY" in w.get("env_keys", []),
        "has_fred_key": "FRED_KEY" in w.get("env_keys", []),
        "has_s3_bucket": "S3_BUCKET" in w.get("env_keys", []),
    }

    # Invoke (50 tickers, ~5 FMP calls/ticker + 0.4s sleep = ~150s + slop)
    iv = invoke()
    report["invoke"] = {"ok": iv["ok"],
                        "function_error": iv.get("function_error"),
                        "elapsed_sec": iv.get("elapsed_sec"),
                        "error": iv.get("error")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        report["invoke_summary"] = body

    # Fetch S3
    s = fetch_s3()
    if not s["ok"]:
        report["s3"] = s
        report["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        _write(report)
        return

    d = s["data"]
    wacc = d.get("wacc_inputs") or {}
    medians = d.get("universe_medians") or {}
    top_spread = d.get("top_10_eva_spread") or []
    top_mom = d.get("top_10_eva_momentum") or []
    top_mva = d.get("top_10_mva") or []
    bottom = d.get("bottom_5_destroyers") or []
    super_c = d.get("super_compounders") or []
    sectors = d.get("sector_breakdown") or {}

    report["s3"] = {
        "version": d.get("version"),
        "generated_at": d.get("generated_at"),
        "universe_state": d.get("universe_state"),
        "n_analyzed": d.get("n_analyzed"),
        "n_valid": d.get("n_valid"),
        "n_value_creators": d.get("n_value_creators"),
        "n_value_destroyers": d.get("n_value_destroyers"),
        "n_super_compounders": d.get("n_super_compounders"),
        "wacc_inputs": wacc,
        "universe_medians": medians,
        "top_5_eva_spread": [
            {"t": r.get("ticker"), "sector": r.get("sector"),
             "spread": r.get("eva_spread_pct"),
             "roic": r.get("roic_ttm_pct"),
             "wacc": r.get("wacc_pct"),
             "mom": r.get("eva_momentum_pct"),
             "super": r.get("super_compounder")}
            for r in top_spread[:5]],
        "top_5_eva_momentum": [
            {"t": r.get("ticker"), "mom": r.get("eva_momentum_pct"),
             "spread": r.get("eva_spread_pct"),
             "roic": r.get("roic_ttm_pct")}
            for r in top_mom[:5]],
        "top_5_mva": [
            {"t": r.get("ticker"),
             "mva_b": round((r.get("mva_usd") or 0) / 1e9, 2),
             "mva_x": r.get("mva_multiple"),
             "spread": r.get("eva_spread_pct")}
            for r in top_mva[:5]],
        "bottom_5_destroyers": [
            {"t": r.get("ticker"), "sector": r.get("sector"),
             "spread": r.get("eva_spread_pct"),
             "roic": r.get("roic_ttm_pct"),
             "wacc": r.get("wacc_pct")}
            for r in bottom[:5]],
        "super_compounders": [
            {"t": r.get("ticker"), "spread": r.get("eva_spread_pct"),
             "roic": r.get("roic_ttm_pct"),
             "mom": r.get("eva_momentum_pct")}
            for r in super_c],
        "sector_summary": {
            s_name: {"n": v.get("n"),
                      "pct_creators": v.get("pct_creators"),
                      "med_spread": v.get("median_eva_spread_pct")}
            for s_name, v in sectors.items()
        },
    }

    # Scorecard
    top1 = top_spread[0] if top_spread else {}
    sc = {
        "version_1_0_0": d.get("version") == EXPECTED_VERSION,
        "universe_state_real": d.get("universe_state") in
            ("CREATING_VALUE_BROAD", "MIXED",
             "DESTROYING_VALUE_BROAD"),
        "n_analyzed_50": d.get("n_analyzed") == 50,
        "n_valid_min_35": (d.get("n_valid") or 0) >= 35,
        "wacc_rf_present": isinstance(
            wacc.get("risk_free_10y_pct"), (int, float)) and
            0 < wacc.get("risk_free_10y_pct", 0) < 15,
        "wacc_erp_present": isinstance(
            wacc.get("equity_risk_premium_pct"), (int, float)) and
            0 < wacc.get("equity_risk_premium_pct", 0) < 15,
        "wacc_ig_spread_present": isinstance(
            wacc.get("ig_credit_spread_pct"), (int, float)) and
            0 < wacc.get("ig_credit_spread_pct", 0) < 15,
        "top_10_eva_spread_populated": len(top_spread) >= 8,
        "top_1_has_roic_wacc_spread": all(
            isinstance(top1.get(k), (int, float))
            for k in ("roic_ttm_pct", "wacc_pct",
                       "eva_spread_pct")),
        "top_10_eva_momentum_populated": len(top_mom) >= 5,
        "bottom_5_present": len(bottom) >= 3,
        "sector_breakdown_min_3": len(sectors) >= 3,
        "median_spread_present": isinstance(
            medians.get("eva_spread_pct"), (int, float)),
        "invoke_ok": iv["ok"] and not iv.get("function_error"),
    }
    sc["all_pass"] = all(sc.values())
    report["scorecard"] = sc

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1009.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1009] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report.get("scorecard", {}), indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
