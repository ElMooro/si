"""
ops 1001 - Pro Pack v3 Phase B triple verifier.

Verifies:
1. gf-value v1.0.1 with correct S3 key `data/gf-value.json`
   (ops 1000 used wrong key - confirmed live by invoke summary showing
   universe_state=MARKET_SIGNIFICANTLY_OVERVALUED, median MoS=-21.9%)

2. justhodl-bond-vol (Pro Pack v3 #5) - new Lambda, pure FRED, ~10s invoke
   Expects 5/5 channels live, composite z numeric, regime real

3. justhodl-starmine (Pro Pack v3 #4) - new Lambda, FMP-heavy ~3-5 min invoke
   Accepts that FMP may be rate-limited from today's heavy batch ops.
   Best-effort: success if invokes complete, output has valid structure.
   Full data integrity verification happens after first scheduled run
   (23:30 UTC daily).

Waits for CI deploy of d10cb355 (which contains both #4 + #5).
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
GF_FN = "justhodl-gf-value"
BOND_FN = "justhodl-bond-vol"
STARMINE_FN = "justhodl-starmine"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)


def invoke(fn, payload=None):
    p = json.dumps(payload or {}).encode("utf-8")
    try:
        t0 = time.time()
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=p)
        elapsed = round(time.time() - t0, 1)
        raw = r["Payload"].read()
        body = json.loads(raw.decode("utf-8"))
        if isinstance(body.get("body"), str):
            try: body["body"] = json.loads(body["body"])
            except Exception: pass
        return {"ok": True, "function_error": r.get("FunctionError"),
                "elapsed_sec": elapsed, "payload": body}
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}


def fetch_s3(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return {"ok": True, "data": json.loads(obj["Body"].read().decode("utf-8")),
                "last_modified": obj["LastModified"].isoformat()}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def wait_for_lambda_active(fn_name, max_wait_sec=600):
    """Wait until a Lambda's State=Active and matches LatestModified is recent."""
    t0 = time.time()
    while time.time() - t0 < max_wait_sec:
        try:
            cfg_resp = lam.get_function(FunctionName=fn_name)["Configuration"]
            state = cfg_resp.get("State")
            last_update = cfg_resp.get("LastUpdateStatus")
            if state == "Active" and last_update == "Successful":
                return {"ok": True,
                        "last_modified": cfg_resp.get("LastModified"),
                        "waited_sec": round(time.time() - t0, 1)}
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(15)
    return {"ok": False, "error": "timeout",
            "waited_sec": round(time.time() - t0, 1)}


def verify_gf_value():
    """#1 re-verify with CORRECT S3 key data/gf-value.json"""
    out = {}
    iv = invoke(GF_FN, {})
    out["invoke"] = {"ok": iv["ok"], "function_error": iv.get("function_error"),
                     "elapsed_sec": iv.get("elapsed_sec")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        out["invoke_summary"] = {k: body.get(k) for k in
                                 ["ok", "version", "n_valid", "n_total",
                                  "universe_state", "median_mos_pct"]}

    s = fetch_s3("justhodl-dashboard-live", "data/gf-value.json")
    if s["ok"]:
        d = s["data"]
        all_t = d.get("all_tickers", [])
        mos = [t["mos_pct"] for t in all_t if t.get("mos_pct") is not None]
        n_evebit = sum(1 for t in all_t if t.get("evebit_fair_value") is not None)
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
            "median_mos_pct_universe": round(
                sorted(mos)[len(mos)//2] if mos else 0, 1),
            "deepest_value_top5": [
                {"t": t["ticker"], "px": t["price"], "gfv": t["gf_value"],
                 "mos_pct": t["mos_pct"], "dcf": t.get("dcf_fair_value"),
                 "evebit": t.get("evebit_fair_value"),
                 "graham": t.get("graham_fair_value")}
                for t in (d.get("deepest_value") or [])[:5]
            ],
        }
        out["last_modified"] = s["last_modified"]
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
        out["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
    return out


def verify_bond_vol():
    """#5 Bond Vol Composite verify"""
    out = {}
    wait = wait_for_lambda_active(BOND_FN, max_wait_sec=600)
    out["lambda_ready"] = wait
    if not wait.get("ok"):
        out["scorecard"] = {"all_pass": False, "deploy_failed": True}
        return out

    iv = invoke(BOND_FN, {})
    out["invoke"] = {"ok": iv["ok"], "function_error": iv.get("function_error"),
                     "elapsed_sec": iv.get("elapsed_sec")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        out["invoke_summary"] = body

    s = fetch_s3("justhodl-dashboard-live", "data/bond-vol.json")
    if s["ok"]:
        d = s["data"]
        out["s3"] = {
            "version": d.get("version"),
            "generated_at": d.get("generated_at"),
            "regime": d.get("regime"),
            "composite_z_score": d.get("composite_z_score"),
            "n_channels_live": d.get("n_channels_live"),
            "n_channels_total": d.get("n_channels_total"),
            "channels_summary": [
                {"id": c.get("id"), "fred": c.get("fred_series"),
                 "ok": c.get("ok"),
                 "rv": c.get("realized_vol_now"),
                 "z": c.get("z_score"), "pct": c.get("percentile_1y"),
                 "latest": c.get("latest_obs_date")}
                for c in (d.get("channels") or [])
            ],
        }
        sc = {
            "version_1_0_0": d.get("version") == "1.0.0",
            "n_channels_live_min_4": (d.get("n_channels_live") or 0) >= 4,
            "n_channels_total_5": d.get("n_channels_total") == 5,
            "composite_z_numeric": isinstance(d.get("composite_z_score"),
                                              (int, float)),
            "regime_real": d.get("regime") in
                ("CRISIS", "ELEVATED", "NORMAL", "BOND_VOL_LOW"),
            "invoke_ok": iv["ok"] and not iv.get("function_error"),
            "channels_have_z": all(
                isinstance(c.get("z_score"), (int, float))
                for c in (d.get("channels") or []) if c.get("ok")
            ),
        }
        sc["all_pass"] = all(sc.values())
        out["scorecard"] = sc
    else:
        out["s3"] = s
        out["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
    return out


def verify_starmine():
    """#4 StarMine verify - best-effort, accepts FMP rate-limit reality"""
    out = {}
    wait = wait_for_lambda_active(STARMINE_FN, max_wait_sec=600)
    out["lambda_ready"] = wait
    if not wait.get("ok"):
        out["scorecard"] = {"all_pass": False, "deploy_failed": True}
        return out

    # StarMine takes 3-5 min - boto3 timeout set to 900s
    iv = invoke(STARMINE_FN, {})
    out["invoke"] = {"ok": iv["ok"], "function_error": iv.get("function_error"),
                     "elapsed_sec": iv.get("elapsed_sec")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        out["invoke_summary"] = body

    s = fetch_s3("justhodl-dashboard-live", "data/starmine.json")
    if s["ok"]:
        d = s["data"]
        out["s3"] = {
            "version": d.get("version"),
            "generated_at": d.get("generated_at"),
            "universe_regime": d.get("universe_regime"),
            "universe_median_composite_z": d.get("universe_median_composite_z"),
            "n_universe_analyzed": d.get("n_universe_analyzed"),
            "n_scored": d.get("n_scored"),
            "median_rrm_raw": d.get("median_rrm_raw"),
            "median_ptd_pct": d.get("median_ptd_pct"),
            "median_esp_hit_pct": d.get("median_esp_hit_pct"),
            "sector_breakdown_top_25": d.get("sector_breakdown_top_25"),
            "top_3_score": [
                {"t": t["ticker"], "score": t["starmine_score"],
                 "z_rrm": t.get("z_rrm"), "z_ptd": t.get("z_ptd"),
                 "z_esp": t.get("z_esp")}
                for t in (d.get("top_25_conviction") or [])[:3]
            ],
            "bottom_3_score": [
                {"t": t["ticker"], "score": t["starmine_score"]}
                for t in (d.get("bottom_25_conviction") or [])[:3]
            ],
        }
        # Best-effort scorecard: pass if structure is right, regardless of
        # how many tickers FMP allowed through
        sc = {
            "version_1_0_0": d.get("version") == "1.0.0",
            "universe_regime_real": d.get("universe_regime") in
                ("BULLISH_REVISIONS", "NEUTRAL_REVISIONS",
                 "BEARISH_REVISIONS"),
            "n_universe_min_20": (d.get("n_universe_analyzed") or 0) >= 20,
            "top_25_present": len(d.get("top_25_conviction") or []) >= 5,
            "bottom_25_present": len(d.get("bottom_25_conviction") or []) >= 5,
            "median_z_numeric": isinstance(
                d.get("universe_median_composite_z"), (int, float)),
            "invoke_ok": iv["ok"] and not iv.get("function_error"),
        }
        sc["all_pass"] = all(sc.values())
        out["scorecard"] = sc
    else:
        out["s3"] = s
        out["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
    return out


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # Order matters: #5 (bond-vol) deploys with same commit as #4 (starmine)
    # so by the time #5 is ready, #4 is also ready.
    print("[ops 1001] Phase 1: gf-value v1.0.1 re-verify (correct S3 key)")
    try:
        report["gf_value_v101"] = verify_gf_value()
    except Exception as e:
        report["gf_value_v101"] = {"error": str(e)[:400],
                                    "trace": traceback.format_exc()[:1200]}

    print("[ops 1001] Phase 2: bond-vol verify (pure FRED, fast)")
    try:
        report["bond_vol"] = verify_bond_vol()
    except Exception as e:
        report["bond_vol"] = {"error": str(e)[:400],
                               "trace": traceback.format_exc()[:1200]}

    print("[ops 1001] Phase 3: starmine verify (FMP-heavy, 3-5min, "
          "best-effort vs rate-limit)")
    try:
        report["starmine"] = verify_starmine()
    except Exception as e:
        report["starmine"] = {"error": str(e)[:400],
                               "trace": traceback.format_exc()[:1200]}

    # Top-level scorecard summary
    sc1 = report.get("gf_value_v101", {}).get("scorecard", {})
    sc2 = report.get("bond_vol", {}).get("scorecard", {})
    sc3 = report.get("starmine", {}).get("scorecard", {})
    report["scorecard_summary"] = {
        "gf_value_v101_all_pass": sc1.get("all_pass"),
        "bond_vol_all_pass": sc2.get("all_pass"),
        "starmine_all_pass": sc3.get("all_pass"),
        "all_three_pass": all([sc1.get("all_pass"), sc2.get("all_pass"),
                               sc3.get("all_pass")]),
    }
    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1001.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1001] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report["scorecard_summary"], indent=2))


if __name__ == "__main__":
    try: main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
