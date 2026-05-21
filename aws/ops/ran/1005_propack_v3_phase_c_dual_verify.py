"""
ops 1005 - Pro Pack v3 Phase C dual verify (#7 Predictability + #8 Smart Beta).

Verifies:
- justhodl-predictability: 1-5 star moat ranks from 10y rev/EPS R^2
- justhodl-smart-beta: 4-factor (Value/Quality/Momentum/LowVol) composite

For each:
- Wait for Lambda Active
- Invoke RequestResponse (long timeout - FMP-heavy)
- Fetch S3 output
- Score against expected structure
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
PRED_FN = "justhodl-predictability"
PRED_KEY = "data/predictability.json"
SB_FN = "justhodl-smart-beta"
SB_KEY = "data/smart-beta.json"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)


def invoke(fn, payload=None):
    p = json.dumps(payload or {}).encode("utf-8")
    try:
        t0 = time.time()
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=p)
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


def fetch_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return {"ok": True,
                "data": json.loads(obj["Body"].read().decode("utf-8")),
                "last_modified": obj["LastModified"].isoformat()}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def wait_for_active(fn_name, max_wait=600):
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            c = lam.get_function(FunctionName=fn_name)["Configuration"]
            if (c.get("State") == "Active" and
                    c.get("LastUpdateStatus") == "Successful"):
                return {"ok": True,
                        "last_modified": c.get("LastModified"),
                        "waited_sec": round(time.time() - t0, 1)}
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(15)
    return {"ok": False, "error": "timeout",
            "waited_sec": round(time.time() - t0, 1)}


# ----- Per-Lambda verifiers -----
def verify_predictability():
    ret = {}
    w = wait_for_active(PRED_FN)
    ret["lambda_ready"] = w
    if not w.get("ok"):
        ret["scorecard"] = {"all_pass": False, "deploy_failed": True}
        return ret

    iv = invoke(PRED_FN, {})
    ret["invoke"] = {"ok": iv["ok"],
                     "function_error": iv.get("function_error"),
                     "elapsed_sec": iv.get("elapsed_sec")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        ret["invoke_summary"] = body

    s = fetch_s3(PRED_KEY)
    if not s["ok"]:
        ret["s3"] = s
        ret["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
        return ret

    d = s["data"]
    sd = d.get("star_distribution") or {}
    vd = d.get("valuation_distribution") or {}
    sweet = d.get("sweet_spot_picks") or []
    elite = d.get("elite_moats") or []
    top_pred = d.get("most_predictable_top_15") or []

    ret["s3"] = {
        "version": d.get("version"),
        "generated_at": d.get("generated_at"),
        "universe_state": d.get("universe_state"),
        "n_analyzed": d.get("n_analyzed"),
        "n_sweet_spot": d.get("n_sweet_spot"),
        "n_elite_moats": d.get("n_elite_moats"),
        "star_distribution": sd,
        "valuation_distribution": vd,
        "top_3_most_predictable": [
            {"t": x.get("ticker"), "stars": x.get("stars"),
             "comp_r2": x.get("composite_r2"),
             "val": x.get("valuation"), "pe": x.get("pe_ttm")}
            for x in top_pred[:3]],
        "elite_5star_sample": [
            {"t": x.get("ticker"), "rev_r2": x.get("rev_r2"),
             "eps_r2": x.get("eps_r2"), "val": x.get("valuation")}
            for x in elite[:3]],
        "sweet_spot_sample": [
            {"t": x.get("ticker"), "pe": x.get("pe_ttm"),
             "rev_r2": x.get("rev_r2"), "eps_r2": x.get("eps_r2")}
            for x in sweet[:3]],
        "sector_breakdown": d.get("sector_breakdown"),
    }
    sc = {
        "version_1_0_0": d.get("version") == "1.0.0",
        "universe_state_real": d.get("universe_state") in
            ("HIGH_MOAT_CONCENTRATION", "BALANCED_PREDICTABILITY",
             "LOW_MOAT_CONCENTRATION"),
        "n_analyzed_min_30": (d.get("n_analyzed") or 0) >= 30,
        "star_distribution_populated":
            sum(v for k, v in sd.items() if k.endswith("_star")) >= 30,
        "valuation_distribution_populated":
            (vd.get("CHEAP", 0) + vd.get("FAIR", 0) + vd.get("RICH", 0)) >= 30,
        "elite_or_pred_present":
            len(elite) > 0 or len(top_pred) > 0,
        "all_tickers_have_attempt": len(d.get("all_tickers") or []) >= 30,
        "invoke_ok": iv["ok"] and not iv.get("function_error"),
    }
    sc["all_pass"] = all(sc.values())
    ret["scorecard"] = sc
    return ret


def verify_smart_beta():
    ret = {}
    w = wait_for_active(SB_FN)
    ret["lambda_ready"] = w
    if not w.get("ok"):
        ret["scorecard"] = {"all_pass": False, "deploy_failed": True}
        return ret

    iv = invoke(SB_FN, {})
    ret["invoke"] = {"ok": iv["ok"],
                     "function_error": iv.get("function_error"),
                     "elapsed_sec": iv.get("elapsed_sec")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        ret["invoke_summary"] = body

    s = fetch_s3(SB_KEY)
    if not s["ok"]:
        ret["s3"] = s
        ret["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
        return ret

    d = s["data"]
    fm = d.get("factor_medians") or {}
    top25 = d.get("top_25_diversified") or []
    div = d.get("diversified_factor_leaders") or []

    ret["s3"] = {
        "version": d.get("version"),
        "generated_at": d.get("generated_at"),
        "factor_regime": d.get("factor_regime"),
        "leading_factor": d.get("leading_factor"),
        "lagging_factor": d.get("lagging_factor"),
        "factor_medians": fm,
        "factor_spread_points": d.get("factor_spread_points"),
        "n_analyzed": d.get("n_analyzed"),
        "n_valid": d.get("n_valid"),
        "n_diversified_leaders": d.get("n_diversified_leaders"),
        "top_3_composite": [
            {"t": x.get("ticker"), "score": x.get("composite"),
             "val_p": x.get("value_pct"), "qual_p": x.get("quality_pct"),
             "mom_p": x.get("momentum_pct"), "lv_p": x.get("low_vol_pct")}
            for x in top25[:3]],
        "bottom_3_composite": [
            {"t": x.get("ticker"), "score": x.get("composite")}
            for x in (d.get("bottom_25_laggards") or [])[:3]],
        "factor_leader_samples": {
            f: [{"t": x.get("ticker"), "pct": x.get(f + "_pct")}
                for x in (d.get("factor_leaders") or {}).get(f, [])[:3]]
            for f in ("value", "quality", "momentum", "low_vol")
        },
        "sector_breakdown": d.get("sector_breakdown"),
    }
    sc = {
        "version_1_0_0": d.get("version") == "1.0.0",
        "factor_regime_real": isinstance(d.get("factor_regime"), str) and
            ("LEADERSHIP" in d.get("factor_regime", "") or
             "TILT" in d.get("factor_regime", "") or
             "BALANCED" in d.get("factor_regime", "")),
        "leading_factor_set": d.get("leading_factor") in
            ("value", "quality", "momentum", "low_vol"),
        "all_4_factor_medians_present":
            all(fm.get(f) is not None for f in
                ("value", "quality", "momentum", "low_vol")),
        "n_valid_min_25": (d.get("n_valid") or 0) >= 25,
        "top_25_populated": len(top25) >= 10,
        "top_3_have_composite":
            all(x.get("composite") is not None for x in top25[:3]),
        "invoke_ok": iv["ok"] and not iv.get("function_error"),
    }
    sc["all_pass"] = all(sc.values())
    ret["scorecard"] = sc
    return ret


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    try:
        report["predictability"] = verify_predictability()
    except Exception:
        report["predictability"] = {"error": traceback.format_exc()[:1500]}
    try:
        report["smart_beta"] = verify_smart_beta()
    except Exception:
        report["smart_beta"] = {"error": traceback.format_exc()[:1500]}

    pred_pass = report.get("predictability", {}).get("scorecard", {}).get(
        "all_pass")
    sb_pass = report.get("smart_beta", {}).get("scorecard", {}).get("all_pass")
    report["scorecard_summary"] = {
        "predictability_all_pass": pred_pass,
        "smart_beta_all_pass": sb_pass,
        "both_pass": bool(pred_pass and sb_pass),
    }
    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1005.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1005] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report["scorecard_summary"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
