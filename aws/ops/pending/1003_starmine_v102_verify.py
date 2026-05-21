"""
ops 1003 - StarMine v1.0.2 verify after FMP-rate-limit refactor.

Architectural change:
- Old: SP500 const fetch (1 call) + quote batch (10 calls) + 150x3 endpoints = 1010 FMP calls
- New: STATIC_TOP50 + single batched quote (1 call) + 50x3 endpoints = 151 FMP calls
       (6.7x reduction)

#1 GF Value v1.0.1 + #5 Bond Vol v1.0.1 already certified all_pass in ops 1002.
Only re-verify #4 StarMine here.
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
    t0 = time.time()
    while time.time() - t0 < max_wait_sec:
        try:
            c = lam.get_function(FunctionName=fn_name)["Configuration"]
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                return {"ok": True, "last_modified": c.get("LastModified"),
                        "waited_sec": round(time.time() - t0, 1)}
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(15)
    return {"ok": False, "error": "timeout",
            "waited_sec": round(time.time() - t0, 1)}


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    wait = wait_for_lambda_active(STARMINE_FN)
    report["lambda_ready"] = wait
    if not wait.get("ok"):
        report["scorecard"] = {"all_pass": False, "deploy_failed": True}
    else:
        iv = invoke(STARMINE_FN, {})
        report["invoke"] = {"ok": iv["ok"],
                            "function_error": iv.get("function_error"),
                            "elapsed_sec": iv.get("elapsed_sec")}
        body = iv.get("payload", {}).get("body") if iv.get("ok") else None
        if isinstance(body, dict):
            report["invoke_summary"] = body

        s = fetch_s3("justhodl-dashboard-live", "data/starmine.json")
        if s["ok"]:
            d = s["data"]
            report["s3"] = {
                "version": d.get("version"),
                "generated_at": d.get("generated_at"),
                "universe_regime": d.get("universe_regime"),
                "universe_source_tier": d.get("universe_source_tier"),
                "universe_median_composite_z": d.get(
                    "universe_median_composite_z"),
                "n_universe_analyzed": d.get("n_universe_analyzed"),
                "n_scored": d.get("n_scored"),
                "median_rrm_raw": d.get("median_rrm_raw"),
                "median_ptd_pct": d.get("median_ptd_pct"),
                "median_esp_hit_pct": d.get("median_esp_hit_pct"),
                "sector_breakdown_top_25": d.get("sector_breakdown_top_25"),
                "top_5_score": [
                    {"t": t.get("ticker"), "score": t.get("starmine_score"),
                     "sec": t.get("sector"),
                     "z_rrm": t.get("z_rrm"), "z_ptd": t.get("z_ptd"),
                     "z_esp": t.get("z_esp"),
                     "n_up_90d": t.get("n_upgrades_90d"),
                     "n_down_90d": t.get("n_downgrades_90d"),
                     "ptd_pct": t.get("ptd_pct"),
                     "esp_hit_pct": t.get("esp_hit_pct")}
                    for t in (d.get("top_25_conviction") or [])[:5]
                ],
                "bottom_5_score": [
                    {"t": t.get("ticker"),
                     "score": t.get("starmine_score"),
                     "sec": t.get("sector"),
                     "n_up_90d": t.get("n_upgrades_90d"),
                     "n_down_90d": t.get("n_downgrades_90d")}
                    for t in (d.get("bottom_25_conviction") or [])[:5]
                ],
            }
            sc = {
                "version_1_0_2": d.get("version") == "1.0.2",
                "universe_regime_real": d.get("universe_regime") in
                    ("BULLISH_REVISIONS", "NEUTRAL_REVISIONS",
                     "BEARISH_REVISIONS"),
                "n_universe_min_30": (d.get("n_universe_analyzed") or 0) >= 30,
                "n_scored_min_20": (d.get("n_scored") or 0) >= 20,
                "top_25_present": len(d.get("top_25_conviction") or []) >= 10,
                "bottom_25_present": len(d.get("bottom_25_conviction") or [])
                                    >= 10,
                "median_z_numeric": isinstance(
                    d.get("universe_median_composite_z"), (int, float)),
                "universe_source_static_top50": (
                    d.get("universe_source_tier") in (
                        "static_top50_live_prices",
                        "static_top50_no_prices")),
                "invoke_ok": iv["ok"] and not iv.get("function_error"),
            }
            sc["all_pass"] = all(sc.values())
            report["scorecard"] = sc
        else:
            report["s3"] = s
            report["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1003.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1003] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps({"starmine_all_pass": report.get("scorecard", {})
                                                .get("all_pass")}, indent=2))


if __name__ == "__main__":
    try: main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
