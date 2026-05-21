"""
ops 1004 - Beneish M-Score verify (Pro Pack v3 #6).

Verifies new justhodl-beneish Lambda:
- Deploys successfully
- Invokes successfully (FMP heavy: 50 tickers x 3 endpoints = 150 calls)
- Writes S3 with valid structure
- Scorecard: version, universe_state band, per-ticker M-Score range,
  red flag classification, etc.
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
BENEISH_FN = "justhodl-beneish"

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
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(15)
    return {"ok": False, "error": "timeout",
            "waited_sec": round(time.time() - t0, 1)}


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    wait = wait_for_lambda_active(BENEISH_FN)
    report["lambda_ready"] = wait
    if not wait.get("ok"):
        report["scorecard"] = {"all_pass": False, "deploy_failed": True}
    else:
        iv = invoke(BENEISH_FN, {})
        report["invoke"] = {"ok": iv["ok"],
                            "function_error": iv.get("function_error"),
                            "elapsed_sec": iv.get("elapsed_sec")}
        body = iv.get("payload", {}).get("body") if iv.get("ok") else None
        if isinstance(body, dict):
            report["invoke_summary"] = body

        s = fetch_s3("justhodl-dashboard-live", "data/beneish.json")
        if s["ok"]:
            d = s["data"]
            verdicts = [t.get("verdict") for t in (d.get("all_tickers") or [])]
            n_red = sum(1 for v in verdicts if v == "LIKELY_MANIPULATOR")
            n_watch = sum(1 for v in verdicts if v == "WATCH_LIST")
            n_clean = sum(1 for v in verdicts if v == "UNLIKELY_MANIPULATOR")
            report["s3"] = {
                "version": d.get("version"),
                "generated_at": d.get("generated_at"),
                "universe_state": d.get("universe_state"),
                "n_universe_analyzed": d.get("n_universe_analyzed"),
                "n_likely_manipulator": d.get("n_likely_manipulator"),
                "n_watch_list": d.get("n_watch_list"),
                "n_unlikely_manipulator": d.get("n_unlikely_manipulator"),
                "median_m_score": d.get("median_m_score"),
                "max_m_score": d.get("max_m_score"),
                "min_m_score": d.get("min_m_score"),
                "verdict_counts_recomputed":
                    {"red": n_red, "watch": n_watch, "clean": n_clean},
                "sector_red_flags": d.get("sector_breakdown_red_flags"),
                "red_flags_sample": [
                    {"t": t.get("ticker"), "m": t.get("m_score"),
                     "fy": t.get("fiscal_year_t"),
                     "dsri": t.get("components", {}).get("dsri"),
                     "gmi": t.get("components", {}).get("gmi"),
                     "tata": t.get("components", {}).get("tata")}
                    for t in (d.get("red_flags") or [])[:5]
                ],
                "cleanest_3": [
                    {"t": t.get("ticker"), "m": t.get("m_score"),
                     "fy": t.get("fiscal_year_t")}
                    for t in (d.get("cleanest_10") or [])[:3]
                ],
                "worst_5_in_all": [
                    {"t": t.get("ticker"), "m": t.get("m_score")}
                    for t in (d.get("all_tickers") or [])[:5]
                ],
            }
            sc = {
                "version_1_0_0": d.get("version") == "1.0.0",
                "universe_state_real": d.get("universe_state") in
                    ("LOW_FRAUD_RISK", "MODERATE_FRAUD_RISK",
                     "ELEVATED_FRAUD_RISK"),
                "n_universe_min_30": (d.get("n_universe_analyzed") or 0) >= 30,
                "median_m_in_range": isinstance(d.get("median_m_score"),
                                                 (int, float)) and
                                      -10 < d.get("median_m_score", 0) < 10,
                "all_tickers_have_verdicts": all(
                    t.get("verdict") in ("LIKELY_MANIPULATOR", "WATCH_LIST",
                                          "UNLIKELY_MANIPULATOR")
                    for t in (d.get("all_tickers") or [])),
                "verdict_counts_match": (n_red ==
                                          d.get("n_likely_manipulator") and
                                          n_watch == d.get("n_watch_list") and
                                          n_clean ==
                                          d.get("n_unlikely_manipulator")),
                "invoke_ok": iv["ok"] and not iv.get("function_error"),
            }
            sc["all_pass"] = all(sc.values())
            report["scorecard"] = sc
        else:
            report["s3"] = s
            report["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1004.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1004] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps({"beneish_all_pass": report.get("scorecard", {})
                                                .get("all_pass")}, indent=2))


if __name__ == "__main__":
    try: main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
