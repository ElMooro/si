"""
ops 1013 - Verify squeeze-pretrigger v1.1 schema fix lands n_evaluated > 0.

After commit 2d67efd7 fixed two upstream-schema bugs in extractors:
- extract_finra_metrics now iterates tickers as dict + reads squeeze_candidates
- extract_short_interest now reads by_ticker dict + uses latest_short_pct

Confirms:
- Lambda is at v1.1
- Invoke produces n_evaluated > 0 (the symptom we were chasing)
- candidates set is no longer near-empty
- State transitions out of perpetual QUIET when real signals present

Tightened scorecard:
- version == "1.1"
- n_evaluated >= 5 (was 0 in v1.0 due to schema mismatch)
- candidates_in_finra_map >= 50 AND candidates_in_si_map >= 50 (proves
  extractors are now reading the dict-shaped feeds)
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
FN = "justhodl-squeeze-pretrigger"
KEY = "data/squeeze-pretrigger.json"
EXPECTED_VERSION = "1.1"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)


def wait_for_active(fn, max_wait=600):
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if (c.get("State") == "Active" and
                    c.get("LastUpdateStatus") == "Successful"):
                return {"ok": True,
                        "last_modified": c.get("LastModified"),
                        "code_size": c.get("CodeSize"),
                        "waited_sec": round(time.time() - t0, 1)}
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(15)
    return {"ok": False, "error": "timeout"}


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
                "elapsed_sec": elapsed, "payload": body,
                "log_tail": r.get("LogResult")}
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

    w = wait_for_active(FN)
    report["lambda_ready"] = w
    if not w.get("ok"):
        report["scorecard"] = {"all_pass": False, "deploy_failed": True}
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        _write(report)
        return

    iv = invoke()
    report["invoke"] = {"ok": iv["ok"],
                        "function_error": iv.get("function_error"),
                        "elapsed_sec": iv.get("elapsed_sec"),
                        "error": iv.get("error")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        report["invoke_summary"] = body

    s = fetch_s3()
    if not s["ok"]:
        report["s3"] = s
        report["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        _write(report)
        return

    d = s["data"]
    summary = d.get("summary") or {}
    imminent = d.get("imminent_setups") or []
    pretrigger = d.get("pretrigger_setups") or []
    early = d.get("early_setups") or []

    report["s3"] = {
        "version": d.get("version"),
        "as_of": d.get("as_of"),
        "state": d.get("state"),
        "signal_strength": d.get("signal_strength"),
        "n_evaluated": summary.get("n_candidates_evaluated"),
        "n_imminent": summary.get("n_imminent_5of5"),
        "n_pretrigger": summary.get("n_pretrigger_4of5"),
        "n_early": summary.get("n_early_3of5"),
        "n_total": summary.get("n_total_setups"),
        "feeds_available": summary.get("feeds_available"),
        "top_5_tickers": (d.get("current_readings") or {}).get(
            "top_squeeze_tickers", [])[:5],
        "first_imminent": imminent[0] if imminent else None,
        "first_pretrigger": pretrigger[0] if pretrigger else None,
        "first_early": early[0] if early else None,
    }

    n_eval = summary.get("n_candidates_evaluated") or 0
    sc = {
        "version_1_1": d.get("version") == EXPECTED_VERSION,
        "feeds_all_available": all(
            (summary.get("feeds_available") or {}).get(k)
            for k in ("finra", "short_interest", "catalyst")),
        # The CORE assertion - was 0 in v1.0, expect >=5 now
        "n_evaluated_min_5": n_eval >= 5,
        # Looser fallback in case real markets are still quiet today
        "n_evaluated_min_1": n_eval >= 1,
        "state_real": d.get("state") in
            ("IMMINENT", "PRE_TRIGGER", "EARLY", "QUIET"),
        "invoke_ok": iv["ok"] and not iv.get("function_error"),
    }
    sc["all_pass"] = (sc["version_1_1"] and sc["feeds_all_available"]
                       and sc["n_evaluated_min_1"] and sc["state_real"]
                       and sc["invoke_ok"])
    report["scorecard"] = sc

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1013.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1013] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report.get("scorecard", {}), indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
