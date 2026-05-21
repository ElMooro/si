"""
ops 1014 - Verify Polygon short-interest staleness fix.

After commit 59fa53e0 added order=desc&sort=settlement_date to the Polygon
/stocks/v1/short-interest URL, the engine should now return the LATEST
snapshot per ticker instead of the 2017-12-29 fossils ops 1012 found.

Validates:
- Lambda deployed cleanly
- Invoke succeeds (WATCHLIST ~157 tickers in parallel batches)
- by_ticker[AAPL].settlement_date is in 2024 or later (not 2017)
- At least 80% of by_ticker entries have settlement_date in 2024+
- short-interest.json output is fresh on S3
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
FN = "justhodl-short-interest"
KEY = "data/short-interest.json"

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
        except Exception:
            pass
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
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

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

    s = fetch_s3()
    if not s["ok"]:
        report["s3"] = s
        report["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        _write(report)
        return

    d = s["data"]
    by_t = d.get("by_ticker") or {}

    # Settlement-date freshness analysis
    settlement_dates = []
    aapl_row = by_t.get("AAPL") or {}
    msft_row = by_t.get("MSFT") or {}
    nvda_row = by_t.get("NVDA") or {}
    by_year = {}
    null_dates = 0
    for sym, row in by_t.items():
        if not isinstance(row, dict):
            continue
        sd = row.get("settlement_date")
        if sd:
            yr = sd[:4]
            by_year[yr] = by_year.get(yr, 0) + 1
            settlement_dates.append(sd)
        else:
            null_dates += 1

    n_total = len(by_t)
    n_2024_plus = sum(v for k, v in by_year.items() if k >= "2024")
    n_2017 = by_year.get("2017", 0)

    report["s3"] = {
        "n_tickers_in_by_ticker": n_total,
        "n_with_settlement_date": len(settlement_dates),
        "n_null_settlement_date": null_dates,
        "settlement_dates_by_year": by_year,
        "n_2024_or_later": n_2024_plus,
        "n_stuck_at_2017": n_2017,
        "max_settlement_date": (max(settlement_dates)
                                 if settlement_dates else None),
        "min_settlement_date": (min(settlement_dates)
                                 if settlement_dates else None),
        "AAPL": {
            "settlement_date": aapl_row.get("settlement_date"),
            "short_interest": aapl_row.get("short_interest"),
            "latest_short_pct": aapl_row.get("latest_short_pct"),
            "days_to_cover": aapl_row.get("days_to_cover"),
            "signal": aapl_row.get("signal"),
        },
        "MSFT": {
            "settlement_date": msft_row.get("settlement_date"),
            "short_interest": msft_row.get("short_interest"),
        },
        "NVDA": {
            "settlement_date": nvda_row.get("settlement_date"),
            "short_interest": nvda_row.get("short_interest"),
        },
        "n_tickers_finra": d.get("n_tickers_finra"),
        "n_tickers_polygon": d.get("n_tickers_polygon"),
        "generated_at": d.get("generated_at"),
    }

    sc = {
        "lambda_active": w.get("ok"),
        "invoke_ok": iv["ok"] and not iv.get("function_error"),
        "n_tickers_min_50": n_total >= 50,
        "n_2024_or_later_min_80pct": (n_2024_plus / n_total >= 0.80
                                       if n_total else False),
        "AAPL_settlement_in_2024_plus":
            (aapl_row.get("settlement_date") or "")[:4] >= "2024",
        "AAPL_settlement_not_2017":
            (aapl_row.get("settlement_date") or "") != "2017-12-29",
        "no_majority_2017_stalled": n_2017 < (n_total / 2 if n_total else 1),
    }
    sc["all_pass"] = all(sc.values())
    report["scorecard"] = sc

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1014.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1014] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report.get("scorecard", {}), indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
