"""ops 3402 — Proven-Edge Portfolio composer: E2E birth.

The audit's keystone gap. New engine justhodl-proven-portfolio (graded
signal_types -> sized paper book -> daily NAV vs SPY, attribution by family,
self-upgrading PROVEN/PROVISIONAL/WAITING gate) + proven-portfolio.html +
Scheduler daily 21:45 UTC (after the 21:29 checker).

Gates:
  G1  engine deployed (v1.0.0 + qualifying_types markers in zip)
  G2  daily Scheduler ensured (create if absent, codified in config already)
  G3  invoke -> fresh feed: mode set, book/attribution/nav present, ledger
      row for today written (book MAY be legitimately empty pre-day_7 —
      structure is the gate, count is the report)
  G4  page live with all markers
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=340, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
SCH = boto3.client("scheduler", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3402)"}
FN = "justhodl-proven-portfolio"


def invoke_resilient(fn, itype="RequestResponse", tries=6):
    for k in range(tries):
        try:
            return LAM.invoke(FunctionName=fn, InvocationType=itype, Payload=b"{}")
        except Exception as e:  # noqa: BLE001
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e):
                time.sleep(15 * (k + 1))
                continue
            raise
    raise RuntimeError("throttled")


def zsrc(fn):
    info = LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")


with report("3402_proven_portfolio") as rep:
    rep.heading("ops 3402 — the composer, born")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:340]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:290]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(n)

    ok1 = False
    dl = time.time() + 360
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
                src = zsrc(FN)
                if 'VERSION = "1.0.0"' in src and "qualifying_types" in src:
                    ok1 = True
                    break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G1_engine_deployed", ok1, "markers in zip")

    created = None
    try:
        arn = LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
        try:
            SCH.get_schedule(Name="justhodl-proven-portfolio-daily")
            created = "exists"
        except Exception:  # noqa: BLE001
            SCH.create_schedule(
                Name="justhodl-proven-portfolio-daily",
                ScheduleExpression="cron(45 21 ? * * *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn,
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"},
                Description="Proven-Edge Portfolio daily compose (ops 3402)")
            created = "created cron(45 21 ? * * *)"
    except Exception as e:  # noqa: BLE001
        created = f"FAILED {str(e)[:120]}"
    gate("G2_schedule", created in ("exists",) or "created" in str(created), created)

    resp = invoke_resilient(FN)
    print(f"[invoke] status={resp.get('StatusCode')} err={resp.get('FunctionError')}")
    feed, led = None, None
    dl = time.time() + 120
    while time.time() < dl:
        try:
            feed = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                             Key="data/proven-portfolio.json")["Body"].read())
            led = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                            Key="data/proven-portfolio-history.json")["Body"].read())
            break
        except Exception:  # noqa: BLE001
            time.sleep(10)
    today = datetime.now(timezone.utc).date().isoformat()
    ok3 = (bool(feed) and feed.get("mode") in ("PROVEN", "PROVISIONAL", "WAITING")
           and "book" in feed and "attribution" in feed and "nav" in feed
           and bool(led) and (led.get("rows") or []) and led["rows"][-1].get("date") == today)
    gate("G3_first_compose", ok3,
         f"mode={feed and feed.get('mode')} types={len((feed or {}).get('qualifying_types') or {})} "
         f"book={len((feed or {}).get('book') or [])} nav={((feed or {}).get('nav') or {}).get('nav')} "
         f"ledger_today={bool(led and led['rows'][-1].get('date') == today)}")
    out["snapshot"] = {"mode": (feed or {}).get("mode"),
                       "qualifying_types": (feed or {}).get("qualifying_types"),
                       "n_book": len((feed or {}).get("book") or []),
                       "top": ((feed or {}).get("book") or [])[:5]}

    need = ["Proven-Edge Portfolio", "nav-chart", "proven-portfolio.json", "The gate", "Attribution"]
    ok4, missing = False, need
    dl = time.time() + 240
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/proven-portfolio.html?t={int(time.time())}",
                    headers=UA), timeout=25) as r:
                b = r.read().decode("utf-8", "replace")
            missing = [m for m in need if m not in b]
            if not missing:
                ok4 = True
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G4_page_live", ok4, f"missing={missing}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3402.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
