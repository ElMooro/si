#!/usr/bin/env python3
"""ops 3005 -- (1) flows join v3 verify (etf-flows/daily.json metrics
map -- the writer's actual output, no per-ticker files exist);
(2) deep-dive the 2 genuine feed suspects from 3004's NO_WRITE set
with SYNC invokes to capture real errors (Event invokes swallow
them): justhodl-ici-flows (expected: ICI anti-scraped .xls, known),
justhodl-engine-robustness (unknown). The other 7 no-writes are
on-demand/API-class (history-api, trade-journal, transcript-indexer,
transcript-query, watchlist, feedback, kill-switch) -- reclassified
honestly, no fix needed, recorded so future triages skip them.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=460, connect_timeout=10,
                                 retries={"max_attempts": 0}))
LAM_SHORT = boto3.client("lambda", region_name="us-east-1",
                         config=Config(read_timeout=185,
                                       connect_timeout=10,
                                       retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
IR = "justhodl-industry-rotation"
SUSPECTS = ["justhodl-engine-robustness", "justhodl-ici-flows"]
ONDEMAND_RECLASS = ["justhodl-history-api", "justhodl-trade-journal",
                    "justhodl-transcript-indexer",
                    "justhodl-transcript-query", "justhodl-watchlist",
                    "justhodl-feedback", "justhodl-kill-switch"]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    out = {"ops": 3005,
           "ts": datetime.now(timezone.utc).isoformat(),
           "ondemand_reclassified": ONDEMAND_RECLASS}
    with report("3005_flows_v3_deepdive") as rep:

        rep.section("1. IR deploy gate + flows v3")
        time.sleep(75)
        ok = False
        for _ in range(50):
            cfg = LAM.get_function_configuration(FunctionName=IR)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            if cfg.get("LastUpdateStatus") == "Successful" and \
                    (datetime.now(timezone.utc) - lm
                     ).total_seconds() < 1800:
                ok = True
                break
            time.sleep(8)
        if not ok:
            fails.append("IR no fresh deploy")
        else:
            resp = LAM.invoke(FunctionName=IR, Payload=b"{}")
            body = json.loads(resp["Payload"].read() or b"{}")
            if resp.get("FunctionError"):
                fails.append("IR invoke: %s" % json.dumps(body)[:250])
            else:
                d = s3_json("data/industry-rotation.json")
                rows = d.get("ladder") or []
                fj = sum(1 for r in rows if r.get("fund_flows"))
                out["flows_joined"] = fj
                out["flows_sample"] = [
                    {"etf": r["etf"], **r["fund_flows"]}
                    for r in rows if r.get("fund_flows")][:6]
                jw = next((w for w in (d.get("warns") or [])
                           if "fund_flows" in w or "flows join" in w),
                          None)
                out["join_warn"] = jw
                rep.kv(flows_joined=fj, warn=jw,
                       sample=json.dumps(out["flows_sample"])[:280])
                if fj < 10:
                    fails.append("flows still %d/40 (warn: %s)"
                                 % (fj, jw))

        rep.section("2. Suspect sync invokes (error capture)")
        diag = {}
        for fn in SUSPECTS:
            row = {}
            try:
                t0 = time.time()
                r = LAM_SHORT.invoke(FunctionName=fn, Payload=b"{}")
                row["secs"] = round(time.time() - t0, 1)
                b = json.loads(r["Payload"].read() or b"{}")
                row["fn_error"] = r.get("FunctionError")
                row["body"] = json.dumps(b)[:400]
            except Exception as e:
                row["client_err"] = str(e)[:200]
            diag[fn] = row
            rep.kv(**{fn.replace("justhodl-", ""):
                      json.dumps(row)[:300]})
        out["suspect_diag"] = diag

        if not fails:
            rep.ok("flows %s/40 | suspects: %s"
                   % (out.get("flows_joined"),
                      json.dumps({k: (v.get("fn_error")
                                      or ("ok" if "body" in v
                                          else v.get("client_err",
                                                     "?")[:60]))
                                  for k, v in diag.items()})))
        _w(rep, out, fails, warns)


def _w(rep, out, fails, warns):
    out["fails"], out["warns"] = fails, warns
    out["verdict"] = "PASS" if not fails else "FAIL"
    (AWS_DIR / "ops" / "reports" / "3005.json").write_text(
        json.dumps(out, indent=1, default=str))
    rep.log("FAILS=%d" % len(fails))
    if fails:
        sys.exit(1)


try:
    main()
except SystemExit:
    raise
except Exception as e:
    import traceback
    (AWS_DIR / "ops" / "reports" / "3005.json").write_text(json.dumps(
        {"ops": 3005, "verdict": "FAIL",
         "fails": ["CRASH: %s" % str(e)[:200]],
         "trace": traceback.format_exc()[-1500:],
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    sys.exit(1)
sys.exit(0)
