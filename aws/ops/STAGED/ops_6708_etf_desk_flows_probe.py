"""ops 6708 - READ-ONLY: chart.html's ETF RESEARCH strip for SPY (Khalid, 2026-11-02 10:34 ET: "isn't working") shows
Profile 2026-10-30 and Holdings 2026-10-29 but "USD Unavailable" for the 1 / 5 / 21 reported-observation flow windows.
The strip (jh-chart-etf-desk.js -> JHEtfDeskResearch.windowValue) prints a window only when the fund's flow is
current-eligible (canonical status complete_acquired_history and now < source_valid_until, which is min(acquired +
26 h, latest flow date + 6 days)), the window status is matched_reporting_window and flow_usd_decimal is a decimal.
This reads the desk packet (data/etf-desk-research.json), the canonical flow packet it replays
(data/provider-fund-flow-research.json), and both engines' last 5 days of runs and errors - to name the failing link.
Nothing is written.  No key involved.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=120)
FUNDS = ("SPY", "QQQ", "IWM", "TLT", "GLD")


def get(s3, key):
    obj = s3.get_object(Bucket=LIVE, Key=key)
    return json.loads(obj["Body"].read()), obj["LastModified"]


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    logs = boto3.client("logs", region_name="us-east-1", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6708_etf_desk_flows_probe") as r:
        r.heading("ops 6708 - ETF RESEARCH strip: why SPY's flow windows read 'Unavailable' (read-only)")
        r.section("A. the desk packet data/etf-desk-research.json")
        desk, lm = get(s3, "data/etf-desk-research.json")
        q = desk.get("quality") or {}
        r.log("written %s | generated_at %s | query_date %s | version %s | quality %s" % (lm.isoformat()[:19], desk.get("generated_at"), desk.get("query_date"),
                                                                                      desk.get("version"), json.dumps({k: v for k, v in q.items() if k != "additional_funds"})[:500]))
        r.log("canonical_sources.flows.generated_at %s | holdings %s | reference %s" % (((desk.get("canonical_sources") or {}).get("flows") or {}).get("generated_at"),
              ((desk.get("canonical_sources") or {}).get("holdings") or {}).get("generated_at"), json.dumps(desk.get("reference"))[:300]))
        for t in FUNDS:
            f = (desk.get("funds") or {}).get(t)
            if not f:
                r.log("%s: not in the desk" % t)
                continue
            fl = f.get("flows") or {}
            r.log("%s: basis %s | flow quality %s | flows.status %s acquisition %s acquired %s valid_until %s latest_effective %s | profile %s | holdings %s" % (
                t, f.get("source_basis"), json.dumps(f.get("quality")), (fl.get("quality") or {}).get("status"), fl.get("acquisition_status"),
                fl.get("source_acquired_at"), fl.get("source_valid_until"), fl.get("latest_effective_date"),
                ((f.get("profiles") or {}).get("current") or {}).get("effective_date"), list(((f.get("holdings") or {}).get("current") or {}).get("effective_dates") or {})))
            for n in ("1", "5", "21"):
                w = (fl.get("aligned_windows") or {}).get(n) or {}
                r.log("     window %s: status %s | %s .. %s | flow_usd_decimal %s | observations %s" % (n, w.get("status"), w.get("start_date"), w.get("end_date"),
                                                                                                  w.get("flow_usd_decimal"), w.get("observations") or w.get("reported_observations")))
            if t == "SPY":
                r.log("     SPY flows keys: %s | quality: %s" % (sorted(fl)[:40], json.dumps(fl.get("quality"))[:400]))
        r.section("B. the canonical flow packet data/provider-fund-flow-research.json")
        try:
            can, lm2 = get(s3, "data/provider-fund-flow-research.json")
            funds = can.get("funds") or {}
            r.log("written %s | generated_at %s | source_valid_until %s | reference %s | funds %d by status %s" % (
                lm2.isoformat()[:19], can.get("generated_at"), can.get("source_valid_until"), json.dumps(can.get("reference"))[:200], len(funds),
                dict(Counter((f.get("quality") or {}).get("status") for f in funds.values()))))
            for t in FUNDS:
                f = funds.get(t) or {}
                r.log("%s: status %s | acquisition %s | acquired %s | valid_until %s | latest_effective %s | quality %s" % (
                    t, (f.get("quality") or {}).get("status"), f.get("acquisition_status"), f.get("source_acquired_at"), f.get("source_valid_until"),
                    f.get("latest_effective_date"), json.dumps(f.get("quality"))[:300]))
        except Exception as e:  # noqa: BLE001
            r.warn("canonical packet: %s %s" % (type(e).__name__, str(e)[:200]))
        r.section("C. the two engines: schedules, last runs, errors (5 days)")
        since = int((now - timedelta(days=5)).timestamp() * 1000)
        for fn in ("justhodl-etf-fund-flows", "justhodl-etf-global-desk"):
            cfg = lam.get_function_configuration(FunctionName=fn)
            r.log("%s: %s MB / %s s, LastModified %s, state %s" % (fn, cfg["MemorySize"], cfg["Timeout"], cfg["LastModified"], cfg.get("State")))
            try:
                rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
                r.log("   reserved concurrency %s" % rc)
            except Exception:  # noqa: BLE001
                pass
            for page in sch.get_paginator("list_schedules").paginate(NamePrefix=fn):
                for s in page.get("Schedules", []):
                    d = sch.get_schedule(Name=s["Name"], GroupName=s.get("GroupName", "default"))
                    r.log("   schedule %s [%s] %s" % (s["Name"], d.get("State"), d.get("ScheduleExpression")))
            evs, token = [], None
            while True:
                kw = dict(logGroupName="/aws/lambda/" + fn, startTime=since, filterPattern='?"REPORT RequestId" ?ERROR ?Traceback ?Exception ?"Task timed out"')
                if token:
                    kw["nextToken"] = token
                try:
                    resp = logs.filter_log_events(**kw)
                except Exception as e:  # noqa: BLE001
                    r.warn("   logs: %s" % type(e).__name__)
                    break
                evs += resp.get("events", [])
                token = resp.get("nextToken")
                if not token or len(evs) > 3000:
                    break
            reports = [e for e in evs if "REPORT RequestId" in e["message"]]
            errs = [e for e in evs if "REPORT RequestId" not in e["message"]]
            r.log("   %d runs, %d error lines" % (len(reports), len(errs)))
            for e in reports[-6:]:
                r.log("   %s %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).isoformat()[:16], e["message"].strip().replace("\t", " ")[40:240]))
            for e in errs[-8:]:
                r.log("   ERR %s %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).isoformat()[:16], e["message"].strip().replace("\n", " | ")[:400]))
        r.ok("ops 6708 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
