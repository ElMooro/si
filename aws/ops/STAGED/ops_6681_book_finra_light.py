"""ops 6681 - first light of the factor book v1.0.6 (FINRA's own short interest joined beside Polygon's) after the fleet lane
FLEET2 (run 40360350275, 292 bank-module importers + bank-factors; green 18:48 UTC).

One asynchronous invocation of justhodl-bank-factors (its 02:50 UTC daily join already ran today on v1.0.5; this is the
verification run), then the REPORT line, the manifest (version, coverage.finra_short, sources.finra_short, as_of) and three
documents' short fields (AAPL, GME, a FINRA-only name found from the manifest's sources) - so the join is shown from the
published objects, not assumed.  Also reads the deployed versions of the four fleet-health engines and the five contract
engines after the fleet lane.  Writes nothing except what the engine writes.  No key involved.
"""
from pathlib import Path
import gzip
import json
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
BOOK = "data/warm/factor-book/"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=60)
WAIT_S = int(os.environ.get("WAIT_S") or "780")
FLEET_CHECK = ("justhodl-analytics-snapshot", "justhodl-earnings-whisper", "justhodl-event-flow-monitor", "justhodl-signal-genealogy",
               "justhodl-eps-revision-velocity", "justhodl-momentum-leaders", "justhodl-earnings-pead", "justhodl-estimate-revisions",
               "justhodl-pump-positioning", "justhodl-bank-factors", "justhodl-equity-research", "justhodl-magic-formula")


def jget(s3, key):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        try:
            raw = gzip.decompress(raw)
        except Exception:  # noqa: BLE001
            pass
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {"_error": type(e).__name__}


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6681_book_finra_light") as r:
        r.heading("ops 6681 - factor book v1.0.6 first light (FINRA short interest) + deployed versions after the fleet lane")
        r.section("0. deployed configuration after lane FLEET2")
        for fn in FLEET_CHECK:
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: %s MB / %s s, modified %s" % (fn, c.get("MemorySize"), c.get("Timeout"), (c.get("LastModified") or "")[:19]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(e).__name__))
        man0 = jget(s3, BOOK + "manifest.json")
        r.log("book manifest before: version %s generated %s coverage.finra_short %s" % (man0.get("version"), man0.get("generated_at"), (man0.get("coverage") or {}).get("finra_short")))
        r.section("1. one async join")
        since_ms = int(time.time() * 1000) - 5000
        resp = lam.invoke(FunctionName="justhodl-bank-factors", InvocationType="Event", Payload=json.dumps({"force": True}).encode())
        rid = resp.get("ResponseMetadata", {}).get("RequestId")
        r.log("accepted %s (request %s)" % (resp.get("StatusCode"), rid))
        line, t0 = None, time.time()
        while time.time() - t0 < WAIT_S and not line:
            try:
                ev = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-bank-factors", startTime=since_ms, filterPattern='"REPORT RequestId: %s"' % rid, limit=3).get("events", [])
            except Exception:  # noqa: BLE001
                ev = []
            if ev:
                m = ev[-1].get("message", "")
                line = " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:", "Init Duration")))
            else:
                time.sleep(20)
        r.log("REPORT: %s" % (line or "(not within %d s)" % WAIT_S))
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-bank-factors", startTime=since_ms, limit=30,
                                        filterPattern='?"finra" ?"[book]" ?Traceback ?"[ERROR]" ?"Task timed out" ?"start;" ?"wrote"').get("events", [])
            for e in ev[-14:]:
                r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:300]))
        except Exception as e:  # noqa: BLE001
            r.warn("logs: %s" % type(e).__name__)
        r.section("2. the published book")
        man = jget(s3, BOOK + "manifest.json")
        r.log("manifest: version %s generated %s n %s | coverage.finra_short %s (short %s) | sources.finra_short %s | as_of.finra-short-interest %s" % (
            man.get("version"), man.get("generated_at"), man.get("n"), (man.get("coverage") or {}).get("finra_short"), (man.get("coverage") or {}).get("short"),
            json.dumps((man.get("sources") or {}).get("finra_short")), (man.get("as_of") or {}).get("finra-short-interest")))
        fields = man.get("fields") or []
        r.log("matrix fields: %d; finra columns present: %s" % (len(fields), [f for f in fields if f.startswith("finra_")]))
        for sym in ("AAPL", "GME", "SOFI", "PLTR", "TSLA"):
            doc = jget(s3, BOOK + "symbols/%s.json.gz" % sym)
            d = doc.get("derived") or {}
            r.log("%s: short_source %s short_pct_float %s si_settlement %s | polygon SI %s | finra SI %s dtc %s change%% %s vs-polygon%% %s settlement %s" % (
                sym, d.get("short_source"), d.get("short_pct_float"), d.get("si_settlement_date"), (doc.get("short") or {}).get("short_interest"),
                d.get("finra_short_interest"), d.get("finra_days_to_cover"), d.get("finra_si_change_pct"), d.get("finra_vs_polygon_si_pct"), d.get("finra_settlement_date")))
        r.ok("ops 6681 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
