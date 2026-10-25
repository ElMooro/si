"""ops 6654 - read-only: is justhodl-polygon-full's grouped-daily bank made of COMPLETE sessions?

The Polygon aggregates lane (docs/providers/WAREHOUSE_BANKS.md, "Next lane") will transpose polygon-full's per-session
grouped files into per-ticker histories and answer /v2/aggs/ticker/{T}/range/1/day from them.  That is only honest if
every banked session is the closed day's final aggregate.  polygon-full runs on rate(24 hours) and takes "last closed
day" = now - 8 h, so a run inside the US session could bank a partial day.  This op reads: the engine's run times from
CloudWatch REPORT lines (last 14 days), its state (cursor, sessions, last_n), the manifest, and for the last 12 banked
sessions the file's resultsCount, the hour it was written (LastModified) and SPY's bar (t, o, h, l, c, v) - a session
written before ~20:15 UTC on its own date is a partial-day suspect; SPY's volume on such a day would be far below its
neighbours.  Nothing is written.  Keys never printed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-polygon-full"
PFX = "data/warm/polygon-full/"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def jget(s3, key):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)
    except Exception:  # noqa: BLE001
        return None


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    sch = boto3.client("scheduler", region_name=REGION, config=CFG)
    with report("ops_6654_polygon_full_session_probe") as r:
        r.heading("ops 6654 - polygon-full grouped sessions: complete days or partial? (read-only)")
        st = jget(s3, PFX + "_state/state.json") or {}
        man = jget(s3, PFX + "manifest.json") or {}
        r.log("state: phase %s cursor %s sessions %s last_n %s window_start %s lease_until %s" % (st.get("phase"), st.get("cursor"), st.get("sessions"), st.get("last_n"), st.get("window_start"), st.get("lease_until")))
        r.log("manifest keys: %s" % sorted(man.keys())[:20])
        try:
            sc = sch.get_schedule(Name="justhodl-polygon-full-2h", GroupName="default")
            r.log("schedule justhodl-polygon-full-2h: %s state %s tz %s created %s" % (sc.get("ScheduleExpression"), sc.get("State"), sc.get("ScheduleExpressionTimezone"), str(sc.get("CreationDate"))[:19]))
        except Exception as e:  # noqa: BLE001
            r.warn("schedule read: %s" % type(e).__name__)
        r.section("run times (CloudWatch REPORT lines, last 14 days)")
        since = int((datetime.now(timezone.utc) - timedelta(days=14)).timestamp() * 1000)
        times = []
        try:
            kwargs = {"logGroupName": "/aws/lambda/" + FN, "startTime": since, "filterPattern": "REPORT", "limit": 10000}
            for _ in range(10):
                resp = logs.filter_log_events(**kwargs)
                times += [ev["timestamp"] for ev in resp.get("events", [])]
                if resp.get("nextToken"):
                    kwargs["nextToken"] = resp["nextToken"]
                else:
                    break
        except Exception as e:  # noqa: BLE001
            r.warn("logs: %s" % type(e).__name__)
        hours = sorted(datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%m-%d %H:%M") for t in times)
        r.log("%d runs: %s" % (len(hours), ", ".join(hours[-20:])))
        r.section("last 12 banked sessions: written when, how many tickers, SPY's bar")
        keys = []
        for y in (datetime.now(timezone.utc).year, datetime.now(timezone.utc).year - 1):
            tok = None
            while True:
                kw = {"Bucket": LIVE, "Prefix": PFX + "grouped/%d/" % y, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                keys += [(o["Key"], o["LastModified"], o["Size"]) for o in resp.get("Contents", [])]
                tok = resp.get("NextContinuationToken")
                if not tok:
                    break
        keys.sort()
        r.log("sessions banked in the last two years: %d (first %s, last %s)" % (len(keys), keys[0][0][-18:-8] if keys else None, keys[-1][0][-18:-8] if keys else None))
        for key, lm, size in keys[-12:]:
            session = key[-18:-8]
            doc = jget(s3, key) or {}
            spy = next((row for row in doc.get("results") or [] if row.get("T") == "SPY"), None)
            written = lm.astimezone(timezone.utc)
            same_day_early = written.date().isoformat() == session and (written.hour * 60 + written.minute) < 20 * 60 + 15
            r.log("%s: written %s UTC%s, %s bytes, resultsCount %s, SPY %s" % (session, written.strftime("%m-%d %H:%M"), "  <- SAME DAY BEFORE 20:15 UTC: partial-day suspect" if same_day_early else "", size, doc.get("resultsCount"),
                                                                            json.dumps({k: spy.get(k) for k in ("o", "h", "l", "c", "v", "t")}) if spy else None))
        r.ok("ops 6654 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
