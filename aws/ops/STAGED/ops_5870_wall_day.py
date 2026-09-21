"""ops 5870 -- Monday wall day (Claude, 2026-09-21). READ-ONLY: the staging record, the public receipt, the accepted
entries under factory/salon/accepted/<week>-*, the two schedule invocations (CloudWatch), and the desk file."""
import json, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI, PUB, WEEK = "justhodl-ai-857687956942", "justhodl-dashboard-live", "2026-09-21"


def get(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_missing": str(e)[:80]}


def main():
    s3 = boto3.client("s3", region_name="us-east-1"); cw = boto3.client("cloudwatch", region_name="us-east-1"); logs = boto3.client("logs", region_name="us-east-1")
    with report("5870_wall_day") as r:
        r.heading("ops 5870 -- the first Monday wall post")
        st = get(s3, PRI, "factory/salon/student/%s.json" % WEEK)
        r.section("1. Staging (prepare 09:05 ET)")
        r.kv(prepared_at=st.get("prepared_at"), sessions=(st.get("sessions") or [None])[-1:], read_id=st.get("read_id"), missing=st.get("_missing"))
        for sym, e in (st.get("symbols") or {}).items():
            ov = e.get("owned") or {}
            r.log("%s bars=%s desk=%s owned=%s%s%s" % (sym, len(e.get("bars") or []), (e.get("desk") or {}).get("direction"), ov.get("state"), (" pred=" + json.dumps((ov.get("prediction") or {}).get("direction"))) if ov.get("prediction") else "", (" err=" + str(e.get("error"))) if e.get("error") else ""))
        r.section("2. Receipt (post 09:31 ET)")
        rc = st.get("receipt") or get(s3, PUB, "data/ai/wall/student-latest.json")
        r.kv(week=rc.get("week"), rehearsal=rc.get("rehearsal"), entries=len(rc.get("entries") or []), skipped=len(rc.get("skipped") or []), missing=rc.get("_missing"))
        for e in rc.get("entries") or []:
            r.log("  %-14s %-4s %-5s %-10s crisis=%s %s" % (e.get("agent"), e.get("symbol"), e.get("direction"), e.get("regime"), e.get("crisis_probability"), e.get("status")))
        for s_ in rc.get("skipped") or []:
            r.warn("  skipped %s %s: %s" % (s_.get("agent"), s_.get("symbol"), s_.get("reason")))
        r.section("3. Accepted on the wall")
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/salon/accepted/%s-" % WEEK).get("Contents", [])]
        r.kv(accepted_entries=len(keys), agents=sorted({k.rsplit("/", 1)[-1].split("-", 3)[3].rsplit("-", 1)[0] for k in keys}) if keys else [])
        r.section("4. Did the two schedules fire? (justhodl-ai invocations, log lines)")
        now = datetime.now(timezone.utc)
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-ai", logStreamNamePrefix=now.strftime("%Y/%m/%d"), limit=50).get("logStreams", [])
            hits = []
            for stg in streams:
                for ev in logs.filter_log_events(logGroupName="/aws/lambda/justhodl-ai", logStreamNames=[stg["logStreamName"]], filterPattern="wall", limit=40).get("events", []):
                    hits.append((ev["timestamp"], ev["message"].strip()[:160]))
            for ts, msg in sorted(hits)[-12:]:
                r.log("%s %s" % (datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%H:%M:%S"), msg))
            if not hits:
                r.warn("no 'wall' log lines today")
        except Exception as e:  # noqa: BLE001
            r.warn("logs: %s" % str(e)[:120])
        desk = get(s3, PUB, "data/ai-student-desk.json")
        r.section("5. Public desk")
        r.kv(desk_generated_at=desk.get("generated_at"), source=str(desk.get("source"))[:60], wall=json.dumps(desk.get("student_wall")), next_lesson=str(desk.get("next_lesson"))[:160])
        r.ok("done")


if __name__ == "__main__":
    main()
