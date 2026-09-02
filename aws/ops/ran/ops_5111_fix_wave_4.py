"""ops_5111 -- fix wave 4 + the before/after re-measure.

  1  deploy-wait + run + verify: import-sentinel (NameError now), fleet-monitor /
     signal-harvester / feed-registry (Delimiter listing: the undelimited walk over
     data/ was 9.7M objects), repo-monitor (CP-OIS proxy, fixed-rate repo endpoint),
     manufacturing-global-agent (live regional-Fed ids only), portwatch v1.6.3
     (one-time backfill -> ports with yoy)
  2  imf-full: the 6h drain schedule times out every run (850s) on a warehouse
     that is 218/222 complete; keep the weekly schedule, disable the 6h one
  3  ici-flows: ici.org answers 403 to the fetcher and nothing consumes the feed
     -> rule disabled, reason recorded (rebuild on OFR's MMF monitor if wanted)
  4  census-us: state docs freshness after the 204 fix
  5  re-measure: Invocations/Errors over the last 2 hours for every engine on the
     5098 FIX_ERRORS list and the 63 restored fan-out members, vs the 7d baseline
Gate: import-sentinel feed present; fleet-monitor/feed-registry/signal-harvester
finish under their timeouts; repo-monitor without HTTP_ERR; portwatch ports
with yoy > 0.
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=200, retries={"max_attempts": 2}))
ev = boto3.client("events", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
NOW = datetime.now(timezone.utc)
PUSH = NOW.strftime("%Y-%m-%dT%H:%M")


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:100]


def wait_deploy(r, fn, marker=None, secs=900):
    t0 = time.time()
    while time.time() - t0 < secs:
        c = lam.get_function_configuration(FunctionName=fn)
        lm = c.get("LastModified") or ""
        if c.get("LastUpdateStatus") == "Successful" and ((marker and marker in (c.get("Description") or "")) or (not marker and lm[:16] >= PUSH)):
            r.ok(f"{fn} deployed ({lm}) after {time.time() - t0:.0f}s")
            return True
        time.sleep(20)
    r.warn(f"{fn}: deploy not observed within {secs}s")
    return False


def log_lines(fn, since, pattern=None, limit=40):
    try:
        kw = {"logGroupName": f"/aws/lambda/{fn}", "startTime": int(since.timestamp() * 1000), "limit": limit}
        if pattern:
            kw["filterPattern"] = pattern
        return [e["message"].rstrip()[:240] for e in (logs.filter_log_events(**kw).get("events") or [])]
    except Exception as e:  # noqa: BLE001
        return [f"log read failed: {str(e)[:120]}"]


def run_and_check(r, fn, wait_s, err_pattern, ok_pattern="REPORT", payload=b"{}"):
    t0 = datetime.now(timezone.utc)
    lam.invoke(FunctionName=fn, InvocationType="Event", Payload=payload)
    time.sleep(wait_s)
    errs = log_lines(fn, t0, pattern=err_pattern, limit=8)
    rep = log_lines(fn, t0, pattern=ok_pattern, limit=3)
    dur = None
    for ln in rep:
        if "Duration:" in ln:
            try:
                dur = float(ln.split("Duration:")[1].split("ms")[0].strip())
            except Exception:  # noqa: BLE001
                pass
    timed_out = any("Status: timeout" in ln for ln in rep)
    r.log(f"{fn}: errors={len(errs)} {json.dumps(errs)[:300]} duration_ms={dur} timeout={timed_out}")
    r.kv(engine=fn, errors=len(errs), duration_ms=dur, timed_out=timed_out, reported=bool(rep))
    return errs, rep, timed_out


def main():
    with report("5111-fix-wave-4") as r:
        r.heading("ops 5111 -- fix wave 4 + before/after re-measure")
        fails = []

        r.section("1. deploy-wait + run + verify")
        if wait_deploy(r, "justhodl-import-sentinel"):
            errs, rep, to = run_and_check(r, "justhodl-import-sentinel", 120, "?NameError ?Traceback")
            doc, lm = get_json("data/import-sentinel.json")
            r.log(f"import-sentinel feed: {'present' if doc else 'MISSING'} generated_at={(doc or {}).get('generated_at') or (doc or {}).get('as_of')} chips={len((doc or {}).get('pipeline') or (doc or {}).get('chips') or [])}")
            if errs or not doc:
                fails.append("import-sentinel still failing / feed missing")
        for fn, wait_s in (("justhodl-fleet-monitor", 240), ("justhodl-feed-registry", 240), ("justhodl-signal-harvester", 300)):
            if wait_deploy(r, fn):
                errs, rep, to = run_and_check(r, fn, wait_s, '?"[ERROR]" ?Traceback ?"Task timed out"')
                if to or (not rep):
                    fails.append(f"{fn}: {'timed out' if to else 'no REPORT within wait'}")
        if wait_deploy(r, "justhodl-repo-monitor"):
            errs, rep, to = run_and_check(r, "justhodl-repo-monitor", 90, "?HTTP_ERR ?SRF_ERR ?Traceback")
            if errs:
                fails.append("repo-monitor still logs HTTP_ERR/SRF_ERR")
            rm, lm = get_json("data/repo-monitor.json")
            if rm:
                m = rm.get("metrics") or rm.get("M") or {}
                r.log(f"repo-monitor feed: FRA_OIS_Proxy={json.dumps((m or {}).get('FRA_OIS_Proxy'))[:200]} SRF={json.dumps((m or {}).get('SRF_Usage'))[:160]}")
        if wait_deploy(r, "manufacturing-global-agent"):
            errs, rep, to = run_and_check(r, "manufacturing-global-agent", 60, '?"Error fetching" ?Traceback')
            if errs:
                fails.append("manufacturing-global-agent still logs fetch errors")
        if wait_deploy(r, "justhodl-portwatch", marker="v1.6.3"):
            t0 = datetime.now(timezone.utc)
            try:
                resp = lam.invoke(FunctionName="justhodl-portwatch", InvocationType="RequestResponse", Payload=b"{}")
                r.log(f"portwatch invoke {resp.get('StatusCode')} {resp['Payload'].read()[:160]}")
            except Exception as e:  # noqa: BLE001
                r.warn(f"portwatch sync: {str(e)[:100]} -> async")
                lam.invoke(FunctionName="justhodl-portwatch", InvocationType="Event", Payload=b"{}")
                time.sleep(300)
            pw, lm = get_json("data/portwatch.json")
            if pw and (pw.get("generated_at") or "") > t0.isoformat():
                ports = pw.get("ports") or []
                with_yoy = sum(1 for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float)))
                r.log(f"portwatch v{pw.get('version')}: ports={len(ports)} with_yoy={with_yoy} requests={json.dumps(pw.get('requests'))} history_through={json.dumps(pw.get('history_through'))} errors={json.dumps(pw.get('errors'))[:240]}")
                top = [(p.get("name"), p.get("country"), p.get("yoy_pct")) for p in ports if isinstance(p.get("yoy_pct"), (int, float))][:8]
                r.log(f"  sample: {top}")
                r.kv(engine="portwatch", ports=len(ports), with_yoy=with_yoy)
                if with_yoy == 0:
                    fails.append("portwatch: still no ports with yoy")
            else:
                fails.append("portwatch feed not regenerated")

        r.section("2. imf-full 6h schedule off")
        try:
            s = sch.get_schedule(Name="justhodl-imf-full-6h")
            sch.update_schedule(Name="justhodl-imf-full-6h", ScheduleExpression=s["ScheduleExpression"], ScheduleExpressionTimezone=s.get("ScheduleExpressionTimezone", "UTC"),
                                FlexibleTimeWindow=s.get("FlexibleTimeWindow") or {"Mode": "OFF"}, Target=s["Target"], State="DISABLED",
                                Description="DISABLED by ops 5111: every 6h run timed out at 850s on a 218/222-complete warehouse; the weekly schedule keeps discovery + vintages")
            r.ok("justhodl-imf-full-6h -> DISABLED (weekly schedule kept)")
        except Exception as e:  # noqa: BLE001
            r.warn(f"imf-full schedule: {str(e)[:120]}")

        r.section("3. ici-flows rule off")
        try:
            ev.disable_rule(Name="justhodl-ici-flows-weekly")
            r.ok("justhodl-ici-flows-weekly -> DISABLED: ici.org answers 403 to the fetcher (sitemap + /research), no consumer reads data/ici-flows*.json; rebuild on OFR's MMF monitor if the desk wants MMF flows back")
        except Exception as e:  # noqa: BLE001
            r.warn(f"ici rule: {str(e)[:120]}")

        r.section("4. census-us state freshness")
        try:
            objs = s3.list_objects_v2(Bucket=B, Prefix="data/warm/census-us/_state/").get("Contents") or []
            newest = max((o["LastModified"] for o in objs), default=None)
            r.log(f"census-us _state docs: {len(objs)} newest age {round((NOW - newest).total_seconds() / 3600, 1) if newest else None}h")
            r.kv(engine="census-us-state", docs=len(objs), newest_age_h=round((NOW - newest).total_seconds() / 3600, 1) if newest else None)
        except Exception as e:  # noqa: BLE001
            r.warn(f"census state: {str(e)[:100]}")

        r.section("5. re-measure: last 2h vs 7d baseline")
        audit, _ = get_json("data/audit/fleet-data-audit-5098.json")
        fanout, _ = get_json("config/fanout-manifest.json")
        base = {x["fn"]: x for x in (audit or {}).get("engines") or []}
        fix_list = [x["fn"] for x in base.values() if x["cls"] in ("ERRORING", "SOME_ERRORS", "SCHEDULED_SILENT")]
        members = sorted({fn for v in (fanout or {}).get("ticks", {}).values() for fn in v})
        names = sorted(set(fix_list) | set(members))
        start = NOW - timedelta(hours=2)
        got = {}
        for i in range(0, len(names), 250):
            chunk = names[i:i + 250]
            q = []
            for j, n in enumerate(chunk):
                for met, tag in (("Invocations", "i"), ("Errors", "e")):
                    q.append({"Id": f"{tag}{j}", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": met, "Dimensions": [{"Name": "FunctionName", "Value": n}]},
                                                                 "Period": 7200, "Stat": "Sum"}, "ReturnData": True})
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=start, EndTime=NOW + timedelta(minutes=2))
            for m in res.get("MetricDataResults") or []:
                j = int(m["Id"][1:])
                got.setdefault(chunk[j], {})[m["Id"][0]] = sum(m.get("Values") or [0])
        r.log("FIX_ERRORS list -- 7d baseline (inv/err) -> last 2h (inv/err):")
        still = []
        for fn in fix_list:
            b = base[fn]
            g = got.get(fn, {})
            r.log(f"  {fn:<40} {b['inv7d']:.0f}/{b['err7d']:.0f} -> {g.get('i', 0):.0f}/{g.get('e', 0):.0f}")
            r.kv(engine=fn, inv7d=b["inv7d"], err7d=b["err7d"], inv2h=g.get("i", 0), err2h=g.get("e", 0))
            if g.get("e", 0) > 0:
                still.append((fn, g.get("i", 0), g.get("e", 0)))
        r.log(f"still erroring in the last 2h: {still}")
        inv_members = sum(1 for fn in members if got.get(fn, {}).get("i", 0) > 0)
        err_members = [(fn, got[fn].get("e", 0)) for fn in members if got.get(fn, {}).get("e", 0) > 0]
        r.log(f"fan-out members: {inv_members}/{len(members)} invoked in the last 2h; errored: {err_members}")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok(f"VERDICT: GREEN -- wave 4 verified; still erroring (2h): {len(still)} engines")


if __name__ == "__main__":
    main()
