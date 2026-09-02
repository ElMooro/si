"""ops_5113 -- fix wave 6: post-deploy-only verification of the last three.

  * census-us: the geo_state branch of refresh() had `i += 1` with no `i` in
    scope (a second UnboundLocalError site); repo-monitor: SOFRVOLUME -> SOFRVOL
    and paced FRED calls; portwatch v1.6.5: per-port coverage decides the
    one-time backfill.
  * measurement is strictly AFTER each deploy: errors in the window from the
    function's LastModified to now, invoked once, plus a 20-minute settle so the
    scheduled runs are counted too.
Gate: portwatch ports with yoy > 0; census-us / repo-monitor / import-sentinel
0 errors after their deploys.
"""
import json
import re
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
            return datetime.fromisoformat(lm.replace("+0000", "+00:00"))
        time.sleep(20)
    r.warn(f"{fn}: deploy not observed within {secs}s")
    return None


def errors_since(fn, since):
    md = cw.get_metric_data(MetricDataQueries=[
        {"Id": "i", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations", "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 60, "Stat": "Sum"}, "ReturnData": True},
        {"Id": "e", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Errors", "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 60, "Stat": "Sum"}, "ReturnData": True}],
        StartTime=since, EndTime=datetime.now(timezone.utc) + timedelta(minutes=1))
    res = {m["Id"]: sum(m.get("Values") or [0]) for m in md.get("MetricDataResults") or []}
    return int(res.get("i", 0)), int(res.get("e", 0))


def log_lines(fn, since, pattern=None, limit=30):
    try:
        kw = {"logGroupName": f"/aws/lambda/{fn}", "startTime": int(since.timestamp() * 1000), "limit": limit}
        if pattern:
            kw["filterPattern"] = pattern
        return [e["message"].rstrip()[:260] for e in (logs.filter_log_events(**kw).get("events") or [])]
    except Exception as e:  # noqa: BLE001
        return [f"log read failed: {str(e)[:120]}"]


def main():
    with report("5113-fix-wave-6") as r:
        r.heading("ops 5113 -- fix wave 6: post-deploy-only verification")
        fails = []
        deployed = {}
        for fn, marker in (("justhodl-portwatch", "v1.6.5"), ("justhodl-census-us", None), ("justhodl-repo-monitor", None)):
            deployed[fn] = wait_deploy(r, fn, marker=marker)
        r.section("portwatch v1.6.5")
        if deployed.get("justhodl-portwatch"):
            t0 = datetime.now(timezone.utc)
            try:
                resp = lam.invoke(FunctionName="justhodl-portwatch", InvocationType="RequestResponse", Payload=b"{}")
                r.log(f"invoke {resp.get('StatusCode')} {resp['Payload'].read()[:160]}")
            except Exception as e:  # noqa: BLE001
                r.warn(f"sync: {str(e)[:100]} -> async")
                lam.invoke(FunctionName="justhodl-portwatch", InvocationType="Event", Payload=b"{}")
                time.sleep(300)
            pw, lm = get_json("data/portwatch.json")
            if pw and (pw.get("generated_at") or "") > t0.isoformat():
                ports = pw.get("ports") or []
                wy = [p for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float))]
                r.log(f"v{pw.get('version')}: ports={len(ports)} with_yoy={len(wy)} requests={json.dumps(pw.get('requests'))} history_through={json.dumps(pw.get('history_through'))} errors={json.dumps(pw.get('errors'))[:200]}")
                r.log(f"  sample: {[(p.get('name'), p.get('country'), p.get('yoy_pct'), p.get('n_days')) for p in wy[:10]]}")
                r.log(f"  n_days: {sorted(p.get('n_days') or 0 for p in ports)[:6]} … {sorted(p.get('n_days') or 0 for p in ports)[-4:]}")
                r.kv(engine="portwatch", ports=len(ports), with_yoy=len(wy), requests=(pw.get("requests") or {}).get("n"))
                if not wy:
                    fails.append("portwatch: no ports with yoy")
            else:
                fails.append("portwatch feed not regenerated")
        r.section("census-us / repo-monitor / import-sentinel: invoke, settle 20 min, count errors after deploy")
        for fn in ("justhodl-census-us", "justhodl-repo-monitor", "justhodl-import-sentinel"):
            lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        time.sleep(1200)
        for fn in ("justhodl-census-us", "justhodl-repo-monitor", "justhodl-import-sentinel"):
            since = deployed.get(fn) or datetime(2026, 9, 2, 2, 53, tzinfo=timezone.utc)
            inv, err = errors_since(fn, since + timedelta(minutes=1))
            samples = log_lines(fn, since + timedelta(minutes=1), pattern='?"[ERROR]" ?Traceback ?"Task timed out" ?HTTP_ERR', limit=8)
            r.log(f"{fn}: since {since.isoformat(timespec='minutes')} invocations={inv} errors={err} samples={json.dumps(samples)[:500]}")
            r.kv(engine=fn, since=since.isoformat(timespec="minutes"), invocations=inv, errors=err)
            if err > 0 or any("HTTP_ERR" in s and "429" not in s for s in samples):
                fails.append(f"{fn}: {err} errors after deploy")
        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("VERDICT: GREEN -- portwatch ports with yoy, census-us / repo-monitor / import-sentinel error-free after their deploys")


if __name__ == "__main__":
    main()
