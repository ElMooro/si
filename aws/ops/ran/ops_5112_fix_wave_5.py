"""ops_5112 -- fix wave 5: the tail after the 5111 re-measure.

  1  portwatch v1.6.4 (coverage-based one-time backfill) -> ports with yoy
  2  manufacturing-global-agent (ISM block removed) and repo-monitor (series id
     now visible in HTTP_ERR lines): deploy-wait, run, read the remaining errors
  3  paced probe of every FRED id repo-monitor still requests -> which one 400s
  4  boj-full: invocations per 10 minutes over the last 3h (is the storm really
     over after the warm-rule detach at 02:10?), error samples after the
     NameError deploy (02:31), the warm rule's current targets
  5  import-sentinel (writes data/import-health.json, not import-sentinel.json),
     census-us, insider-trades: error samples over the last 90 minutes with a
     broad filter
Gate: portwatch ports with yoy > 0.
"""
import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FRED_KEY = "2f057499936072679d8843d7fce99989"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=200, retries={"max_attempts": 2}))
ev = boto3.client("events", region_name=REGION)
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
        return [e["message"].rstrip()[:260] for e in (logs.filter_log_events(**kw).get("events") or [])]
    except Exception as e:  # noqa: BLE001
        return [f"log read failed: {str(e)[:120]}"]


def distinct(lines, n=6):
    seen, out = set(), []
    for ln in lines:
        key = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}|\d{4}-\d\d-\d\d[T ][\d:.]+Z?|\d+", "#", ln)[:110]
        if key in seen:
            continue
        seen.add(key)
        out.append(ln)
        if len(out) >= n:
            break
    return out


def main():
    with report("5112-fix-wave-5") as r:
        r.heading("ops 5112 -- fix wave 5: the tail")
        fails = []

        r.section("1. portwatch v1.6.4")
        if wait_deploy(r, "justhodl-portwatch", marker="v1.6.4"):
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
                with_yoy = [p for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float))]
                r.log(f"portwatch v{pw.get('version')}: ports={len(ports)} with_yoy={len(with_yoy)} requests={json.dumps(pw.get('requests'))} history_through={json.dumps(pw.get('history_through'))} errors={json.dumps(pw.get('errors'))[:240]}")
                r.log(f"  sample: {[(p.get('name'), p.get('country'), p.get('yoy_pct'), p.get('n_days')) for p in with_yoy[:8]]}")
                r.log(f"  n_days distribution: {sorted(p.get('n_days') or 0 for p in ports)[:12]} … {sorted(p.get('n_days') or 0 for p in ports)[-5:]}")
                r.kv(engine="portwatch", ports=len(ports), with_yoy=len(with_yoy), requests=(pw.get("requests") or {}).get("n"))
                if not with_yoy:
                    fails.append("portwatch: still no ports with yoy")
            else:
                fails.append("portwatch feed not regenerated")

        r.section("2. manufacturing-global-agent / repo-monitor")
        for fn, pat, wait_s in (("manufacturing-global-agent", '?"Error fetching" ?Traceback', 60), ("justhodl-repo-monitor", "?HTTP_ERR ?SRF_ERR ?Traceback", 90)):
            if wait_deploy(r, fn):
                t0 = datetime.now(timezone.utc)
                lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
                time.sleep(wait_s)
                errs = distinct(log_lines(fn, t0, pattern=pat, limit=20))
                r.log(f"{fn}: distinct error lines after deploy: {len(errs)} {json.dumps(errs)[:700]}")
                r.kv(engine=fn, errors=len(errs))

        r.section("3. repo-monitor ids probe (paced)")
        ids = ["EFFR", "M2SL", "OBFR", "OTHL1690", "RRPONTSYD", "SOFR", "SOFRVOLUME", "SWPT", "WLCFLPCL", "AMERIBOR", "SOFR25", "SOFR75",
               "WDTGAL", "RIFSPPFAAD90NB", "RIFSPPNAAD90NB", "DCPN3M", "T10Y2Y", "T10Y3M"]
        dead = []
        for sid in ids:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=1"
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "ops5112"}), timeout=20) as rr:
                    d = json.loads(rr.read().decode())
                obs = [(o["date"], o["value"]) for o in d.get("observations") or []]
                r.log(f"  {sid}: 200 {obs[:1]}")
                if obs and obs[0][0] < "2025":
                    dead.append((sid, "stale " + obs[0][0]))
            except urllib.error.HTTPError as e:
                r.log(f"  {sid}: HTTP {e.code}")
                dead.append((sid, f"HTTP {e.code}"))
            except Exception as e:  # noqa: BLE001
                r.log(f"  {sid}: {str(e)[:60]}")
            time.sleep(1.5)
        r.log(f"repo-monitor dead/stale ids: {dead}")
        r.kv(engine="repo-monitor-probe", dead=",".join(f"{a}({b})" for a, b in dead) or "none")

        r.section("4. boj-full after the detach + NameError deploy")
        try:
            tg = ev.list_targets_by_rule(Rule="benzinga-news-agent-warm")
            r.log(f"warm rule targets now: {[(t['Id'], t['Arn'].split(':')[-1]) for t in tg.get('Targets') or []]}")
        except Exception as e:  # noqa: BLE001
            r.warn(str(e)[:100])
        md = cw.get_metric_data(MetricDataQueries=[
            {"Id": "i", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations", "Dimensions": [{"Name": "FunctionName", "Value": "justhodl-boj-full"}]}, "Period": 600, "Stat": "Sum"}, "ReturnData": True},
            {"Id": "e", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Errors", "Dimensions": [{"Name": "FunctionName", "Value": "justhodl-boj-full"}]}, "Period": 600, "Stat": "Sum"}, "ReturnData": True}],
            StartTime=NOW - timedelta(hours=3), EndTime=NOW)
        res = {m["Id"]: sorted(zip(m.get("Timestamps") or [], m.get("Values") or [])) for m in md.get("MetricDataResults") or []}
        r.log("boj invocations/errors per 10 min (last 3h): " + " ".join(f"{ts.strftime('%H:%M')}={int(v)}/{int(dict(res.get('e') or []).get(ts, 0))}" for ts, v in res.get("i") or []))
        errs = distinct(log_lines("justhodl-boj-full", datetime(2026, 9, 2, 2, 32, tzinfo=timezone.utc), pattern='?"[ERROR]" ?Traceback ?"Error Type"', limit=40))
        r.log(f"boj distinct errors since 02:32: {len(errs)} {json.dumps(errs)[:900]}")
        r.kv(engine="boj-full", distinct_errors_since_fix=len(errs))

        r.section("5. import-sentinel / census-us / insider-trades error samples (90 min)")
        for fn in ("justhodl-import-sentinel", "justhodl-census-us", "justhodl-insider-trades"):
            errs = distinct(log_lines(fn, NOW - timedelta(minutes=90), pattern='?"[ERROR]" ?Traceback ?"Task timed out" ?"Error Type"', limit=60), n=8)
            r.log(f"{fn}: {len(errs)} distinct: {json.dumps(errs)[:1200]}")
            r.kv(engine=fn, distinct_errors=len(errs))
        ih, lm = get_json("data/import-health.json")
        r.log(f"import-health.json: {'present' if ih else 'MISSING'} generated_at={(ih or {}).get('generated_at')} keys={list((ih or {}).keys())[:10]}")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("VERDICT: GREEN")


if __name__ == "__main__":
    main()
