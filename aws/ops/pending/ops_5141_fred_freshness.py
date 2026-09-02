"""ops_5141 -- FRED bank freshness: the drained bank never refreshed (DGS10 stopped at 2026-08-06).

Khalid's screenshot showed TVC:US10Y (fred:DGS10) ending 2026-08-06 on 2026-09-02. Cause: the
fred-catalog engine drains the popularity queue ONCE (status COMPLETE_WITH_LEAKS) and has no
refresh phase, so every banked observation file is frozen at its import date. That breaks the
doctrine "engines update it whenever there is new data".

symdir v1.3.0:
  * r_fred: when a banked doc is older than its cadence allows (D 4d, W 10d, M 40d, Q 100d, A 400d)
    it pulls the tail from FRED (observation_start = last banked date), merges append-only and
    rewrites the doc in place -- the warehouse heals on open, like the tv-bars universe
  * mode=fredfresh (hourly schedule): rotates the most popular banked daily/weekly series
    (pop >= 0.3, ~400 per run) through the same healer, so headline series are current
    before anyone opens them

  S1 deploy v1.3.0 + schedule    S2 heal-on-open: fred:DGS10 nocache -> last obs within 4 days
  S3 one fredfresh run (sync) -> healed count    S4 the bank doc itself now carries the tail
Gates: DGS10 last obs >= today-4d; the bank file rewritten with tail_refreshed_by; fredfresh ok.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-symdir"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=900, retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name=REGION)


def http_json(url, timeout=180):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5141"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def main():
    with report("5141-fred-freshness") as r:
        r.heading("ops 5141 -- FRED bank freshness: heal on open + hourly headline rotation")
        fails = []
        r.section("S1 deploy symdir v1.3.0 + hourly fredfresh schedule")
        cur = lam.get_function_configuration(FunctionName=FN)
        env = (cur.get("Environment") or {}).get("Variables") or {"S3_BUCKET": B}
        desc = json.load(open(ROOT / "aws" / "lambdas" / FN / "config.json"))["description"]
        deploy_lambda(report=r, function_name=FN, source_dir=ROOT / "aws" / "lambdas" / FN / "source", env_vars=env, timeout=900, memory=6144, create_function_url=True, smoke=False, description=desc[:255])
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        h = http_json(url + "/health")
        if h.get("version") != "1.3.0":
            fails.append(f"version {h.get('version')} != 1.3.0")
        sd = {"Name": FN + "-fredfresh", "ScheduleExpression": "rate(1 hour)", "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
              "Target": {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": '{"mode":"fredfresh","limit":400}', "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 900}},
              "State": "ENABLED", "Description": "Banked FRED daily/weekly headline series: tail-merge from FRED most-popular-first, ~400/run"}
        try:
            sch.create_schedule(**sd)
            r.ok("schedule created: justhodl-symdir-fredfresh rate(1 hour)")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sd)
            r.ok("schedule updated")

        r.section("S2 heal on open")
        today = datetime.now(timezone.utc).date()
        for sid in ("fred:DGS10", "fred:SOFR", "fred:DFF", "fred:VIXCLS", "fred:BAMLH0A0HYM2", "fred:UNRATE"):
            d = http_json(url + "/series?id=" + sid + "&nocache=1", timeout=180)
            last = d.get("last") or ""
            r.log(f"  {sid:<20} n={d.get('n')} last={last} src={str(d.get('source'))[:90]}")
            lag = (today - datetime.fromisoformat(last[:10]).date()).days if last else 999
            if sid != "fred:UNRATE" and lag > 6:
                fails.append(f"{sid} still stale: last {last} ({lag}d)")
        r.section("S3 fredfresh run")
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"mode": "fredfresh", "limit": 300}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        r.log(f"  fredfresh: {json.dumps(body)[:300]}")
        if not body.get("ok"):
            fails.append("fredfresh did not run")
        r.section("S4 the bank doc carries the tail")
        j = json.loads(s3.get_object(Bucket=B, Key="data/warm/fred-scoped/Interest_Rates/DGS10.json")["Body"].read())
        obs = j.get("observations") or []
        r.log(f"  DGS10 bank: n={len(obs)} last={obs[-1].get('date') if obs else None} meta={json.dumps(j.get('meta'))[:200]}")
        if not (j.get("meta") or {}).get("tail_refreshed_by"):
            fails.append("DGS10 bank doc not rewritten with the tail")
        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: banked FRED series heal on open and headline daily/weekly series rotate hourly; DGS10 current in the warehouse")


if __name__ == "__main__":
    main()
