"""ops 6714 - treasury-api + macro-financial-intelligence fully on (Khalid, 2026-11-02 11:08 ET: "turn these on"),
step 2, after the deploy lane shipped commit a2509ce58 (treasury-api FiscalData query fix; macro-financial-intelligence
v2.0.1 - no constants, honest labels, page crash fixed).  ops 6712 already lifted both governor holds.

A. the deployed code carries the fixes (the function's code is fetched and grepped - no invoke).
B. triggers, as declared in config/schedule-manifest.json (the reconciler's source of truth):
   * macro-financial-daily-execution rate(1 day): DISABLED -> ENABLED (existing rule, existing target);
   * treasury-api-history-update cron(0 10 ? * MON,THU *) UTC -> treasury-api, input {"source":"aws.events"}: an
     EventBridge Scheduler schedule (the classic rule quota is full; the role is the one treasury-rehypo-daily uses,
     checked by IAM simulation first); the old treasury-auto-update rule was deleted before October.
   The deployed manifest copy (s3://justhodl-dashboard-live/config/schedule-manifest.json) gets exactly these two changes
   (previous text backed up to the private bucket); the reconciler mode is reported, not changed.
C. one run of each, read back: treasury-api's history file (instruments, newest auctions) + its API answer; the macro
   brief (no constant fields, FRED values) + both public Function URL pages fetched over HTTPS.
No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
MACRO_RULE, TREAS_RULE = "macro-financial-daily-execution", "treasury-api-history-update"
TREAS_ARN = "arn:aws:lambda:%s:%s:function:treasury-api" % (REGION, ACCOUNT)
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=120)
ONCE = Config(retries={"max_attempts": 1}, read_timeout=900, connect_timeout=10)
MARKERS = {"treasury-api": ("lambda_function.py", "page%%5Bsize%%5D=10000", "not.is.null,auction_date"),
           "macro-financial-intelligence": ("lambda_function.py", "VERSION='2.0.1'", "'health':7.5")}


def code_has(lam, fn):
    url = lam.get_function(FunctionName=fn)["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=60) as resp:
        z = zipfile.ZipFile(io.BytesIO(resp.read()))
    name, must, must_not = MARKERS[fn]
    src = z.read(name).decode("utf-8", "replace")
    return must in src and must_not not in src


def run(lam1, fn, payload, r):
    resp = lam1.invoke(FunctionName=fn, InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps(payload).encode())
    body = resp["Payload"].read().decode("utf-8", "replace")
    tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
    r.log("%s %s -> %s %s | %s" % (fn, json.dumps(payload), resp.get("StatusCode"), resp.get("FunctionError") or "", body[:400]))
    r.log("   log: %s" % " | ".join(l.strip() for l in tail.splitlines() if l.strip() and not l.startswith(("START", "END", "REPORT", "XRAY")))[-700:])
    return body


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 justhodl-ops/6714"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    lam1 = boto3.client("lambda", region_name=REGION, config=ONCE)
    ev = boto3.client("events", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    ssm = boto3.client("ssm", region_name=REGION, config=CFG)
    sch = boto3.client("scheduler", region_name=REGION, config=CFG)
    iam = boto3.client("iam", config=CFG)
    with report("ops_6714_two_engines_on") as r:
        r.heading("ops 6714 - treasury-api + macro-financial-intelligence: deployed fixes, triggers, one run each")
        r.section("A. deployed code")
        for fn in MARKERS:
            ok = code_has(lam, fn)
            cfg = lam.get_function_configuration(FunctionName=fn)
            rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            (r.ok if ok else r.fail)("%s: fix in deployed code %s | modified %s | reserved concurrency %s" % (fn, ok, cfg["LastModified"][:19], rc))
            if not ok:
                raise SystemExit(1)

        r.section("B. triggers + the deployed manifest copy")
        rule = ev.describe_rule(Name=MACRO_RULE)
        tg = ev.list_targets_by_rule(Rule=MACRO_RULE).get("Targets", [])
        r.log("%s before: %s %s targets %s" % (MACRO_RULE, rule.get("State"), rule.get("ScheduleExpression"), [t["Arn"].split(":")[-1] for t in tg]))
        if rule.get("State") != "ENABLED":
            ev.enable_rule(Name=MACRO_RULE)
        r.ok("%s now %s" % (MACRO_RULE, ev.describe_rule(Name=MACRO_RULE).get("State")))
        ref = sch.get_schedule(Name="treasury-rehypo-daily", GroupName="default")
        role = ref["Target"]["RoleArn"]
        sim = iam.simulate_principal_policy(PolicySourceArn=role, ActionNames=["lambda:InvokeFunction"], ResourceArns=[TREAS_ARN])
        dec = sim["EvaluationResults"][0]["EvalDecision"]
        r.log("scheduler role %s may invoke treasury-api: %s" % (role.split("/")[-1], dec))
        if dec != "allowed":
            r.fail("the shared scheduler role cannot invoke treasury-api - not created")
            raise SystemExit(1)
        try:
            live_s = sch.get_schedule(Name=TREAS_RULE, GroupName="default")
            r.log("%s exists already" % TREAS_RULE)
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=TREAS_RULE, GroupName="default", ScheduleExpression="cron(0 10 ? * MON,THU *)", ScheduleExpressionTimezone="UTC",
                                State="ENABLED", FlexibleTimeWindow={"Mode": "OFF"},
                                Description="treasury-api history update Mon+Thu 10:00 UTC (restored 2026-11-02, ops 6714)",
                                Target={"Arn": TREAS_ARN, "RoleArn": role, "Input": json.dumps({"source": "aws.events"}, separators=(",", ":")),
                                        "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 3600}})
            live_s = sch.get_schedule(Name=TREAS_RULE, GroupName="default")
        ok = live_s.get("ScheduleExpression") == "cron(0 10 ? * MON,THU *)" and live_s.get("State") == "ENABLED" and live_s["Target"]["Arn"] == TREAS_ARN
        (r.ok if ok else r.fail)("%s: %s %s %s -> %s input %s" % (TREAS_RULE, live_s.get("State"), live_s.get("ScheduleExpression"),
                                                                  live_s.get("ScheduleExpressionTimezone"), live_s["Target"]["Arn"].split(":")[-1], live_s["Target"].get("Input")))

        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        live = json.loads(old_text)
        repo = json.loads((ROOT / KEY).read_text(encoding="utf-8"))
        want = {e["name"]: e for e in repo["rules"] + repo["schedules"] if e["name"] in (MACRO_RULE, TREAS_RULE)}
        changed = []
        for e in live["rules"]:
            if e["name"] == MACRO_RULE and e.get("state") != "ENABLED":
                e["state"] = "ENABLED"
                changed.append(MACRO_RULE)
        if TREAS_RULE not in {e["name"] for e in live["schedules"]}:
            live["schedules"].append(want[TREAS_RULE])
            live["schedules"].sort(key=lambda e: e["name"])
            changed.append(TREAS_RULE)
        if changed:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=(json.dumps(live, indent=1, ensure_ascii=False) + "\n").encode("utf-8"),
                          ContentType="application/json", CacheControl="no-cache")
            bk = json.loads(s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read())
            back = {e["name"]: e for e in bk["rules"] + bk["schedules"]}
            ok = back.get(MACRO_RULE, {}).get("state") == "ENABLED" and back.get(TREAS_RULE, {}).get("expr") == "cron(0 10 ? * MON,THU *)"
            (r.ok if ok else r.fail)("deployed manifest: %s changed (backup backups/schedule-manifest/%s/); %d rules" % (changed, stamp, len(back)))
        else:
            r.ok("deployed manifest already declares both")
        try:
            mode = ssm.get_parameter(Name="/justhodl/schedules/mode")["Parameter"]["Value"]
        except Exception as e:  # noqa: BLE001
            mode = "unreadable (%s)" % type(e).__name__
        r.log("schedule reconciler mode: %s (not changed)" % mode)

        r.section("C. one run each, read back")
        before = s3.head_object(Bucket="openbb-lambda-data", Key="treasury_historical_comprehensive.json")["LastModified"]
        run(lam1, "treasury-api", {"source": "aws.events"}, r)
        h = s3.head_object(Bucket="openbb-lambda-data", Key="treasury_historical_comprehensive.json")
        doc = json.loads(s3.get_object(Bucket="openbb-lambda-data", Key="treasury_historical_comprehensive.json")["Body"].read())
        first = (doc.get("updates") or [{}])[0]
        inst = first.get("instruments") or {}
        newest = {k: {f: (v.get("latest_auction") or {}).get(f) for f in ("auction_date", "bid_to_cover_ratio", "high_yield")} for k, v in inst.items()}
        fresh = h["LastModified"] > before and inst
        (r.ok if fresh else r.fail)("history file written %s (was %s), %d updates kept, %d instruments: %s" % (
            h["LastModified"].isoformat()[:19], before.isoformat()[:19], doc.get("total_updates") or 0, len(inst), json.dumps(newest)[:900]))
        body = run(lam1, "treasury-api", {"rawPath": "/auctions/current"}, r)
        ans = json.loads(json.loads(body).get("body") or "{}")
        flat = json.dumps(ans)
        (r.ok if "No complete auction data" not in flat and "auction_date" in flat else r.warn)("treasury-api /auctions/current answers real data: %s" % flat[:500])
        st, page = get(lam.get_function_url_config(FunctionName="treasury-api")["FunctionUrl"] + "auctions/current")
        r.log("treasury-api Function URL /auctions/current: HTTP %s %s" % (st, page[:300]))

        cfg = lam.get_function_configuration(FunctionName="macro-financial-intelligence")
        bucket = cfg["Environment"]["Variables"]["S3_BUCKET"]
        run(lam1, "macro-financial-intelligence", {}, r)
        brief = json.loads(s3.get_object(Bucket=bucket, Key="latest_brief.json")["Body"].read())
        clean = "health" not in brief and "liq" not in brief and brief.get("version") == "2.0.1" and brief.get("inds")
        (r.ok if clean else r.fail)("macro brief: %s" % json.dumps(brief)[:900])
        st, page = get(lam.get_function_url_config(FunctionName="macro-financial-intelligence")["FunctionUrl"])
        good = st == 200 and "Fed MBS holdings (FRED WSHOMCB" in page and "Market Health" not in page and "unhashable" not in page
        (r.ok if good else r.warn)("macro Function URL page: HTTP %s, %d chars, honest labels %s | text: %s" % (
            st, len(page), good, " ".join(__import__("re").sub(r"<style.*?</style>|<[^>]+>", " ", page, flags=__import__("re").S).split())[:600]))
        r.ok("ops 6714 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
