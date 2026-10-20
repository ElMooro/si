"""ops 6558 - alert channel + blast-radius caps for public Function URLs (Khalid, 2026-10-20: "never ... bankrupt me").

1. justhodl-fleet-alerts e-mail channel: the governor's alerts (cap stages, fuses, cadence, egress) publish here.  An SNS
   e-mail subscription that is never confirmed expires after 3 days, so the October alerts may have gone nowhere.  The
   address is copied from the AWS Budget's own EMAIL subscriber inside the runner (never written in this public repo);
   a pending or missing subscription is (re)created and Khalid gets a confirmation e-mail to click.
2. Function URLs: every function with a URL is listed with its AuthType.  A URL with AuthType NONE can be called by
   anyone on the internet; a heavy engine behind one (3 GB x 10 minutes = about $0.03 per call) is a bankruptcy vector
   the fuses only stop after the runaway alarm's 15-minute window.  Each public, heavy (>= 2 s or >= 1 GB) URL
   function gets reserved concurrency 2: at most 2 copies run at once, whatever a caller does.  While the fleet is
   frozen the governor keeps them at 0 and records 2 as the value its Nov 1 restore puts back (documented drift rule).
   Previous values are saved to the private bucket.  Lightweight API handlers are left alone.
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
ALERT_TOPIC = "arn:aws:sns:%s:%s:justhodl-fleet-alerts" % (REGION, ACCOUNT)
BUDGET = "JustHodl-Monthly-Hard-Cap-600"
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
CAP = 2
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, max_pool_connections=20)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def mask(addr):
    return addr[:2] + "***" + addr[addr.find("@"):] if "@" in addr else "***"


def email_channel(r):
    b, sns = c("budgets"), c("sns")
    emails = set()
    for n in b.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=BUDGET).get("Notifications", []):
        for s in b.describe_subscribers_for_notification(AccountId=ACCOUNT, BudgetName=BUDGET, Notification=n)["Subscribers"]:
            if s["SubscriptionType"] == "EMAIL":
                emails.add(s["Address"])
    subs = sns.list_subscriptions_by_topic(TopicArn=ALERT_TOPIC).get("Subscriptions", [])
    have = {s["Endpoint"]: s["SubscriptionArn"] for s in subs if s["Protocol"] == "email"}
    out = {}
    for e in sorted(emails):
        arn = have.get(e)
        if arn and arn != "PendingConfirmation":
            out[mask(e)] = "confirmed"
            continue
        sns.subscribe(TopicArn=ALERT_TOPIC, Protocol="email", Endpoint=e)
        out[mask(e)] = "confirmation e-mail sent (was %s)" % ("pending" if arn else "absent")
    others = [(s["Protocol"], "confirmed" if s["SubscriptionArn"] != "PendingConfirmation" else "pending") for s in subs if s["Protocol"] != "email"]
    r.log("fleet-alerts subscriptions: email %s; other %s" % (json.dumps(out), others))
    return out


def url_caps(r):
    lam = c("lambda")
    fns = []
    for p in lam.get_paginator("list_functions").paginate():
        fns += p["Functions"]
    cw = c("cloudwatch")
    from datetime import datetime, timedelta, timezone
    end = datetime(2026, 10, 12, tzinfo=timezone.utc)
    start = datetime(2026, 9, 15, tzinfo=timezone.utc)
    urls = {}
    for f in fns:
        name = f["FunctionName"]
        try:
            u = lam.get_function_url_config(FunctionName=name)
            urls[name] = {"auth": u.get("AuthType"), "mem": f.get("MemorySize"), "timeout": f.get("Timeout")}
        except lam.exceptions.ResourceNotFoundException:
            continue
    names = sorted(urls)
    q = [{"Id": "d%d" % i, "ReturnData": True, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Duration",
          "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": int((end - start).total_seconds()), "Stat": "Average"}} for i, n in enumerate(names)]
    avg = {}
    for i in range(0, len(q), 500):
        for res in cw.get_metric_data(MetricDataQueries=q[i:i + 500], StartTime=start, EndTime=end)["MetricDataResults"]:
            avg[res["Id"]] = (res.get("Values") or [0])[0] if res.get("Values") else 0.0
    for i, n in enumerate(names):
        urls[n]["avg_ms"] = round(avg.get("d%d" % i, 0.0))
    public = {n: u for n, u in urls.items() if u["auth"] == "NONE"}
    heavy = {n: u for n, u in public.items() if u["avg_ms"] >= 2000 or (u["mem"] or 0) >= 1024}
    r.log("function URLs: %d total, %d with AuthType NONE (public), %d public + heavy" % (len(urls), len(public), len(heavy)))
    before, changed, skipped = {}, [], []
    for n in sorted(heavy):
        try:
            cur = lam.get_function_concurrency(FunctionName=n).get("ReservedConcurrentExecutions")
        except Exception as e:  # noqa: BLE001
            skipped.append((n, type(e).__name__))
            continue
        before[n] = cur
        if cur is not None and cur != 0 and cur <= CAP:
            continue                         # already capped
        # held at 0 by the governor: setting the cap now un-holds it for a minute; the tick below re-holds it and records
        # the cap as the value its Nov 1 restore puts back (governor drift rule, enforce_drift in lambda_function.py)
        try:
            lam.put_function_concurrency(FunctionName=n, ReservedConcurrentExecutions=CAP)
            changed.append(n)
        except Exception as e:  # noqa: BLE001
            skipped.append((n, type(e).__name__))
    held = [n for n in changed if before.get(n) == 0]
    reheld, recorded = 0, 0
    if held:
        for attempt in range(6):            # governor reserved concurrency 1: a running tick answers 429
            try:
                res = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps({"action": "tick", "source": "ops"}).encode())
                body = json.loads(res["Payload"].read() or b"{}")
                r.log("governor tick: stage %s drift %s" % (body.get("stage"), json.dumps(body.get("drift"))[:200]))
                break
            except lam.exceptions.TooManyRequestsException:
                time.sleep(15)
        st = json.loads(c("s3").get_object(Bucket=PRIVATE, Key="spend-governor/state.json")["Body"].read())
        ledger = st.get("ledger") or {}
        for n in held:
            cur = lam.get_function_concurrency(FunctionName=n).get("ReservedConcurrentExecutions")
            e = ledger.get("lambda:" + n) or {}
            reheld += 1 if cur == 0 else 0
            recorded += 1 if (e.get("before") or {}).get("reserved") == CAP else 0
        if reheld < len(held) or recorded < len(held):
            r.warn("%d of %d held functions re-held, %d recorded with restore value %d - check state.json" % (reheld, len(held), recorded, CAP))
        else:
            r.ok("%d held functions re-held at 0 by the governor; restore value %d recorded for every one" % (reheld, CAP))
    c("s3").put_object(Bucket=PRIVATE, Key="backups/url-caps/%s.json" % time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()),
                       Body=json.dumps({"cap": CAP, "before": before, "changed": changed, "skipped": skipped, "public_heavy": heavy},
                                       indent=1, default=str).encode(), ServerSideEncryption="AES256")
    r.log("public heavy URL functions: %d; cap %d applied to %d (%d of them were held by the governor and are held again); skipped %d" % (
        len(heavy), CAP, len(changed), len(held), len(skipped)))
    return urls, public, heavy, changed, len(held), before


def main():
    with report("ops_6558_alerts_and_url_caps") as r:
        r.heading("ops 6558 - fleet-alerts e-mail channel + reserved-concurrency caps on public heavy Function URLs")
        mail = email_channel(r)
        urls, public, heavy, changed, held, before = url_caps(r)
        top = sorted(heavy.items(), key=lambda kv: -(kv[1]["avg_ms"] * (kv[1]["mem"] or 128)))[:15]
        for n, u in top:
            r.log("  public+heavy %-40s mem %5s  avg %7d ms  reserved before %s" % (n, u["mem"], u["avg_ms"], before.get(n)))
        r.kv(email=json.dumps(mail), urls=len(urls), public=len(public), public_heavy=len(heavy), capped_now=len(changed),
             held_by_governor=held)
        r.ok("done")


if __name__ == "__main__":
    main()
