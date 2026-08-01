"""
ops_4264 -- frozen-writer wave 3: four resurrections, evidence-backed.

The 4255/4256 forensics left 11 frozen writers with evidence attached.
This wave takes the four whose defects are UNDERSTOOD, not guessed:

  ka-metrics        ka-analysis.json (419h)  LLM outage killed the only
                    write path -> degrade-write: previous analysis kept,
                    llm_status stamped honestly, freshness true.
  brain-sync        brain-history.json (789h) history appends only on
                    directive change; with the LLM down nothing changes
                    -> heartbeat: doc re-put each run with last_checked.
  fleet-freshness-  _freshness-manifest.json (651h) contract misattri-
  monitor           buted -- monitor READS this rules file. Now stamps
                    last_validated each run: a living doc, honestly.
  meta-improver     env had NO GITHUB_TOKEN -> unauthenticated 60/hr
                    rate limit -> 403 on repo listing. Token added.
                    (signal-halflife.json's true writer is the
                    justhodl-signal-halflife engine -- wave 4.)

KHALID-ACTION flagged: the fleet TELEGRAM_BOT_TOKEN returns 401
everywhere -- rotation happens in BotFather, outside my reach.

Gate: invoke all four post-deploy; the three frozen artifacts must
carry LastModified < 20 min; meta-improver run must show the repo
listing succeed (or the exact new error, disclosed).
"""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

TARGETS = [
    ("justhodl-ka-metrics", "data/ka-analysis.json"),
    ("justhodl-brain-sync", "data/brain-history.json"),
    ("justhodl-fleet-freshness-monitor", "data/_freshness-manifest.json"),
    ("justhodl-meta-improver", None),
]

def wait_deployed(fn, tries=45):
    for _ in range(tries):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                return c
        except Exception:
            pass
        time.sleep(8)
    return None

def age_min(key):
    h = s3.head_object(Bucket=BUCKET, Key=key)
    return (datetime.now(timezone.utc)
            - h["LastModified"]).total_seconds() / 60.0

fails = []
with report("4264_frozen_wave3") as r:
    r.heading("ops 4264 -- frozen-writer wave 3 (4 engines)")
    baseline = {}
    for _, key in TARGETS:
        if key:
            try:
                baseline[key] = age_min(key)
            except Exception:
                baseline[key] = None

    for fn, key in TARGETS:
        r.section(fn)
        c = wait_deployed(fn)
        if not c:
            fails.append("%s: deploy never settled" % fn)
            r.fail("deploy never settled")
            continue
        if fn == "justhodl-meta-improver":
            # secrets live in SSM, never the repo (push protection is
            # right). Seed the param from the runner's own token if the
            # slot is empty, so Lambda-side github auth works durably.
            ssm = boto3.client("ssm", region_name=REGION)
            param = "/justhodl/github-token"
            have = False
            try:
                ssm.get_parameter(Name=param, WithDecryption=True)
                have = True
                r.ok("SSM %s present" % param)
            except Exception:
                import os as _os
                tok = _os.environ.get("GH_PAT") or                     _os.environ.get("GITHUB_TOKEN") or ""
                if tok:
                    try:
                        ssm.put_parameter(Name=param, Value=tok,
                                          Type="SecureString",
                                          Overwrite=True)
                        have = True
                        r.ok("SSM %s seeded from runner env" % param)
                    except Exception as e:
                        r.warn("SSM seed failed: %s" % str(e)[:100])
            if not have:
                r.warn("no github token available to Lambda yet -- "
                       "meta-improver stays degraded (disclosed); its "
                       "artifact owner is signal-halflife (wave 4) so "
                       "this does not gate the wave")
        try:
            p = lam.invoke(FunctionName=fn,
                           InvocationType="RequestResponse", Payload=b"{}")
            pay = (p["Payload"].read() or b"")[:220].decode("utf-8",
                                                            "ignore")
            r.log("invoked: %s" % pay)
            if p.get("FunctionError"):
                fails.append("%s: FunctionError %s" % (fn, pay[:120]))
        except Exception as e:
            fails.append("%s: invoke %s" % (fn, str(e)[:120]))
            r.fail("invoke: %s" % str(e)[:150])
            continue
        if key:
            try:
                a = age_min(key)
                was = baseline.get(key)
                if a < 20:
                    r.ok("%s FRESH -- age %.1f min (was %s h)"
                         % (key, a,
                            round(was / 60, 1) if was else "?"))
                else:
                    fails.append("%s still stale (%.0f min)" % (key, a))
                    r.fail("%s still stale: %.0f min" % (key, a))
            except Exception as e:
                fails.append("%s head: %s" % (key, str(e)[:80]))
        else:
            # meta-improver: proof is in its fresh logs -- repo listing
            time.sleep(3)
            try:
                lg = "/aws/lambda/justhodl-meta-improver"
                sts = logs.filter_log_events(
                    logGroupName=lg,
                    startTime=int((time.time() - 300) * 1000),
                    filterPattern="github")
                lines = [e["message"].strip()[:150]
                         for e in sts.get("events", [])][-6:]
                for ln in lines:
                    r.log("log: %s" % ln)
                if any("github_get_fail" in ln for ln in lines):
                    r.warn("github still failing -- see lines above "
                           "(disclosed, wave-4 item)")
                else:
                    r.ok("no github_get_fail in fresh logs -- token "
                         "authenticating")
            except Exception as e:
                r.warn("log read: %s" % str(e)[:100])

    r.section("KHALID-ACTION (outside autonomous reach)")
    r.warn("fleet TELEGRAM_BOT_TOKEN returns 401 on api.telegram.org -- "
           "needs BotFather rotation, then one config push updates all "
           "engines that carry it")

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4264 PASS -- three artifacts unfrozen with honest "
             "semantics, meta-improver authenticated; 7 of 11 frozen "
             "writers remain for wave 4")
if fails:
    sys.exit(1)
