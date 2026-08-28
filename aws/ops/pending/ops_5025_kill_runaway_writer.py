"""ops_5025 -- KILL the runaway writer behind the S3 anomaly.

ops 5024 evidence (Cost Explorer + CloudWatch, 2026-08-28):
  * us-east-1 PutObject ~500k/day every day since Aug-9, unchanged by the
    Aug-26 downshift (4986) -- the DR kill (4988) only fixed us-west-2.
  * live bucket: 338k objects / 77GB (Aug-1) -> 9.69M objects / 2.59TB
    (Aug-26); +~500k objects and +~130GB per DAY.
  * justhodl-signal-registry-ingest: 1,557,590 invocations / 72h
    (519k/day). Next engine in the fleet: 1,830/day. No repo dir, no
    schedule, no reference anywhere; flagged orphan in ops 2892 (July)
    "awaits keep/delete". DynamoDB ($0.63/day ~ 500k on-demand writes)
    and EventBridge ($0.49/day ~ 490k custom events) match its rate --
    the fingerprint of a self-feeding loop.

  P0 evidence  : config, resource policy (who may invoke), event source
                 mappings, EventBridge rules on every bus targeting it,
                 S3 notifications, SNS subs, function URL, code archive
                 -> data/ops/archive/, code grep for the write keys,
                 24h invocation/error metrics, log tail (what it writes)
  P1 kill      : reserved concurrency = 0 (AWS kill switch, reversible)
                 + disable every trigger found (rules -> DISABLED, ESMs
                 -> disabled, S3 notifications -> entry removed)
                 -> quarantine ledger data/ops/signal-registry-ingest-
                 quarantine.json (exact reversal recipe)
  P2 locate    : write prefixes from code literals + log lines; sample
                 listings prove density + recency
  P3 purge     : lifecycle Expiration=1d + NoncurrentVersionExpiration
                 =1d on the junk prefix(es) in the live bucket AND the
                 us-west-2 DR mirror. Only fires when the prefix is
                 (a) a literal from the function's own code, (b) not a
                 warehouse root, (c) sampled dense+recent. Otherwise
                 PURGE=DEFERRED with the evidence printed.
  P4 verify    : concurrency 0 read back; invocations/min post-kill
GREEN = killed + quarantined (purge may be DEFERRED). RED = kill
did not take.
"""
import io
import json
import re
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

ACCT = "857687956942"
REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
DR = "justhodl-dashboard-live-dr"
FN = "justhodl-signal-registry-ingest"
FN_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
WAREHOUSE_ROOTS = ("data/warm/", "data/raw/", "data/attic/", "archive/",
                   "data/", "")

cfg = Config(read_timeout=60, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
s3w = boto3.client("s3", region_name="us-west-2", config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
sns = boto3.client("sns", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
logs = boto3.client("logs", region_name=REGION, config=cfg)

NOW = datetime.now(timezone.utc)
TS = NOW.strftime("%Y%m%dT%H%M%SZ")


def invocations(minutes):
    r = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Invocations",
        Dimensions=[{"Name": "FunctionName", "Value": FN}],
        StartTime=datetime.now(timezone.utc) - timedelta(minutes=minutes),
        EndTime=datetime.now(timezone.utc), Period=60,
        Statistics=["Sum"])
    pts = sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])
    return [(p["Timestamp"].strftime("%H:%M"), int(p["Sum"])) for p in pts]


with report("ops_5025_kill_runaway_writer") as R:
    fails = []
    ledger = {"function": FN, "at": NOW.isoformat(timespec="seconds"),
              "reason": "ops 5025 -- 519k invocations/day, ~500k "
                        "PutObject/day, orphan (no repo dir/schedule); "
                        "S3 anomaly since 2026-08-09",
              "reversal": []}

    # ------------------------------------------------------------ P0
    R.section("P0 evidence")
    conf = {}
    try:
        f = lam.get_function(FunctionName=FN)
        conf = f["Configuration"]
        R.log("  runtime=%s mem=%s timeout=%s lastmod=%s size=%s" % (
            conf.get("Runtime"), conf.get("MemorySize"),
            conf.get("Timeout"), conf.get("LastModified"),
            conf.get("CodeSize")))
        R.log("  desc: %s" % (conf.get("Description") or "")[:200])
        R.log("  env keys: %s" % sorted(
            (conf.get("Environment") or {}).get("Variables", {}).keys()))
        R.log("  role: %s" % (conf.get("Role") or "").rsplit("/", 1)[-1])
        try:
            rc = lam.get_function_concurrency(FunctionName=FN)
            R.log("  reserved concurrency BEFORE: %s" % rc.get(
                "ReservedConcurrentExecutions", "unreserved"))
        except Exception as e:
            R.log("  concurrency read: %s" % str(e)[:80])
        # archive code before anything is touched
        code = b""
        try:
            with urllib.request.urlopen(f["Code"]["Location"],
                                        timeout=120) as r_:
                code = r_.read()
            key = "data/ops/archive/%s-%s.zip" % (FN, TS)
            s3.put_object(Bucket=LIVE, Key=key, Body=code,
                          ContentType="application/zip")
            R.log("  code archived -> %s (%d bytes)" % (key, len(code)))
            ledger["code_archive"] = key
        except Exception as e:
            R.log("  code archive err %s" % str(e)[:100])
    except lam.exceptions.ResourceNotFoundException:
        R.log("  FUNCTION NOT FOUND -- nothing to kill (already gone)")
        code = b""
    except Exception as e:
        R.log("  get_function err %s" % str(e)[:120])
        code = b""

    # code grep: where does it write, what does it publish, who does it call
    literals = set()
    hints = []
    if code:
        try:
            zf = zipfile.ZipFile(io.BytesIO(code))
            names = zf.namelist()
            R.log("  zip files: %d %s" % (len(names), names[:12]))
            for nm in names:
                if not nm.endswith(".py"):
                    continue
                src = zf.read(nm).decode("utf-8", "replace")
                for i, line in enumerate(src.splitlines(), 1):
                    ll = line.strip()
                    if re.search(r"put_object|put_events|put_item|"
                                 r"batch_write|invoke\(|send_message|"
                                 r"publish\(|Key\s*=|Bucket\s*=|"
                                 r"TableName|EventBusName|def lambda_handler|"
                                 r"Records|eventSource|strftime|uuid",
                                 ll):
                        hints.append("%s:%d: %s" % (nm, i, ll[:150]))
                for m in re.finditer(r"['\"]((?:data|signals?|registry|"
                                     r"archive|events?)/[A-Za-z0-9_./{}%-]*)"
                                     r"['\"]", src):
                    literals.add(m.group(1))
            for h in hints[:60]:
                R.log("    %s" % h)
            R.log("  key literals in code: %s" % sorted(literals)[:40])
        except Exception as e:
            R.log("  code grep err %s" % str(e)[:100])

    # who can invoke it
    rule_arns = []
    try:
        pol = json.loads(lam.get_policy(FunctionName=FN)["Policy"])
        for st in pol.get("Statement", []):
            prin = st.get("Principal", {})
            src = ((st.get("Condition") or {}).get("ArnLike") or
                   (st.get("Condition") or {}).get("ArnEquals") or {})
            src_arn = src.get("AWS:SourceArn") or src.get("aws:SourceArn")
            R.log("  policy: %s <- %s src=%s" % (
                st.get("Sid", "")[:40], json.dumps(prin)[:80], src_arn))
            if src_arn and ":rule/" in src_arn:
                rule_arns.append(src_arn)
            ledger.setdefault("policy_statements", []).append(st)
    except Exception as e:
        R.log("  policy: %s" % str(e)[:100])

    esms = []
    try:
        for m in lam.list_event_source_mappings(FunctionName=FN).get(
                "EventSourceMappings", []):
            esms.append(m)
            R.log("  ESM %s src=%s state=%s batch=%s" % (
                m.get("UUID"), m.get("EventSourceArn"), m.get("State"),
                m.get("BatchSize")))
    except Exception as e:
        R.log("  esm: %s" % str(e)[:100])

    rules = []   # (bus, name)
    try:
        buses = [b["Name"] for b in ev.list_event_buses().get(
            "EventBuses", [])]
        R.log("  event buses: %s" % buses)
        for bus in buses:
            try:
                names = ev.list_rule_names_by_target(
                    TargetArn=FN_ARN, EventBusName=bus).get("RuleNames", [])
                for n in names:
                    rules.append((bus, n))
                    d = ev.describe_rule(Name=n, EventBusName=bus)
                    R.log("  RULE %s@%s state=%s sched=%s pattern=%s" % (
                        n, bus, d.get("State"), d.get("ScheduleExpression"),
                        (d.get("EventPattern") or "")[:200]))
            except Exception as e:
                R.log("  rules@%s: %s" % (bus, str(e)[:80]))
    except Exception as e:
        R.log("  buses: %s" % str(e)[:100])
    for arn in rule_arns:
        bus_name = "default"
        parts = arn.split(":rule/", 1)[1]
        if "/" in parts:
            bus_name, rname = parts.split("/", 1)
        else:
            rname = parts
        if (bus_name, rname) not in rules:
            rules.append((bus_name, rname))
            R.log("  (policy-only) rule %s@%s" % (rname, bus_name))

    s3_hits = []
    try:
        nc = s3.get_bucket_notification_configuration(Bucket=LIVE)
        for c in nc.get("LambdaFunctionConfigurations", []):
            tgt = c.get("LambdaFunctionArn", "")
            filt = json.dumps(c.get("Filter") or {})
            R.log("  S3 notif -> %s events=%s %s" % (
                tgt.rsplit(":", 1)[-1], c.get("Events"), filt[:120]))
            if tgt.rsplit(":", 1)[-1] == FN:
                s3_hits.append(c.get("Id"))
        ledger["s3_notification_before"] = {
            k: v for k, v in nc.items() if k != "ResponseMetadata"}
    except Exception as e:
        R.log("  s3 notif: %s" % str(e)[:100])

    sns_subs = []
    try:
        pag = sns.get_paginator("list_subscriptions")
        for pg in pag.paginate():
            for sb in pg.get("Subscriptions", []):
                if sb.get("Endpoint") == FN_ARN:
                    sns_subs.append(sb)
                    R.log("  SNS sub %s topic=%s" % (
                        sb.get("SubscriptionArn", "")[-20:],
                        sb.get("TopicArn")))
    except Exception as e:
        R.log("  sns: %s" % str(e)[:80])
    try:
        u = lam.get_function_url_config(FunctionName=FN)
        R.log("  FUNCTION URL: %s auth=%s" % (u.get("FunctionUrl"),
                                                u.get("AuthType")))
        ledger["function_url"] = u.get("FunctionUrl")
    except Exception:
        R.log("  function url: none")

    # what has it been doing -- metrics + log tail
    try:
        pts = invocations(24 * 60)
        tot = sum(n for _, n in pts)
        R.log("  invocations last 24h: %d (%.1f/min avg); last 5 min: %s"
              % (tot, tot / max(1, len(pts)), pts[-5:]))
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": FN}],
            StartTime=NOW - timedelta(hours=24), EndTime=NOW,
            Period=86400, Statistics=["Sum"])
        R.log("  errors last 24h: %s" % [
            int(p["Sum"]) for p in r.get("Datapoints", [])])
    except Exception as e:
        R.log("  metrics err %s" % str(e)[:80])
    log_keys = set()
    try:
        r = logs.filter_log_events(
            logGroupName="/aws/lambda/" + FN,
            startTime=int((NOW - timedelta(minutes=30)).timestamp() * 1000),
            limit=60, interleaved=True)
        evs = r.get("events", [])
        R.log("  log tail (%d events, last 30 min):" % len(evs))
        for e_ in evs[-25:]:
            msg = e_.get("message", "").strip().replace("\n", " | ")
            if msg.startswith(("START", "END")):
                continue
            R.log("    %s" % msg[:220])
            for m in re.finditer(r"(data/[A-Za-z0-9_./-]+)", msg):
                log_keys.add(m.group(1))
        R.log("  keys seen in logs: %s" % sorted(log_keys)[:20])
    except Exception as e:
        R.log("  logs err %s" % str(e)[:100])

    # ------------------------------------------------------------ P1
    R.section("P1 kill switch + trigger quarantine")
    killed = False
    if conf:
        try:
            lam.put_function_concurrency(FunctionName=FN,
                                         ReservedConcurrentExecutions=0)
            time.sleep(2)
            rc = lam.get_function_concurrency(FunctionName=FN)
            killed = rc.get("ReservedConcurrentExecutions") == 0
            R.log("  reserved concurrency -> %s  %s" % (
                rc.get("ReservedConcurrentExecutions"),
                "KILLED" if killed else "NOT APPLIED"))
            ledger["reversal"].append(
                "lam.delete_function_concurrency(FunctionName=%r)" % FN)
        except Exception as e:
            R.log("  concurrency err %s" % str(e)[:120])
        if not killed:
            fails.append("P1:kill")
        for bus, n in rules:
            try:
                ev.disable_rule(Name=n, EventBusName=bus)
                R.log("  rule %s@%s -> DISABLED" % (n, bus))
                ledger["reversal"].append(
                    "ev.enable_rule(Name=%r, EventBusName=%r)" % (n, bus))
            except Exception as e:
                R.log("  disable rule %s: %s" % (n, str(e)[:80]))
        for m in esms:
            try:
                lam.update_event_source_mapping(UUID=m["UUID"],
                                                Enabled=False)
                R.log("  ESM %s -> disabled" % m["UUID"])
                ledger["reversal"].append(
                    "lam.update_event_source_mapping(UUID=%r, Enabled=True)"
                    % m["UUID"])
            except Exception as e:
                R.log("  esm disable %s: %s" % (m["UUID"], str(e)[:80]))
        if s3_hits:
            try:
                nc = s3.get_bucket_notification_configuration(Bucket=LIVE)
                new = {k: v for k, v in nc.items()
                       if k != "ResponseMetadata"}
                new["LambdaFunctionConfigurations"] = [
                    c for c in nc.get("LambdaFunctionConfigurations", [])
                    if c.get("LambdaFunctionArn", "").rsplit(":", 1)[-1]
                    != FN]
                s3.put_bucket_notification_configuration(
                    Bucket=LIVE, NotificationConfiguration=new)
                R.log("  S3 notifications targeting %s removed (%d)" % (
                    FN, len(s3_hits)))
                ledger["reversal"].append(
                    "s3.put_bucket_notification_configuration(<ledger."
                    "s3_notification_before>)")
            except Exception as e:
                R.log("  s3 notif edit: %s" % str(e)[:100])
        for sb in sns_subs:
            R.log("  SNS subscription left in place (function throttled "
                  "at 0); %s" % sb.get("SubscriptionArn", "")[-30:])
    else:
        R.log("  no function -> nothing to kill")

    # ------------------------------------------------------------ P2
    R.section("P2 locate the objects it wrote")
    cands = set()
    for lit in literals | log_keys:
        p = re.split(r"[{%]", lit)[0]          # cut at first template token
        p = p.rsplit("/", 1)[0] + "/" if "/" in p else p
        if p and p not in WAREHOUSE_ROOTS and p.count("/") >= 1:
            cands.add(p)
    cands = sorted(cands)
    R.log("  candidate prefixes: %s" % cands[:15])
    dense = {}
    for p in cands[:8]:
        try:
            r = s3.list_objects_v2(Bucket=LIVE, Prefix=p, MaxKeys=1000)
            objs = r.get("Contents", [])
            if not objs:
                R.log("  %-50s EMPTY" % p)
                continue
            recent = sum(1 for o in objs
                         if o["LastModified"] >= NOW - timedelta(days=2))
            sz = sum(o["Size"] for o in objs) / max(1, len(objs))
            R.log("  %-50s sample=%d truncated=%s recent48h=%d avg=%.0fB "
                  "first=%s last=%s" % (
                      p[:50], len(objs), r.get("IsTruncated"), recent, sz,
                      objs[0]["Key"][len(p):][:40],
                      objs[-1]["Key"][len(p):][:40]))
            dense[p] = {"sample": len(objs), "truncated":
                        bool(r.get("IsTruncated")), "recent48h": recent,
                        "avg_bytes": round(sz)}
        except Exception as e:
            R.log("  %s list err %s" % (p, str(e)[:80]))
    # fallback density probe: newest key per top-level data/ prefix
    if not dense:
        try:
            r = s3.list_objects_v2(Bucket=LIVE, Prefix="data/",
                                   Delimiter="/", MaxKeys=1000)
            tops = [c["Prefix"] for c in r.get("CommonPrefixes", [])]
            R.log("  top-level data/ prefixes: %d" % len(tops))
            for p in tops:
                r2 = s3.list_objects_v2(Bucket=LIVE, Prefix=p, MaxKeys=1000)
                objs = r2.get("Contents", [])
                recent = sum(1 for o in objs
                             if o["LastModified"] >= NOW - timedelta(days=1))
                if r2.get("IsTruncated") and recent >= 500:
                    R.log("  HOT %-40s recent24h=%d/%d truncated" % (
                        p, recent, len(objs)))
                    dense[p] = {"sample": len(objs), "truncated": True,
                                "recent48h": recent, "avg_bytes": round(
                                    sum(o["Size"] for o in objs) /
                                    max(1, len(objs)))}
        except Exception as e:
            R.log("  probe err %s" % str(e)[:80])

    # ------------------------------------------------------------ P3
    R.section("P3 purge via lifecycle (only unambiguous junk prefixes)")
    purge = [p for p, d in dense.items()
             if d["truncated"] and d["recent48h"] >= 600
             and p in {c for c in cands} and p not in WAREHOUSE_ROOTS
             and not any(p.startswith(w) and len(p) <= len(w) + 4
                         for w in ("data/warm/", "data/raw/", "data/attic/"))]
    R.log("  purge prefixes: %s" % purge)
    ledger["purge_prefixes"] = purge

    def add_expiry_rules(client, bucket, prefixes):
        try:
            lc = client.get_bucket_lifecycle_configuration(Bucket=bucket)
            rules_ = [r_ for r_ in lc.get("Rules", [])
                      if not r_.get("ID", "").startswith("ops5025-purge-")]
        except Exception:
            rules_ = []
        for i, p in enumerate(prefixes):
            rules_.append({
                "ID": "ops5025-purge-%d" % i, "Status": "Enabled",
                "Filter": {"Prefix": p},
                "Expiration": {"Days": 1},
                "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}})
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration={"Rules": rules_})
        return len(rules_)

    if purge:
        for client, bucket in ((s3, LIVE), (s3w, DR)):
            try:
                n = add_expiry_rules(client, bucket, purge)
                R.log("  %s: lifecycle now %d rules (purge %s, expire "
                      "1d + noncurrent 1d) -- S3 deletes asynchronously "
                      "within ~24-48h at zero request cost" % (
                          bucket, n, purge))
            except Exception as e:
                R.log("  %s lifecycle err %s" % (bucket, str(e)[:120]))
    else:
        R.log("  PURGE DEFERRED -- no prefix met all three conditions; "
              "the S3 Inventory (armed by 5024, first manifest <48h) "
              "will name it exactly; next op purges from that")

    # ------------------------------------------------------------ P4
    R.section("P4 verify")
    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/signal-registry-ingest-quarantine.json",
                      Body=json.dumps(ledger, indent=1,
                                      default=str).encode(),
                      ContentType="application/json")
        R.log("  ledger -> data/ops/signal-registry-ingest-quarantine.json")
    except Exception as e:
        R.log("  ledger err %s" % str(e)[:80])
    if conf:
        time.sleep(90)
        pts = invocations(6)
        R.log("  invocations/min after kill: %s" % pts)
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Throttles",
                Dimensions=[{"Name": "FunctionName", "Value": FN}],
                StartTime=datetime.now(timezone.utc) - timedelta(minutes=6),
                EndTime=datetime.now(timezone.utc), Period=60,
                Statistics=["Sum"])
            R.log("  throttles/min after kill: %s" % sorted(
                [(p["Timestamp"].strftime("%H:%M"), int(p["Sum"]))
                 for p in r.get("Datapoints", [])]))
        except Exception as e:
            R.log("  throttle metric err %s" % str(e)[:80])

    if fails:
        R.log("ops 5025 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(killed=killed, rules_disabled=len(rules), esms_disabled=len(esms),
         s3_notifs_removed=len(s3_hits), purge=",".join(purge) or "DEFERRED")
    R.log("ops 5025 GREEN -- %s throttled to zero and quarantined; "
          "PutObject/GetObject/DDB/EventBridge burn stops now; storage "
          "purge %s" % (FN, "armed via lifecycle" if purge else
                        "deferred to inventory"))
