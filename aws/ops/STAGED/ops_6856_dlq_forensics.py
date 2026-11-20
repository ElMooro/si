"""ops 6856 - READ-ONLY forensics of the shared dead-letter queue justhodl-dlq-default (Khalid, 2026-11-20: the fleet
monitor e-mailed "DLQ has 3255 messages - async failures detected"; investigate and fix).

Every Lambda in the account sends an asynchronous event to this one queue when the event is finally given up on (the
function failed all its attempts, or the event aged out while the function was throttled / held at concurrency 0). The
queue's messages do not name the function, so this script attributes them three independent ways:

  A. the queue itself: depth, in flight, retention, age of the oldest message, policy shape, redrive settings;
  B. the queue's CloudWatch record over 15 days: messages sent per day (when did they arrive?), depth, deletes;
  C. per function, CloudWatch AsyncEventsDropped over 15 days (the events Lambda gave up on - exactly what lands here),
     ranked, per day; for the top droppers: reserved concurrency, governor hold, retry settings, live triggers,
     invocations / errors / throttles, and the redacted tail of the newest log stream;
  D. the messages: every message is RECEIVED (never deleted; it becomes visible again after the visibility timeout),
     and counted by arrival day, ErrorCode / ErrorMessage and the shape of the original event - an EventBridge rule or
     Scheduler schedule is resolved to its target function. Payload VALUES are never written: only the event's source,
     detail-type, the rule / schedule name and top-level key names;
  E. the monitor: its last state document, its alert history and when it ran.
Deletes nothing, purges nothing, changes nothing; the only invoke is the governor's read-only status.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
ACCOUNT = "857687956942"
LIVE = "justhodl-dashboard-live"
DLQ = "justhodl-dlq-default"
GOVERNOR = "justhodl-spend-governor"
MONITOR = "justhodl-fleet-error-monitor"
DAYS = 15
VISIBILITY_S = 300          # received messages return to the queue by themselves after this
MAX_MESSAGES = 6000
TOP = 30
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|passwd|authorization|signature|key)(\s*[=:]\s*|%3D)[^\s&\"',}]+")
BEARER = re.compile(r"(?i)\bbearer\s+\S+")
LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-+/=]{20,}")
AWS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
HEX32 = re.compile(r"[0-9a-fA-F]{32,}")
REQ_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def _opaque(tok):
    if AWS_KEY.fullmatch(tok) or HEX32.fullmatch(tok):
        return True
    has_d, has_u, has_l = any(c.isdigit() for c in tok), any(c.isupper() for c in tok), any(c.islower() for c in tok)
    if has_d and has_u and has_l:
        return True
    return len(tok) >= 28 and has_d and (has_u or has_l) and "-" not in tok and "_" not in tok


def redact(line):
    """A line safe for a committed report: key=value secrets and long opaque tokens masked. Pure."""
    line = BEARER.sub("Bearer ***", str(line))
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", line)
    return LONG_TOKEN.sub(lambda m: "***" if _opaque(m.group(0)) else m.group(0), line)


def error_family(msg):
    """An ErrorMessage reduced to its family: request ids and numbers folded so identical failures group. Pure."""
    s = REQ_ID.sub("<id>", str(msg or ""))
    s = re.sub(r"\d+(\.\d+)?", "N", s)
    return redact(" ".join(s.split()))[:110]


def classify_body(body):
    """(kind, label, trigger) for one dead-lettered event - never returns payload values. trigger is ("rule"|"schedule",
    name) when the event names its EventBridge rule / Scheduler schedule, else None. Pure, for the selftest."""
    try:
        d = json.loads(body)
    except (ValueError, TypeError):
        return "non-json", "", None
    if not isinstance(d, dict):
        return "json-%s" % type(d).__name__, "", None
    if "detail-type" in d and "source" in d:
        res = d.get("resources") or []
        arn = str(res[0]) if res else ""
        name = arn.rsplit("/", 1)[-1] if "/" in arn else ""
        src = str(d.get("source"))[:40]
        if ":scheduler:" in arn or src == "aws.scheduler":
            return "scheduler", name, ("schedule", name) if name else None
        if ":rule/" in arn and (src == "aws.events" or d.get("detail-type") == "Scheduled Event"):
            return "eventbridge-schedule", name, ("rule", name) if name else None
        return "eventbridge-event", "%s | %s" % (src, str(d.get("detail-type"))[:50]), None
    recs = d.get("Records")
    if isinstance(recs, list) and recs and isinstance(recs[0], dict):
        return "records", str(recs[0].get("eventSource") or recs[0].get("EventSource") or "?")[:30], None
    keys = sorted(str(k)[:24] for k in d.keys())
    return "payload", "keys: " + (",".join(keys[:8]) or "(empty)") + (" +%d" % (len(keys) - 8) if len(keys) > 8 else ""), None


def selftest():
    k, lab, trig = classify_body(json.dumps({"version": "0", "detail-type": "Scheduled Event", "source": "aws.events",
                                             "resources": ["arn:aws:events:us-east-1:1:rule/my-rule"], "detail": {}}))
    assert (k, lab, trig) == ("eventbridge-schedule", "my-rule", ("rule", "my-rule"))
    k, lab, trig = classify_body(json.dumps({"detail-type": "Scheduled Event", "source": "aws.scheduler",
                                             "resources": ["arn:aws:scheduler:us-east-1:1:schedule/default/s-1"]}))
    assert (k, lab, trig) == ("scheduler", "s-1", ("schedule", "s-1"))
    k, lab, trig = classify_body(json.dumps({"detail-type": "near_miss.extreme", "source": "justhodl.x", "detail": {"apikey": "zzz"}}))
    assert k == "eventbridge-event" and lab == "justhodl.x | near_miss.extreme" and trig is None and "zzz" not in lab
    assert classify_body(json.dumps({"Records": [{"eventSource": "aws:s3"}]}))[:2] == ("records", "aws:s3")
    k, lab, _ = classify_body(json.dumps({"action": "run", "secret": "hunter2"}))
    assert k == "payload" and lab == "keys: action,secret" and "hunter2" not in lab
    assert classify_body("not json")[0] == "non-json" and classify_body("[1]")[0] == "json-list"
    assert error_family("RequestId: 1b2c3d4e-aaaa-bbbb-cccc-0123456789ab Error: Task timed out after 900.00 seconds") == \
        "RequestId: <id> Error: Task timed out after N seconds"
    assert "abc123" not in redact("GET https://x/y?apikey=abc123&z=1")
    print("selftest ok")


def metric_sums(cw, queries_meta, start, end, period):
    """queries_meta: {id: (namespace, metric, dims, stat)} -> {id: {iso-stamp: value}} (paged, 500 queries a call)."""
    ids = list(queries_meta)
    out = {i: {} for i in ids}
    for i in range(0, len(ids), 500):
        q = [{"Id": qid, "MetricStat": {"Metric": {"Namespace": queries_meta[qid][0], "MetricName": queries_meta[qid][1],
                                                    "Dimensions": queries_meta[qid][2]}, "Period": period, "Stat": queries_meta[qid][3]},
              "ReturnData": True} for qid in ids[i:i + 500]]
        tok = None
        while True:
            kw = {"MetricDataQueries": q, "StartTime": start, "EndTime": end}
            if tok:
                kw["NextToken"] = tok
            res = cw.get_metric_data(**kw)
            for r_ in res["MetricDataResults"]:
                for t, v in zip(r_["Timestamps"], r_["Values"]):
                    out[r_["Id"]][t.strftime("%Y-%m-%dT%H")] = out[r_["Id"]].get(t.strftime("%Y-%m-%dT%H"), 0.0) + v
            tok = res.get("NextToken")
            if not tok:
                break
    return out


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=48)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    sqs = boto3.client("sqs", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    evb = boto3.client("events", region_name=REGION, config=cfg)
    sch = boto3.client("scheduler", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    facts = {}
    with report("ops_6856_dlq_forensics") as r:
        r.heading("ops 6856 - dead-letter queue forensics %s (%s: who put 3,255 messages there, when, and why; read-only)" % (now.date(), DLQ))

        r.section("A. the queue")
        url = sqs.get_queue_url(QueueName=DLQ)["QueueUrl"]
        at = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
        vis, infl, delayed = int(at.get("ApproximateNumberOfMessages", 0)), int(at.get("ApproximateNumberOfMessagesNotVisible", 0)), int(at.get("ApproximateNumberOfMessagesDelayed", 0))
        r.log("depth: %d visible + %d in flight + %d delayed | retention %.1f d | visibility timeout %s s | created %s | last modified %s" % (
            vis, infl, delayed, int(at.get("MessageRetentionPeriod", 0)) / 86400, at.get("VisibilityTimeout"),
            datetime.fromtimestamp(int(at.get("CreatedTimestamp", 0)), timezone.utc).date(),
            datetime.fromtimestamp(int(at.get("LastModifiedTimestamp", 0)), timezone.utc).isoformat(timespec="minutes")))
        r.log("redrive policy (this queue's own DLQ): %s | redrive allow policy: %s | encryption: %s" % (
            at.get("RedrivePolicy") or "none", at.get("RedriveAllowPolicy") or "none",
            "SSE-SQS" if at.get("SqsManagedSseEnabled") == "true" else at.get("KmsMasterKeyId") or "none"))
        try:
            pol = json.loads(at.get("Policy") or "{}")
            for s_ in pol.get("Statement", []):
                r.log("policy statement: %s %s on %s | principal %s | condition keys %s" % (
                    s_.get("Effect"), s_.get("Action"), str(s_.get("Resource")).split(":")[-1], redact(json.dumps(s_.get("Principal")))[:120],
                    sorted((s_.get("Condition") or {}).keys())))
        except ValueError:
            r.warn("queue policy not parseable")
        try:
            srcq = sqs.list_dead_letter_source_queues(QueueUrl=url).get("queueUrls", [])
            r.log("SQS queues that use it as their own DLQ: %s" % ([u.rsplit("/", 1)[-1] for u in srcq] or "none"))
        except Exception as exc:  # noqa: BLE001
            r.warn("list_dead_letter_source_queues: %s" % str(exc)[:120])
        facts.update(depth_visible=vis, depth_inflight=infl, retention_days=round(int(at.get("MessageRetentionPeriod", 0)) / 86400, 1))

        r.section("B. the queue's CloudWatch record, %d days (UTC days)" % DAYS)
        qd = [{"Name": "QueueName", "Value": DLQ}]
        qm = metric_sums(cw, {"sent": ("AWS/SQS", "NumberOfMessagesSent", qd, "Sum"), "deleted": ("AWS/SQS", "NumberOfMessagesDeleted", qd, "Sum"),
                              "received": ("AWS/SQS", "NumberOfMessagesReceived", qd, "Sum"),
                              "depth": ("AWS/SQS", "ApproximateNumberOfMessagesVisible", qd, "Maximum"),
                              "oldest": ("AWS/SQS", "ApproximateAgeOfOldestMessage", qd, "Maximum")}, start, now, 86400)
        sent_by_day = {k[:10]: int(v) for k, v in qm["sent"].items()}
        for day in sorted(set(k[:10] for m in qm.values() for k in m)):
            g = lambda name: qm[name].get(day + "T00", 0)   # noqa: E731
            r.log("%s: sent %5d | deleted %5d | received %5d | depth max %5d | oldest %.1f d" % (
                day, g("sent"), g("deleted"), g("received"), g("depth"), g("oldest") / 86400))
        facts["sqs_sent_15d"] = sum(sent_by_day.values())
        hour0 = now.replace(minute=0, second=0, microsecond=0)
        hq = metric_sums(cw, {"sent": ("AWS/SQS", "NumberOfMessagesSent", qd, "Sum")}, hour0 - timedelta(hours=72), now, 3600)
        hot = sorted(((k, int(v)) for k, v in hq["sent"].items() if v), key=lambda kv: kv[0])
        r.log("last 72 h, hours with arrivals (%d h, %d messages): %s" % (len(hot), sum(v for _, v in hot), ", ".join("%s %d" % (k[5:], v) for k, v in hot)[:1500]))

        r.section("C. per function: AsyncEventsDropped over %d days (the events Lambda gave up on)" % DAYS)
        fns = []
        for page in lam.get_paginator("list_functions").paginate():
            fns += page["Functions"]
        with_dlq = sum(1 for f in fns if (f.get("DeadLetterConfig") or {}).get("TargetArn", "").endswith(":" + DLQ))
        r.log("%d functions; %d send their dead letters to %s; other targets: %s" % (len(fns), with_dlq, DLQ, dict(Counter(
            (f.get("DeadLetterConfig") or {}).get("TargetArn", "none").rsplit(":", 1)[-1] for f in fns if not (f.get("DeadLetterConfig") or {}).get("TargetArn", "").endswith(":" + DLQ)))))
        fmeta = {f["FunctionName"]: f for f in fns}
        names = sorted(fmeta)
        dims = lambda n: [{"Name": "FunctionName", "Value": n}]   # noqa: E731
        dropped = metric_sums(cw, {"d%d" % i: ("AWS/Lambda", "AsyncEventsDropped", dims(n), "Sum") for i, n in enumerate(names)}, start, now, 86400)
        per_fn = {}
        for i, n in enumerate(names):
            days = {k[:10]: int(v) for k, v in dropped["d%d" % i].items() if v}
            if days:
                per_fn[n] = days
        ranked = sorted(per_fn.items(), key=lambda kv: -sum(kv[1].values()))
        total_dropped = sum(sum(d.values()) for d in per_fn.values())
        r.log("%d functions dropped async events in %d days, %d events in all (queue: %d sent in the same window)" % (
            len(per_fn), DAYS, total_dropped, facts["sqs_sent_15d"]))
        day_tot = Counter()
        for n, days in per_fn.items():
            for d_, v in days.items():
                day_tot[d_] += v
        r.log("dropped per day (all functions) vs queue sent: %s" % ", ".join("%s %d/%d" % (d_[5:], day_tot.get(d_, 0), sent_by_day.get(d_, 0))
                                                                            for d_ in sorted(set(day_tot) | set(sent_by_day))))
        retention_floor = (now - timedelta(seconds=int(at.get("MessageRetentionPeriod", 1209600)))).date().isoformat()
        in_window = sorted(((n, sum(v for d_, v in days.items() if d_ >= retention_floor)) for n, days in per_fn.items()), key=lambda kv: -kv[1])
        r.log("still inside the queue's retention (since %s): %d events from %d functions" % (
            retention_floor, sum(v for _, v in in_window), sum(1 for _, v in in_window if v)))
        for n, days in ranked[:60]:
            r.log("  %-44s %5d | %s" % (n.replace("justhodl-", ""), sum(days.values()), ", ".join("%s %d" % (d_[5:], v) for d_, v in sorted(days.items()))[:330]))
        if len(ranked) > 60:
            r.log("  ... and %d more functions with %d events" % (len(ranked) - 60, sum(sum(d.values()) for _, d in ranked[60:])))
        facts.update(fns_dropping=len(per_fn), dropped_15d=total_dropped, top_droppers="; ".join("%s %d" % (n.replace("justhodl-", ""), sum(d.values())) for n, d in ranked[:8]))

        # the governor's holds (read-only status)
        capped, anom = set(), set()
        try:
            st = {}
            for attempt in range(5):
                try:
                    st = json.loads(lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"status"}')["Payload"].read() or b"{}")
                    break
                except lam.exceptions.TooManyRequestsException:
                    time.sleep(20)
            capped = set(st.get("capped") or [])
            anom = {fn for fn, _ in (st.get("anomaly_held") or [])}
            r.log("governor: stage %s | CAP %s | ANOM %s" % (st.get("stage"), sorted(capped) or "none", sorted(anom) or "none"))
        except Exception as exc:  # noqa: BLE001
            r.warn("governor status: %s: %s" % (type(exc).__name__, str(exc)[:120]))

        # schedule -> target function, for section D and the trigger listing
        sched_target, by_target = {}, defaultdict(list)
        try:
            tok = None
            while True:
                kw = {"MaxResults": 100}
                if tok:
                    kw["NextToken"] = tok
                res = sch.list_schedules(**kw)
                for s_ in res.get("Schedules", []):
                    arn = (s_.get("Target") or {}).get("Arn") or ""
                    fn = arn.split(":function:")[-1].split(":")[0] if ":function:" in arn else ""
                    sched_target[s_.get("Name")] = fn
                    by_target[fn].append((s_.get("Name"), s_.get("State"), s_.get("GroupName")))
                tok = res.get("NextToken")
                if not tok:
                    break
        except Exception as exc:  # noqa: BLE001
            r.warn("list_schedules: %s" % str(exc)[:120])

        r.log("the top %d droppers in detail:" % TOP)
        top_names = [n for n, _ in ranked[:TOP]]
        mm = {}
        for i, n in enumerate(top_names):
            for tag, m in (("n", "Invocations"), ("e", "Errors"), ("t", "Throttles"), ("r", "AsyncEventsReceived")):
                mm["%s%d" % (tag, i)] = ("AWS/Lambda", m, dims(n), "Sum")
        tm = metric_sums(cw, mm, start + timedelta(days=DAYS - 5), now, 86400) if mm else {}
        for i, n in enumerate(top_names):
            f = fmeta[n]
            try:
                reserved = lam.get_function_concurrency(FunctionName=n).get("ReservedConcurrentExecutions")
            except Exception:  # noqa: BLE001
                reserved = "?"
            try:
                eic = lam.get_function_event_invoke_config(FunctionName=n)
                retry = "retries %s, max age %s s" % (eic.get("MaximumRetryAttempts"), eic.get("MaximumEventAgeInSeconds"))
            except lam.exceptions.ResourceNotFoundException:
                retry = "default (2 retries, 6 h)"
            except Exception as exc:  # noqa: BLE001
                retry = "?(%s)" % str(exc)[:40]
            trig = []
            try:
                for rule in evb.list_rule_names_by_target(TargetArn=f["FunctionArn"]).get("RuleNames", []):
                    d = evb.describe_rule(Name=rule)
                    trig.append("rule %s %s %s" % (rule, d.get("State"), d.get("ScheduleExpression") or "event-pattern"))
            except Exception as exc:  # noqa: BLE001
                trig.append("rules ?(%s)" % str(exc)[:40])
            for sname, sstate, group in by_target.get(n, []):
                try:
                    expr = sch.get_schedule(Name=sname, GroupName=group or "default").get("ScheduleExpression", "?")
                except Exception:  # noqa: BLE001
                    expr = "?"
                trig.append("sched %s %s %s" % (sname, sstate, expr))
            series = []
            for day in sorted(set(k[:10] for tag in "netr" for k in tm.get("%s%d" % (tag, i), {}))):
                g = lambda tag: int(tm["%s%d" % (tag, i)].get(day + "T00", 0))   # noqa: E731
                series.append("%s recv%d n%d e%d t%d" % (day[5:], g("r"), g("n"), g("e"), g("t")))
            hold = "CAP" if n in capped else "ANOM" if n in anom else "-"
            r.log("%s: dropped %d | reserved %s | governor hold %s | %s | timeout %s s mem %s MB | last modified %s" % (
                n, sum(per_fn[n].values()), reserved, hold, retry, f.get("Timeout"), f.get("MemorySize"), str(f.get("LastModified"))[:16]))
            r.log("    triggers: %s" % ("; ".join(trig)[:500] or "NONE (invoked by another function / the bus)"))
            r.log("    5 d: %s" % (", ".join(series) or "no datapoints"))
            if reserved == 0:
                continue      # held: nothing ran, there is no log to read
            try:
                streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + n, orderBy="LastEventTime", descending=True, limit=1).get("logStreams", [])
                if streams:
                    evs = logs.get_log_events(logGroupName="/aws/lambda/" + n, logStreamName=streams[0]["logStreamName"], limit=60, startFromHead=False).get("events", [])
                    keep = [e for e in evs if not e["message"].startswith(("START RequestId", "END RequestId", "INIT_START"))
                            and any(s in e["message"] for s in ("ERROR", "Error", "error", "Traceback", "timed out", "Task timed", "REPORT", "refused", "429", "FATAL"))]
                    for e in keep[-6:]:
                        stamp = datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M")
                        r.log("    log %s: %s" % (stamp, redact(" ".join(e["message"].split()))[:240]))
            except Exception as exc:  # noqa: BLE001
                r.log("    log: %s" % str(exc)[:80])

        r.section("D. the messages themselves (received, never deleted; visible again after %d s)" % VISIBILITY_S)
        msgs, lock, stop = {}, threading.Lock(), threading.Event()

        def pull():
            empty = 0
            while not stop.is_set() and empty < 4:
                try:
                    res = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=VISIBILITY_S,
                                              MessageAttributeNames=["All"], AttributeNames=["All"])
                except Exception as exc:  # noqa: BLE001
                    with lock:
                        msgs.setdefault("_errors", []).append(str(exc)[:160])
                    return
                got = res.get("Messages", [])
                empty = 0 if got else empty + 1
                with lock:
                    for m in got:
                        msgs[m["MessageId"]] = m
                    if len(msgs) >= MAX_MESSAGES:
                        stop.set()
        with cf.ThreadPoolExecutor(max_workers=10) as ex:
            list(ex.map(lambda _: pull(), range(10)))
        errs = msgs.pop("_errors", [])
        if errs:
            r.warn("receive errors: %s" % errs[:3])
        r.log("%d distinct messages read (queue reported %d visible + %d in flight)" % (len(msgs), vis, infl))
        facts["messages_read"] = len(msgs)
        rule_target = {}

        def target_of(trig):
            if not trig:
                return None
            kind, name = trig
            if kind == "schedule":
                return sched_target.get(name) or "?(schedule %s not found)" % name
            if name not in rule_target:
                try:
                    ts = evb.list_targets_by_rule(Rule=name).get("Targets", [])
                    fn_t = [t["Arn"].split(":function:")[-1].split(":")[0] for t in ts if ":function:" in t.get("Arn", "")]
                    rule_target[name] = ",".join(fn_t) or "?(rule %s has no Lambda target)" % name
                except Exception:  # noqa: BLE001
                    rule_target[name] = "?(rule %s not found)" % name
            return rule_target[name]
        by_day, by_err, by_kind, by_label, by_fn, fn_err, sizes, recv_counts = Counter(), Counter(), Counter(), Counter(), Counter(), defaultdict(Counter), [], Counter()
        fn_days = defaultdict(Counter)
        oldest = newest = None
        for m in msgs.values():
            sent = datetime.fromtimestamp(int(m["Attributes"]["SentTimestamp"]) / 1000, timezone.utc)
            oldest = sent if oldest is None or sent < oldest else oldest
            newest = sent if newest is None or sent > newest else newest
            day = sent.date().isoformat()
            by_day[day] += 1
            ma = m.get("MessageAttributes") or {}
            code = (ma.get("ErrorCode") or {}).get("StringValue") or "-"
            fam = error_family((ma.get("ErrorMessage") or {}).get("StringValue"))
            by_err[(code, fam)] += 1
            kind, label, trig = classify_body(m.get("Body"))
            by_kind[kind] += 1
            by_label[(kind, redact(label)[:90])] += 1
            fn = target_of(trig) or "(direct async invoke / bus event - not named in the message)"
            by_fn[fn] += 1
            fn_err[fn][(code, fam)] += 1
            fn_days[fn][day] += 1
            sizes.append(len(m.get("Body") or ""))
            recv_counts[min(int(m["Attributes"].get("ApproximateReceiveCount", 1)), 5)] += 1
        if msgs:
            r.log("oldest %s | newest %s | body bytes median %d max %d | earlier receives (1 = first time read): %s" % (
                oldest.isoformat(timespec="minutes"), newest.isoformat(timespec="minutes"), sorted(sizes)[len(sizes) // 2], max(sizes), dict(sorted(recv_counts.items()))))
            facts.update(oldest_message=oldest.isoformat(timespec="minutes"), newest_message=newest.isoformat(timespec="minutes"))
        r.log("by arrival day: %s" % ", ".join("%s %d" % (d_[5:], v) for d_, v in sorted(by_day.items())))
        r.log("by kind of original event: %s" % dict(by_kind.most_common()))
        r.log("by ErrorCode | ErrorMessage family:")
        for (code, fam), v in by_err.most_common(25):
            r.log("  %5d  code %s | %s" % (v, code, fam or "(no message)"))
        r.log("by event shape (top 40; names only, no payload values):")
        for (kind, label), v in by_label.most_common(40):
            r.log("  %5d  %s | %s" % (v, kind, label))
        r.log("by target function, where the event names its rule / schedule (top 60):")
        for fn, v in by_fn.most_common(60):
            top_err = fn_err[fn].most_common(2)
            r.log("  %5d  %-52s %s | %s" % (v, fn.replace("justhodl-", "")[:52], " ".join("%s:%d" % (d_[5:], c) for d_, c in sorted(fn_days[fn].items()))[:150],
                                           "; ".join("code %s %s x%d" % (c, f_[:60], k) for (c, f_), k in top_err)))
        facts.update(msg_kinds=json.dumps(dict(by_kind.most_common())), msg_top_errors="; ".join("%s|%s x%d" % (c, f_[:50], v) for (c, f_), v in by_err.most_common(4)),
                     msg_top_functions="; ".join("%s %d" % (fn.replace("justhodl-", "")[:40], v) for fn, v in by_fn.most_common(8)))

        r.section("E. the monitor (%s)" % MONITOR)
        for key in ("data/_fleet-monitor.json", "data/_fleet-monitor-alert-history.json"):
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
                doc = json.loads(obj["Body"].read() or b"{}")
                age = (now - obj["LastModified"]).total_seconds() / 3600
                if key.endswith("history.json"):
                    dl = {k: v for k, v in doc.items() if "DLQ" in k}
                    r.log("%s (%.1f h old): %d keys | DLQ entries %s" % (key, age, len(doc), dl))
                else:
                    r.log("%s (%.1f h old): version %s run %s | scanned %s | detected %s raised %s suppressed %s | dlq %s | sns %s telegram %s" % (
                        key, age, doc.get("version"), doc.get("run_id"), doc.get("n_lambdas_scanned"), doc.get("n_alerts_detected"),
                        doc.get("n_alerts_raised"), doc.get("n_alerts_suppressed"), doc.get("dlq_status"), doc.get("sns_sent"), doc.get("telegram_sent")))
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s" % (key, str(exc)[:120]))
        try:
            f = lam.get_function_configuration(FunctionName=MONITOR)
            trig = []
            for rule in evb.list_rule_names_by_target(TargetArn=f["FunctionArn"]).get("RuleNames", []):
                d = evb.describe_rule(Name=rule)
                trig.append("rule %s %s %s" % (rule, d.get("State"), d.get("ScheduleExpression")))
            for sname, sstate, group in by_target.get(MONITOR, []):
                trig.append("sched %s %s %s" % (sname, sstate, sch.get_schedule(Name=sname, GroupName=group or "default").get("ScheduleExpression")))
            r.log("monitor triggers: %s | last modified %s" % ("; ".join(trig) or "NONE", str(f.get("LastModified"))[:16]))
            hm = metric_sums(cw, {"n": ("AWS/Lambda", "Invocations", dims(MONITOR), "Sum")}, hour0 - timedelta(days=6), now, 3600)
            r.log("monitor invocations, last 6 d (hour UTC x count): %s" % ", ".join("%s x%d" % (k[5:], v) for k, v in sorted(hm["n"].items()) if v))
        except Exception as exc:  # noqa: BLE001
            r.warn("monitor read: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing deleted, nothing purged, nothing changed (%d messages return to the queue within %d s)" % (len(msgs), VISIBILITY_S))
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        try:
            main()
        except SystemExit:
            raise
        except Exception:
            import traceback
            traceback.print_exc()
            sys.exit(1)
