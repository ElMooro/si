"""ops 6813 - READ-ONLY follow-up to ops 6812 (daily health run 2026-11-16): the three findings the status run surfaced
without a log line.

  1. justhodl-auction-crisis-detector: 3 errors of 3 invocations on both Nov 14 and Nov 15 (its daily 19:50 UTC slot;
     3 = the async retries) after green runs on Nov 12 and 13 - the first errors since the Nov 12 apply-lane commit
     (e2c05e0a: config.json, _fred_shim.py, auction_crisis_v2.py, auction_quality.py, lambda_function.py). ops 6812's
     40-line tail printed nothing for it. Read the three newest log streams whole (after the last START each) and
     classify: ImportModuleError / timeout / out-of-memory / unhandled exception, with the exception type and the
     traceback's last frames.
  2. justhodl-portwatch: RAN_NO_WRITE, 'PortWatch preserved publication failed: CaptureError' at 11:20 UTC on Nov 15
     (output 72 h). Read the last invocation whole for the CaptureError's message and what preceded it.
  3. justhodl-sec-8k-enrich: data/sec-cik-ticker-map.json crossed the 7-day line (191 h). Its triggers, per-day
     invocations / errors over 7 days and the newest log tail.
Log lines are redacted (key=value secrets, long opaque tokens) before they are written. Invokes nothing; writes nothing
but this report.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
TARGETS = {
    "justhodl-auction-crisis-detector": {"streams": 3, "keys": ("data/auction-crisis.json",)},
    "justhodl-portwatch": {"streams": 1, "keys": ("data/portwatch.json", "data/warm/portwatch/history/daily-rows.json.gz")},
    "justhodl-sec-8k-enrich": {"streams": 1, "keys": ("data/8k-by-ticker.json", "data/8k-filings-enriched.json", "data/sec-cik-ticker-map.json")},
}
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|passwd|authorization|signature|key)(\s*[=:]\s*|%3D)[^\s&\"',}]+")
BEARER = re.compile(r"(?i)\bbearer\s+\S+")
LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-+/=]{20,}")
AWS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
HEX32 = re.compile(r"[0-9a-fA-F]{32,}")


def _opaque(tok):
    if AWS_KEY.fullmatch(tok) or HEX32.fullmatch(tok):
        return True
    has_d, has_u, has_l = any(c.isdigit() for c in tok), any(c.isupper() for c in tok), any(c.islower() for c in tok)
    if has_d and has_u and has_l:
        return True
    return len(tok) >= 28 and has_d and (has_u or has_l) and "-" not in tok and "_" not in tok


def redact(line):
    line = BEARER.sub("Bearer ***", str(line))
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", line)
    return LONG_TOKEN.sub(lambda m: "***" if _opaque(m.group(0)) else m.group(0), line)


def classify(msgs):
    """Pure: the outcome of one invocation from its log lines."""
    text = "\n".join(msgs)
    if "Runtime.ImportModuleError" in text or "Unable to import module" in text:
        return "import-error"
    if "Task timed out" in text:
        return "timeout"
    if "Runtime.OutOfMemory" in text or "MemoryError" in text:
        return "out-of-memory"
    if "[ERROR]" in text or "Traceback (most recent call last)" in text:
        return "unhandled"
    if "REPORT RequestId" in text:
        return "clean"
    return "incomplete"


def selftest():
    assert classify(["START RequestId: x", "[ERROR] ValueError: y", "REPORT RequestId: x"]) == "unhandled"
    assert classify(["START RequestId: x", "2026 Task timed out after 240.00 seconds", "REPORT RequestId: x"]) == "timeout"
    assert classify(["START RequestId: x", "REPORT RequestId: x Duration: 1 ms"]) == "clean"
    assert classify(["START RequestId: x", "[ERROR] Runtime.ImportModuleError: Unable to import module 'lambda_function'"]) == "import-error"
    assert redact("api_key=abc123 Bearer sometokenvalue") == "api_key=*** Bearer ***"
    assert "***" not in redact("a plain log line about auction-crisis")
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    evb = boto3.client("events", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6813_health_followup_logs") as r:
        r.heading("ops 6813 - health follow-up logs %s: auction-crisis-detector (3/3 errors Nov 14 + 15), portwatch CaptureError, "
                  "sec-8k-enrich cik map >7 d (read-only)" % now.date())
        for fn, spec in TARGETS.items():
            short = fn.replace("justhodl-", "")
            r.section("%s" % short)
            try:
                fc = lam.get_function_configuration(FunctionName=fn)
                r.log("config: timeout %s s | memory %s MB | LastModified %s | state %s/%s | runtime %s" % (
                    fc.get("Timeout"), fc.get("MemorySize"), fc.get("LastModified"), fc.get("State"), fc.get("LastUpdateStatus"), fc.get("Runtime")))
                trig = []
                for rule in evb.list_rule_names_by_target(TargetArn=fc["FunctionArn"]).get("RuleNames", []):
                    d = evb.describe_rule(Name=rule)
                    trig.append("rule %s %s %s" % (rule, d.get("State"), d.get("ScheduleExpression") or ("event-pattern" if d.get("EventPattern") else "?")))
                r.log("EventBridge rules: %s" % ("; ".join(trig) or "none (Scheduler schedules not listed here)"))
            except Exception as exc:  # noqa: BLE001
                r.warn("configuration: %s: %s" % (type(exc).__name__, str(exc)[:160]))
            # per-day invocations / errors / throttles over 7 days
            try:
                start, end = now - timedelta(days=7), now
                series = {}
                for metric, key in (("Invocations", "n"), ("Errors", "e"), ("Throttles", "t")):
                    res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                                   StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"])
                    for dp in res.get("Datapoints", []):
                        series.setdefault(dp["Timestamp"].date().isoformat(), {})[key] = int(dp["Sum"])
                r.log("metrics 7 d: %s" % (", ".join("%s n%d e%d t%d" % (d_[5:], v.get("n", 0), v.get("e", 0), v.get("t", 0)) for d_, v in sorted(series.items())) or "no datapoints"))
                facts[short + "_errors_7d"] = sum(v.get("e", 0) for v in series.values())
            except Exception as exc:  # noqa: BLE001
                r.warn("metrics: %s: %s" % (type(exc).__name__, str(exc)[:160]))
            # outputs
            for key in spec["keys"]:
                try:
                    h = s3.head_object(Bucket=LIVE, Key=key)
                    age = (now - h["LastModified"]).total_seconds() / 3600
                    r.log("%s: %.1f h old (%d B)" % (key, age, h["ContentLength"]))
                    facts[key.split("/")[-1] + "_age_h"] = round(age, 1)
                except Exception as exc:  # noqa: BLE001
                    r.log("%s: %s" % (key, type(exc).__name__))
            # log streams, whole invocations
            try:
                streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True,
                                                    limit=spec["streams"]).get("logStreams", [])
                if not streams:
                    r.warn("no log stream")
                outcomes = Counter()
                for s_ in streams:
                    evs, token = [], None
                    for _ in range(6):   # up to 6 pages backwards from the tail
                        kw = {"logGroupName": "/aws/lambda/" + fn, "logStreamName": s_["logStreamName"], "limit": 500, "startFromHead": False}
                        if token:
                            kw["nextToken"] = token
                        page = logs.get_log_events(**kw)
                        got = page.get("events", [])
                        if not got:
                            break
                        evs = got + evs
                        if page.get("nextBackwardToken") == token:
                            break
                        token = page.get("nextBackwardToken")
                        if len(evs) >= 1500:
                            break
                    # split into invocations on START lines
                    invs, cur = [], []
                    for e in evs:
                        if e["message"].startswith("START RequestId"):
                            if cur:
                                invs.append(cur)
                            cur = [e]
                        else:
                            cur.append(e)
                    if cur:
                        invs.append(cur)
                    r.log("stream %s: %d events, %d invocation(s), last event %s" % (
                        s_["logStreamName"][-24:], len(evs), len(invs),
                        datetime.fromtimestamp(s_.get("lastEventTimestamp", 0) / 1000, timezone.utc).strftime("%m-%d %H:%M")))
                    for inv in invs[-3:]:
                        msgs = [" ".join(e["message"].split()) for e in inv]
                        kind = classify(msgs)
                        outcomes[kind] += 1
                        stamp = datetime.fromtimestamp(inv[0]["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M")
                        r.log("  invocation %s -> %s (%d lines)" % (stamp, kind, len(msgs)))
                        # the lines that matter: errors, tracebacks' last frames, the engine's own summary prints, REPORT
                        keep = [m for m in msgs if not m.startswith(("START RequestId", "END RequestId", "INIT_START"))]
                        if kind != "clean":
                            err_idx = [i for i, m in enumerate(keep) if "[ERROR]" in m or "Traceback" in m or "Task timed out" in m or "Error" in m[:80]]
                            show = keep[-14:] if not err_idx else keep[max(0, err_idx[0] - 4): err_idx[0] + 12]
                        else:
                            show = keep[-6:]
                        for m in show:
                            r.log("    %s" % redact(m)[:300])
                facts[short + "_outcomes"] = json.dumps(dict(outcomes), sort_keys=True)
            except logs.exceptions.ResourceNotFoundException:
                r.warn("no log group")
            except Exception as exc:  # noqa: BLE001
                r.warn("log read: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing changed, nothing invoked")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
