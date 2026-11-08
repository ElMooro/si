"""ops 6762 - READ-ONLY probe: why did justhodl-leaders (born Nov 5, daily 03:50 UTC, always writes on success) not write
data/leaders.json on Sunday 2026-11-08? ops 6761 read leaders.json at 31.5 h (last write Sat 03:50) while strength (03:40,
same bank) wrote at 03:40 today. This op reads the Scheduler schedule's state, 4 days of CloudWatch invocations / errors /
throttles / duration, and the last two log streams' events (redacted: key=value secrets, long opaque tokens) for
justhodl-leaders and, as the control, justhodl-strength. Invokes nothing, changes nothing, writes nothing but this report."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
TARGETS = {"justhodl-leaders": ("justhodl-leaders-daily", "data/leaders.json"),
           "justhodl-strength": ("justhodl-strength-daily", "data/strength.json")}
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


def selftest():
    red = redact("GET https://x/stable/quote?symbol=AAPL&apikey=abc123&limit=5")
    assert "abc123" not in red and "symbol=AAPL" in red
    assert redact("justhodl-leaders wrote data/leaders.json 3087200 bytes") == "justhodl-leaders wrote data/leaders.json 3087200 bytes"
    assert redact("id AbCdEfGhIjKlMnOpQrStUvWxYz0123456789 end") == "id *** end"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    sch = boto3.client("scheduler", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6762_leaders_sunday_miss_probe") as r:
        r.heading("ops 6762 - why justhodl-leaders did not write on Sunday %s (Scheduler state, 4-day metrics, last log streams; "
                  "strength as the control; read-only)" % now.date())
        for fn, (sname, key) in TARGETS.items():
            short = fn.replace("justhodl-", "")
            r.section("%s" % fn)
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                age = (now - h["LastModified"]).total_seconds() / 3600
                r.log("%s: %.1f h old (%d B), last modified %s" % (key, age, h["ContentLength"], h["LastModified"].isoformat(timespec="seconds")))
                facts[short + "_age_h"] = round(age, 1)
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s" % (key, str(exc)[:160]))
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                r.log("function: state %s / %s, timeout %ss, memory %s MB, last modified %s, runtime %s" % (
                    c.get("State"), c.get("LastUpdateStatus"), c.get("Timeout"), c.get("MemorySize"), c.get("LastModified"), c.get("Runtime")))
                facts[short + "_timeout_s"] = c.get("Timeout")
            except Exception as exc:  # noqa: BLE001
                r.warn("get_function_configuration: %s" % str(exc)[:160])
            try:
                s_ = sch.get_schedule(Name=sname, GroupName="default")
                r.log("schedule %s: %s %s (tz %s) | target %s | FTW %s" % (sname, s_.get("State"), s_.get("ScheduleExpression"),
                      s_.get("ScheduleExpressionTimezone"), (s_.get("Target") or {}).get("Arn", "?").split(":")[-1],
                      (s_.get("FlexibleTimeWindow") or {}).get("Mode")))
                facts[short + "_schedule"] = "%s %s" % (s_.get("State"), s_.get("ScheduleExpression"))
            except Exception as exc:  # noqa: BLE001
                r.warn("get_schedule %s: %s" % (sname, str(exc)[:160]))
            start, end = now - timedelta(days=4), now
            q = []
            for tag, m, stat in (("n", "Invocations", "Sum"), ("e", "Errors", "Sum"), ("t", "Throttles", "Sum"), ("d", "Duration", "Maximum")):
                q.append({"Id": tag, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m,
                          "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 86400, "Stat": stat}})
            try:
                res = cw.get_metric_data(MetricDataQueries=q, StartTime=start, EndTime=end)
                days = {}
                for r_ in res["MetricDataResults"]:
                    for t, v in zip(r_["Timestamps"], r_["Values"]):
                        days.setdefault(t.date().isoformat(), {})[r_["Id"]] = v
                series = ", ".join("%s n%d e%d t%d dmax%.0fs" % (d_[5:], v.get("n", 0), v.get("e", 0), v.get("t", 0), v.get("d", 0) / 1000.0)
                                   for d_, v in sorted(days.items()))
                r.log("metrics per UTC day: %s" % (series or "no datapoints in 4 days"))
                facts[short + "_metrics"] = series or "none"
            except Exception as exc:  # noqa: BLE001
                r.warn("get_metric_data: %s" % str(exc)[:160])
            try:
                streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True,
                                                    limit=2).get("logStreams", [])
                for st in streams:
                    evs = logs.get_log_events(logGroupName="/aws/lambda/" + fn, logStreamName=st["logStreamName"], limit=120,
                                              startFromHead=False).get("events", [])
                    starts = [i for i, e in enumerate(evs) if e["message"].startswith("START RequestId")]
                    last = evs[starts[-1]:] if starts else evs
                    r.log("stream %s: last event %s, %d events, last invocation %d lines" % (
                        st["logStreamName"][-20:], datetime.fromtimestamp(st.get("lastEventTimestamp", 0) / 1000, timezone.utc).isoformat(timespec="minutes"),
                        len(evs), len(last)))
                    for e in last[-40:]:
                        stamp = datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M:%S")
                        msg = redact(" ".join(e["message"].split()))
                        r.log("  %s %s" % (stamp, msg[:700]))
                        if "Task timed out" in msg or "Status: timeout" in msg or "[ERROR]" in msg or "Traceback" in msg:
                            facts.setdefault(short + "_error_lines", 0)
                            facts[short + "_error_lines"] += 1
            except logs.exceptions.ResourceNotFoundException:
                r.log("(no log group)")
            except Exception as exc:  # noqa: BLE001
                r.warn("logs: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing invoked, nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
