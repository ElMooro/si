"""ops 6627 - the same log-ingestion brake for $0.10/month instead of ~$19/month (Khalid 2026-10-23: "supposed to be $200").

Cost Explorer (ops 6622): CW:MetricInsightAlarmUsage = $0.60 (Oct 21) and $0.64 (Oct 22) a day, ~$19/month, from the
day the fleet thawed. ops 6624: of the account's two Metrics Insights alarms, justhodl-guard-lambda-recursion-dropped
analyses no metric (no function has ever emitted RecursiveInvocationsDropped) and costs nothing; the whole charge is
justhodl-logs-ingestion-2gb-hour (ops 6580), whose query SELECT SUM(IncomingBytes) FROM SCHEMA("AWS/Logs",
LogGroupName) bills $0.10 per log-group metric analysed per month (98 log groups active in a 3-hour window).
CloudWatch Logs also publishes the account total as AWS/Logs IncomingBytes with no dimensions - the same sum.

STAGED (direct lane). Mutates ONE alarm, in place, under the same name, threshold, period, actions and missing-data
treatment, so nothing that reads alarms by name (the governor, the guardrail notifier, docs) changes:
  1. read the alarm; refuse unless it is the Metrics Insights definition ops 6580 created (or already converted)
  2. PROVE equivalence first: GetMetricData for the last complete hours, the Metrics Insights query and the
     account-level metric side by side - every hour must agree within 2% (or 5 MB); refuse otherwise
  3. back the alarm definition up to the private ops bucket
  4. put the standard alarm (AWS/Logs IncomingBytes, Sum, 3600 s, > 2 GiB, 1 of 1, missing = notBreaching)
  5. read back: standard metric, same threshold / actions; on any mismatch the original definition goes back
Cost: ~3 GetMetricData + a few describe calls (< $0.01).
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
PRIVATE = "justhodl-ops-private-857687956942"
ALARM = "justhodl-logs-ingestion-2gb-hour"
MI_EXPR = 'SELECT SUM(IncomingBytes) FROM SCHEMA("AWS/Logs", LogGroupName)'
THRESHOLD = 2147483648.0


def agree(mi, acct):
    """(ok, rows) - every timestamp present in both series agrees within 2% or 5 MB; at least one hour > 0."""
    rows, ok, nonzero = [], True, False
    for ts in sorted(set(mi) & set(acct)):
        a, b = mi[ts], acct[ts]
        diff = abs(a - b)
        good = diff <= max(5e6, 0.02 * max(a, b))
        ok = ok and good
        nonzero = nonzero or (a > 0 and b > 0)
        rows.append((ts, round(a / 1e6, 2), round(b / 1e6, 2), good))
    return ok and nonzero and len(rows) >= 1, rows


def selftest():
    t = [datetime(2026, 10, 23, h, tzinfo=timezone.utc) for h in range(3)]
    assert agree({t[0]: 100e6, t[1]: 120e6}, {t[0]: 101e6, t[1]: 119e6})[0]
    assert not agree({t[0]: 100e6}, {t[0]: 150e6})[0]
    assert not agree({t[0]: 0.0}, {t[0]: 0.0})[0], "an all-zero window proves nothing"
    assert not agree({t[0]: 100e6}, {t[1]: 100e6})[0], "no overlapping hour proves nothing"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    with report("ops_6627_logs_alarm_account_metric") as r:
        r.heading("ops 6627 - log-ingestion brake on the account-level metric (same alarm, ~$19/month -> $0.10)")
        selftest()
        found = cw.describe_alarms(AlarmNames=[ALARM]).get("MetricAlarms", [])
        if not found:
            r.fail("alarm %s not found" % ALARM)
            raise SystemExit(1)
        a = found[0]
        exprs = [m.get("Expression", "") for m in (a.get("Metrics") or []) if m.get("Expression")]
        already = a.get("Namespace") == "AWS/Logs" and a.get("MetricName") == "IncomingBytes" and not a.get("Metrics")
        if not already and exprs != [MI_EXPR]:
            r.fail("refusing: the alarm is not the ops 6580 definition (expressions %s)" % exprs)
            raise SystemExit(1)
        if a.get("Threshold") != THRESHOLD or a.get("ComparisonOperator") != "GreaterThanThreshold":
            r.fail("refusing: threshold/operator differ from ops 6580 (%s %s)" % (a.get("ComparisonOperator"), a.get("Threshold")))
            raise SystemExit(1)
        r.log("current: %s, state %s, actions %s" % ("standard (already converted)" if already else "Metrics Insights", a.get("StateValue"), a.get("AlarmActions")))

        # 2. equivalence over the last complete hours (Metrics Insights reads at most the last 3 hours)
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(hours=3)
        res = cw.get_metric_data(StartTime=start, EndTime=end, ScanBy="TimestampAscending", MetricDataQueries=[
            {"Id": "mi", "Expression": MI_EXPR, "Period": 3600, "ReturnData": True},
            {"Id": "acct", "MetricStat": {"Metric": {"Namespace": "AWS/Logs", "MetricName": "IncomingBytes"}, "Period": 3600, "Stat": "Sum"}, "ReturnData": True}])
        series = {x["Id"]: dict(zip(x["Timestamps"], x["Values"])) for x in res.get("MetricDataResults", [])}
        ok, rows = agree(series.get("mi", {}), series.get("acct", {}))
        r.log("hourly IncomingBytes, Metrics Insights vs account metric (MB): %s" % [(str(t)[11:16], x, y, g) for t, x, y, g in rows])
        if not ok:
            r.fail("refusing: the account-level metric does not reproduce the query (%s)" % rows)
            raise SystemExit(1)
        r.ok("equivalent: the account-level metric equals the Metrics Insights sum hour by hour")
        if already:
            r.kv(changed=False, hours_compared=len(rows))
            r.ok("already on the account-level metric; nothing to put")
            raise SystemExit(0)

        # 3. backup
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bkey = "backups/alarms/%s-%s.json" % (ALARM, stamp)
        s3.put_object(Bucket=PRIVATE, Key=bkey, Body=json.dumps(a, default=str).encode(), ContentType="application/json",
                      ServerSideEncryption="AES256")
        r.ok("backup: s3://%s/%s" % (PRIVATE, bkey))

        # 4. put the standard alarm under the same name
        desc = ("CloudWatch Logs ingestion over 2 GB in an hour, account-wide ($0.50/GB: a chatty loop). ops 6580; "
                "ops 6627: watches the account-level AWS/Logs IncomingBytes (the same sum) instead of a Metrics Insights "
                "query billed per log group (~$19/month -> $0.10/month).")
        kw = dict(AlarmName=ALARM, AlarmDescription=desc, ActionsEnabled=a.get("ActionsEnabled", True),
                  AlarmActions=a.get("AlarmActions", []), OKActions=a.get("OKActions", []),
                  InsufficientDataActions=a.get("InsufficientDataActions", []),
                  Namespace="AWS/Logs", MetricName="IncomingBytes", Statistic="Sum", Period=3600,
                  EvaluationPeriods=a.get("EvaluationPeriods", 1), DatapointsToAlarm=a.get("DatapointsToAlarm", 1),
                  Threshold=THRESHOLD, ComparisonOperator="GreaterThanThreshold",
                  TreatMissingData=a.get("TreatMissingData", "notBreaching"))
        cw.put_metric_alarm(**kw)
        back = cw.describe_alarms(AlarmNames=[ALARM]).get("MetricAlarms", [{}])[0]
        good = (back.get("Namespace") == "AWS/Logs" and back.get("MetricName") == "IncomingBytes" and not back.get("Metrics")
                and back.get("Threshold") == THRESHOLD and back.get("AlarmActions") == a.get("AlarmActions")
                and back.get("OKActions") == a.get("OKActions") and back.get("Period") == 3600)
        if not good:
            restore = dict(AlarmName=ALARM, AlarmDescription=a.get("AlarmDescription", ""), ActionsEnabled=a.get("ActionsEnabled", True),
                           AlarmActions=a.get("AlarmActions", []), OKActions=a.get("OKActions", []),
                           InsufficientDataActions=a.get("InsufficientDataActions", []), Metrics=a["Metrics"],
                           EvaluationPeriods=a.get("EvaluationPeriods", 1), DatapointsToAlarm=a.get("DatapointsToAlarm", 1),
                           Threshold=THRESHOLD, ComparisonOperator="GreaterThanThreshold",
                           TreatMissingData=a.get("TreatMissingData", "notBreaching"))
            cw.put_metric_alarm(**restore)
            r.fail("read-back mismatch; the Metrics Insights definition has been put back (%s)" % json.dumps(back, default=str)[:300])
            raise SystemExit(1)
        r.ok("put + read-back: standard alarm on AWS/Logs IncomingBytes Sum/3600s > 2 GiB, actions %s, state %s" % (back.get("AlarmActions"), back.get("StateValue")))
        r.kv(changed=True, hours_compared=len(rows), state=back.get("StateValue"))
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
