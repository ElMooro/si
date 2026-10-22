"""ops 6615 - Khalid's (A), 2026-10-22 13:22 ET: "Keep the policy ($0) - then let me make the monitoring honest: re-point
the alarm at the daily ping (24-hour period, 1 datapoint) so it fires only when a provider really fails, and stop
counting the deliberately-empty AI fields as sev1. Pages keep saying 'offline'."

 1. justhodl-llm-providers-down: read the live definition (backed up to the private bucket), then put_metric_alarm with
    the same metric (JustHodl/LLM ProvidersUp), statistic, threshold, comparison and actions, and Period 86400 s,
    EvaluationPeriods 1, DatapointsToAlarm 1, TreatMissingData notBreaching - the health ping runs once a day (04:43 UTC,
    the daily rule), so a 6-hour period could only ever report "missing".  Read back; log the state afterwards.
 2. After deploy-lambdas ships the contract gate with the declared-status overlay (llm_status may be "available" or
    "unavailable" for data/ka-analysis.json and data/khalid-analysis.json): wait for the new code, invoke the gate once
    in check mode, read data/contract-violations.json - the two FIELD_VALUE llm_status sev1 rows are expected gone;
    report the sev1 set that remains (ROW_COLLAPSE / MISSING_KEYS) by artifact for the next piece of work.
Limits: no caps, budgets, IAM, lifecycle or archive changes; nothing faster than daily.
"""
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ALARM = "justhodl-llm-providers-down"
GATE = "justhodl-contract-gate"
DISPATCHED_AT = datetime(2026, 10, 22, 17, 50, 0, tzinfo=timezone.utc)   # the deploy dispatch for the overlay change
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=600)
KEEP = ("AlarmName", "AlarmDescription", "ActionsEnabled", "OKActions", "AlarmActions", "InsufficientDataActions", "MetricName", "Namespace",
        "Statistic", "ExtendedStatistic", "Dimensions", "Unit", "Threshold", "ComparisonOperator", "Tags", "ThresholdMetricId", "EvaluateLowSampleCountPercentile")


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def main():
    cw, s3, lam = c("cloudwatch"), c("s3"), c("lambda")
    with report("ops_6615_llm_alarm_daily_and_contract_recheck") as r:
        r.heading("ops 6615 - llm-providers-down on the daily ping; contract gate accepts the declared no-paid state")
        r.section("1. the alarm")
        al = cw.describe_alarms(AlarmNames=[ALARM]).get("MetricAlarms", [])
        if not al:
            r.fail("%s not found" % ALARM)
            raise SystemExit(1)
        a = al[0]
        r.log("before: %s %s %s %s %s thr %s period %s eval %s datapoints %s missing %s actions %s" % (
            a.get("Namespace"), a.get("MetricName"), a.get("Statistic") or a.get("ExtendedStatistic"), a.get("ComparisonOperator"), a.get("StateValue"), a.get("Threshold"),
            a.get("Period"), a.get("EvaluationPeriods"), a.get("DatapointsToAlarm"), a.get("TreatMissingData"), [x.split(":")[-1] for x in a.get("AlarmActions", [])]))
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        s3.put_object(Bucket=PRIVATE, Key="backups/alarms/%s/%s.json" % (stamp, ALARM), Body=json.dumps(a, default=str).encode(), ServerSideEncryption="AES256")
        kw = {k: a[k] for k in KEEP if k in a and a[k] not in (None, [], "")}
        if "Statistic" in kw and "ExtendedStatistic" in kw:
            kw.pop("ExtendedStatistic")
        kw.update(Period=86400, EvaluationPeriods=1, DatapointsToAlarm=1, TreatMissingData="notBreaching")
        kw["AlarmDescription"] = ((a.get("AlarmDescription") or "").split(" | 2026-10-22")[0] +
                                  " | 2026-10-22 (Khalid, policy A): one datapoint a day from justhodl-llm-health's 04:43 UTC ping; missing data is not a failure; fires only when ProvidersUp < 1")[:1024]
        cw.put_metric_alarm(**kw)
        time.sleep(5)
        b = cw.describe_alarms(AlarmNames=[ALARM])["MetricAlarms"][0]
        ok = b.get("Period") == 86400 and b.get("EvaluationPeriods") == 1 and b.get("DatapointsToAlarm") == 1 and b.get("TreatMissingData") == "notBreaching" \
            and b.get("MetricName") == a.get("MetricName") and b.get("Namespace") == a.get("Namespace") and b.get("Threshold") == a.get("Threshold") \
            and b.get("ComparisonOperator") == a.get("ComparisonOperator") and sorted(b.get("AlarmActions", [])) == sorted(a.get("AlarmActions", []))
        r.log("after: period %s eval %s datapoints %s missing %s state %s (%s)" % (b.get("Period"), b.get("EvaluationPeriods"), b.get("DatapointsToAlarm"), b.get("TreatMissingData"), b.get("StateValue"), (b.get("StateReason") or "")[:120]))
        r.kv(alarm=ALARM, period_before=a.get("Period"), period_after=b.get("Period"), missing_before=a.get("TreatMissingData"), missing_after=b.get("TreatMissingData"), state_after=b.get("StateValue"), verified=ok, backup="backups/alarms/%s/" % stamp)
        if not ok:
            r.fail("alarm read-back differs from the intended definition")
            raise SystemExit(1)
        r.ok("%s re-pointed at the daily ping (backup %s)" % (ALARM, stamp))

        r.section("2. the contract gate with the declared-status overlay")
        deadline = time.time() + 12 * 60
        while True:
            cf = lam.get_function_configuration(FunctionName=GATE)
            fresh = datetime.fromisoformat(cf["LastModified"].replace("+0000", "+00:00")) >= DISPATCHED_AT
            if fresh or time.time() > deadline:
                break
            time.sleep(30)
        r.log("%s modified %s -> new code: %s" % (GATE, cf["LastModified"][:19], fresh))
        if not fresh:
            r.fail("deploy-lambdas has not shipped the contract gate yet - run again after it finishes")
            raise SystemExit(1)
        resp = lam.invoke(FunctionName=GATE, Payload=json.dumps({"mode": "check"}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        r.log("gate check: error %s; %s" % (resp.get("FunctionError"), json.dumps(body, default=str)[:500]))
        cv = json.loads(s3.get_object(Bucket=LIVE, Key="data/contract-violations.json")["Body"].read())
        viol = cv.get("violations") or []
        sev1 = [v for v in viol if v.get("sev") == 1]
        llm = [v for v in sev1 if "llm_status" in (v.get("detail") or "")]
        by_cls = Counter(v.get("cls") for v in sev1)
        r.log("contract-violations.json generated %s: sev1 %d by class %s; llm_status rows %d; sev2 STALE %d" % (
            cv.get("generated_at"), len(sev1), json.dumps(by_cls.most_common()), len(llm), sum(1 for v in viol if v.get("sev") == 2 and v.get("cls") == "STALE")))
        r.log("sev1 by artifact: %s" % json.dumps(sorted((v.get("cls"), v.get("artifact"), (v.get("detail") or "")[:60]) for v in sev1))[:5000])
        r.kv(sev1=len(sev1), sev1_llm_status=len(llm), sev1_row_collapse=by_cls.get("ROW_COLLAPSE", 0), sev1_missing_keys=by_cls.get("MISSING_KEYS", 0))
        if llm:
            r.fail("llm_status is still counted as sev1: %s" % json.dumps(llm)[:400])
            raise SystemExit(1)
        r.ok("no llm_status sev1 left; %d sev1 remain (%s)" % (len(sev1), json.dumps(by_cls.most_common())))


if __name__ == "__main__":
    main()
