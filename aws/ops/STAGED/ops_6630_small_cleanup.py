"""ops 6630 - the small cleanup Khalid approved on 2026-10-23 ("yes do the small clean up"); ~$6/month.

  A. Secrets Manager: the 9 secrets created May-Sep 2025 (economic-data-api-keys, fred-api-key, polygon-api-key,
     liquidity-agent-keys, multi-agent-api-keys, alphavantage-api-key, newsapi-key, ai-assistant/api-keys,
     autonomous-ai/keys) - $0.40 each a month. Provider keys have lived in SSM (/justhodl/...) since Sep 2026; no code in
     the repo names any of these secrets (the "fred-api-key" hits are the SSM parameter /justhodl/fred-api-key). Each is
     scheduled for deletion with the longest recovery window (30 days; RestoreSecret brings it back unchanged) only if
     (1) it was not read in the last 60 days and (2) no Lambda function's environment names it. Billing stops once
     deletion is scheduled.
  B. CloudWatch alarms of retired systems (OpenBB, aiapi, scrapeMacroData, indexToOpenSearch, the old API gateways):
     an alarm is deleted only if the metric it watches reported NO datapoint in the last 30 days (it protects nothing)
     - checked per alarm, never by name alone. Every definition is backed up to the private ops bucket first
     (PutMetricAlarm from the backup restores one).
  C. The two stopped EC2 instances ("web server", "OpenBB Server", stopped 2026-08-01, 8 GB disks): read-only here.
     Terminating deletes their disks for good, and the runner cannot take a backup copy first because the 2026-10-20
     seal denies it ec2:CreateSnapshot*. They stay until Khalid says whether to delete them without a copy.
Nothing else is touched.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
PRIVATE = "justhodl-ops-private-857687956942"
SECRETS = ["economic-data-api-keys", "fred-api-key", "polygon-api-key", "liquidity-agent-keys", "multi-agent-api-keys",
           "alphavantage-api-key", "newsapi-key", "ai-assistant/api-keys", "autonomous-ai/keys"]
ALARM_PREFIXES = ("OpenBB-", "enhanced-openbb-handler-", "indexToOpenSearch-", "scrapeMacroData-", "aiapi-lambda-",
                  "EnhancedAPI-", "FinancialDashboardAPI-", "JustHodl-Gateway-", "JustHodl-High-Errors")
INSTANCES = {"i-0a38d50ce030fed84": "web server", "i-0236308c6be77cee8": "OpenBB Server"}
SILENT_DAYS = 30
UNREAD_DAYS = 60


def is_candidate(name):
    return name.startswith(ALARM_PREFIXES) and not name.startswith(("justhodl-", "jh-"))


def selftest():
    assert is_candidate("OpenBB-High-Latency") and is_candidate("scrapeMacroData-Errors") and is_candidate("JustHodl-Gateway-Errors")
    assert not is_candidate("justhodl-runaway-concurrency") and not is_candidate("jh-owned-coder-has-backlog")
    assert not is_candidate("justhodl-logs-ingestion-2gb-hour") and not is_candidate("JustHodl-Monthly")
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from ops_report import report

    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    with report("ops_6630_small_cleanup") as r:
        r.heading("ops 6630 - small cleanup: unused secrets, retired alarms (stopped servers held)")
        selftest()
        failures = 0

        r.section("A. Secrets Manager")
        sm = boto3.client("secretsmanager", region_name=REGION, config=cfg)
        lam = boto3.client("lambda", region_name=REGION, config=cfg)
        env_blob = []
        for page in lam.get_paginator("list_functions").paginate():
            for f in page.get("Functions", []):
                for v in ((f.get("Environment") or {}).get("Variables") or {}).values():
                    env_blob.append(str(v))
        env_text = "\n".join(env_blob)
        r.log("scanned the environment of every Lambda function (%d values)" % len(env_blob))
        scheduled = []
        for name in SECRETS:
            try:
                d = sm.describe_secret(SecretId=name)
            except ClientError as ex:
                r.log("%s: %s (skipped)" % (name, ex.response["Error"]["Code"]))
                continue
            if d.get("DeletedDate"):
                r.log("%s: already scheduled for deletion (%s)" % (name, d["DeletedDate"]))
                continue
            last = d.get("LastAccessedDate")
            recent = bool(last and last > now - timedelta(days=UNREAD_DAYS))
            in_env = name in env_text or d["ARN"] in env_text
            if recent or in_env:
                r.warn("%s kept: last read %s, named in a Lambda environment: %s" % (name, last, in_env))
                continue
            try:
                res = sm.delete_secret(SecretId=name, RecoveryWindowInDays=30)
                scheduled.append((name, str(res.get("DeletionDate"))[:10]))
                r.ok("%s scheduled for deletion on %s (last read %s; RestoreSecret undoes it until then)" % (name, str(res.get("DeletionDate"))[:10], str(last)[:10]))
            except ClientError as ex:
                failures += 1
                r.warn("%s: delete refused: %s" % (name, ex.response["Error"]["Code"]))

        r.section("B. alarms of retired systems")
        cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
        alarms = []
        for page in cw.get_paginator("describe_alarms").paginate(AlarmTypes=["MetricAlarm"]):
            alarms += [a for a in page.get("MetricAlarms", []) if is_candidate(a["AlarmName"])]
        r.log("candidates by name: %d" % len(alarms))
        retired, kept = [], []
        for a in alarms:
            if a.get("Metrics") or not a.get("MetricName"):
                kept.append((a["AlarmName"], "math/insights alarm - not judged here"))
                continue
            st = cw.get_metric_statistics(Namespace=a["Namespace"], MetricName=a["MetricName"], Dimensions=a.get("Dimensions", []),
                                          StartTime=now - timedelta(days=SILENT_DAYS), EndTime=now, Period=86400,
                                          Statistics=["SampleCount"])
            points = sum(p.get("SampleCount", 0) for p in st.get("Datapoints", []))
            dims = ",".join("%s=%s" % (x["Name"], x["Value"]) for x in a.get("Dimensions", []))
            if points == 0:
                retired.append(a)
                r.log("retired: %s (%s %s %s) - no datapoint in %d days, state %s" % (a["AlarmName"], a["Namespace"], a["MetricName"], dims, SILENT_DAYS, a.get("StateValue")))
            else:
                kept.append((a["AlarmName"], "%d datapoints in %d days" % (points, SILENT_DAYS)))
        for name, why in kept:
            r.log("kept: %s - %s" % (name, why))
        if retired:
            bkey = "backups/alarms/retired-systems-%s.json" % stamp
            s3.put_object(Bucket=PRIVATE, Key=bkey, Body=json.dumps(retired, default=str).encode(), ContentType="application/json",
                          ServerSideEncryption="AES256")
            r.ok("backup of %d definitions: s3://%s/%s" % (len(retired), PRIVATE, bkey))
            names = [a["AlarmName"] for a in retired]
            try:
                for i in range(0, len(names), 100):
                    cw.delete_alarms(AlarmNames=names[i:i + 100])
                left = cw.describe_alarms(AlarmNames=names).get("MetricAlarms", [])
                if left:
                    failures += 1
                    r.warn("still present after delete: %s" % [a["AlarmName"] for a in left])
                else:
                    r.ok("deleted %d alarms of retired systems (~$%.2f/month)" % (len(names), 0.10 * len(names)))
            except ClientError as ex:
                failures += 1
                r.warn("delete_alarms refused: %s" % ex.response["Error"]["Code"])

        r.section("C. the two stopped servers (read-only)")
        ec2 = boto3.client("ec2", region_name=REGION, config=cfg)
        res = ec2.describe_instances(InstanceIds=list(INSTANCES))
        for rs in res.get("Reservations", []):
            for i in rs.get("Instances", []):
                vols = [(m["Ebs"]["VolumeId"], m["Ebs"].get("DeleteOnTermination")) for m in i.get("BlockDeviceMappings", []) if "Ebs" in m]
                r.log("%s (%s): %s since %s; disks %s" % (i["InstanceId"], INSTANCES.get(i["InstanceId"]), i["State"]["Name"],
                                                         i.get("StateTransitionReason"), vols))
        eips = ec2.describe_addresses(Filters=[{"Name": "instance-id", "Values": list(INSTANCES)}]).get("Addresses", [])
        r.log("Elastic IPs attached to them: %d" % len(eips))
        r.log("held: terminating deletes the disks for good and the runner may not snapshot them (seal of 2026-10-20) - waiting for Khalid")

        r.kv(secrets_scheduled=len(scheduled), alarms_deleted=len(retired), alarms_kept=len(kept), servers_held=len(INSTANCES), failures=failures)
        if failures:
            r.fail("%d step(s) refused" % failures)
            sys.exit(1)
        r.ok("cleanup done; secrets recoverable for 30 days, alarm definitions backed up")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
