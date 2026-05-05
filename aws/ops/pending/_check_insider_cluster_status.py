"""
Check whether justhodl-insider-cluster-scanner was actually created and what state it's in.
Quick — no SEC calls.
"""
import json, os, time
import boto3

REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

L = boto3.client("lambda", region_name="us-east-1")
EB = boto3.client("events", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
LOGS = boto3.client("logs", region_name="us-east-1")

def main():
    section("1) Lambda existence + state")
    try:
        c = L.get_function(FunctionName="justhodl-insider-cluster-scanner")
        cfg = c["Configuration"]
        log(f"  ✓ exists, state={cfg['State']}, mod={cfg['LastModified']}")
        log(f"  mem={cfg['MemorySize']}MB timeout={cfg['Timeout']}s")
        log(f"  handler={cfg['Handler']}")
    except L.exceptions.ResourceNotFoundException:
        log(f"  ❌ NOT FOUND — ship script never created Lambda")

    section("2) EventBridge schedule")
    try:
        rules = EB.list_rule_names_by_target(
            TargetArn="arn:aws:lambda:us-east-1:857687956942:function:justhodl-insider-cluster-scanner")
        for rn in rules.get("RuleNames", []):
            r = EB.describe_rule(Name=rn)
            log(f"  rule: {rn}  expr={r.get('ScheduleExpression')}  state={r.get('State')}")
        if not rules.get("RuleNames"):
            log(f"  ⚠ no schedule attached")
    except Exception as e:
        log(f"  ❌ {e}")

    section("3) Last CloudWatch logs (any execution attempts?)")
    try:
        streams = LOGS.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-insider-cluster-scanner",
            orderBy="LastEventTime", descending=True, limit=2)
        for s in streams.get("logStreams", []):
            log(f"  stream: {s['logStreamName']}  last_event: {s.get('lastEventTimestamp', '?')}")
            events = LOGS.get_log_events(
                logGroupName="/aws/lambda/justhodl-insider-cluster-scanner",
                logStreamName=s["logStreamName"], limit=80, startFromHead=True)
            for e in events.get("events", [])[-30:]:
                log(f"    {e['message'].rstrip()[:200]}")
            log("")
    except Exception as e:
        log(f"  no logs yet: {e}")

    section("4) S3 output check")
    try:
        head = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        log(f"  ✓ {head['ContentLength']:,}b modified {head['LastModified']}")
    except Exception as e:
        log(f"  no S3 output yet: {type(e).__name__}")

if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "check_insider_cluster_status.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
