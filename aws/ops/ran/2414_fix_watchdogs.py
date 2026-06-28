import boto3, json
ev=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1")
ACCT="857687956942"; REGION="us-east-1"
def ensure(fn, rule, rate, desc):
    farn="arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,fn)
    r=ev.put_rule(Name=rule, ScheduleExpression=rate, State="ENABLED", Description=desc)
    rarn=r["RuleArn"]
    ev.put_targets(Rule=rule, Targets=[{"Id":"1","Arn":farn}])
    # idempotent invoke permission for EventBridge
    try:
        lam.add_permission(FunctionName=fn, StatementId="eb-"+rule[:50], Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rarn)
        perm="added"
    except lam.exceptions.ResourceConflictException:
        perm="already-present"
    except Exception as e:
        perm="err:"+str(e)[:50]
    # verify
    d=ev.describe_rule(Name=rule)
    print("  %s: rule=%s %s | targets=%s | permission=%s"%(fn, d.get("State"), d.get("ScheduleExpression"),
          [t["Arn"].split(":")[-1] for t in ev.list_targets_by_rule(Rule=rule).get("Targets",[])], perm))

print("=== restoring watchdog schedules ===")
ensure("justhodl-fleet-error-monitor","fleet-error-monitor-5min","rate(5 minutes)","AUDIT — scan fleet error rates + DLQ depth")
ensure("justhodl-fleet-freshness-monitor","fleet-freshness-monitor-30min","rate(30 minutes)","AUDIT — detect stale/missing S3 outputs (silent Lambda failures)")
# the freshness-monitor also still has the old daily rule (cron 12:12); leave it (harmless extra trigger) OR note it
print("DONE 2414")
