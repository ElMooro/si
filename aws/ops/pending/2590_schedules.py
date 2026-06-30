"""ops 2590 — verify/create EventBridge daily schedules for the two new attention engines."""
import boto3, json
REGION="us-east-1"; ACCT="857687956942"
ev=boto3.client("events",region_name=REGION); lam=boto3.client("lambda",region_name=REGION)
WANT=[("justhodl-search-attention","justhodl-search-attention-daily","cron(0 15 * * ? *)"),
      ("justhodl-attention-confluence","justhodl-attention-confluence-daily","cron(10 15 * * ? *)")]
def rule_exists(name):
    try: ev.describe_rule(Name=name); return True
    except ev.exceptions.ResourceNotFoundException: return False
for fn,rule,sched in WANT:
    arn=f"arn:aws:lambda:{REGION}:{ACCT}:function:{fn}"
    if rule_exists(rule):
        r=ev.describe_rule(Name=rule)
        tgs=ev.list_targets_by_rule(Rule=rule).get("Targets",[])
        print(f"  ✓ {rule} EXISTS: {r.get('ScheduleExpression')} state={r.get('State')} targets={len(tgs)}")
        continue
    # create rule
    ev.put_rule(Name=rule, ScheduleExpression=sched, State="ENABLED",
                Description=f"Daily trigger for {fn}")
    # permission for events to invoke lambda (idempotent-ish)
    try:
        lam.add_permission(FunctionName=fn, StatementId=f"{rule}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{rule}")
    except lam.exceptions.ResourceConflictException: pass
    ev.put_targets(Rule=rule, Targets=[{"Id":"1","Arn":arn}])
    print(f"  ＋ CREATED {rule}: {sched} -> {fn}")
# final confirm
print("\nfinal state:")
for fn,rule,sched in WANT:
    r=ev.describe_rule(Name=rule); tgs=ev.list_targets_by_rule(Rule=rule).get("Targets",[])
    print(f"  {rule}: {r.get('ScheduleExpression')} {r.get('State')} targets={len(tgs)} -> {tgs[0]['Arn'].split(':')[-1] if tgs else 'NONE'}")
print("DONE 2590")
