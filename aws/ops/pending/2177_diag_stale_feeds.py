import boto3, json
from datetime import datetime, timezone
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
checks=[("justhodl-pairs-scanner","data/pairs-scanner.json"),
        ("justhodl-future-intelligence","data/buzz-velocity.json"),
        ("justhodl-divergence-engine-v2","data/divergence-v2.json"),
        ("justhodl-divergence-scanner","data/report.json"),
        ("justhodl-breadth-divergence","data/breadth-divergence.json")]
now=datetime.now(timezone.utc)
for fn,key in checks:
    print(f"\n=== {fn} ===")
    # output staleness
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        ga=d.get("generated_at") or d.get("timestamp") or d.get("as_of") or d.get("updated_at")
        if ga:
            age=(now-datetime.fromisoformat(ga.replace("Z","+00:00"))).total_seconds()/3600
            print(f"  output {key}: generated_at={ga[:19]} age={age:.0f}h")
        else:
            print(f"  output {key}: no timestamp field; keys={list(d.keys())[:8]}")
    except Exception as e: print(f"  output {key}: {str(e)[:45]}")
    # function exists?
    try:
        arn=lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
    except Exception as e:
        print(f"  function: MISSING {str(e)[:40]}"); continue
    # rules targeting it
    try:
        rn=ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[])
        if not rn: print("  schedule: NO RULE targets this function (dormant)")
        for r in rn:
            d=ev.describe_rule(Name=r)
            print(f"  rule {r}: {d.get('ScheduleExpression') or d.get('EventPattern','(pattern)')[:30]} state={d.get('State')}")
    except Exception as e: print(f"  rules: {str(e)[:45]}")
print("\nDONE 2177")
