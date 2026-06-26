import boto3, json
s3=boto3.client("s3","us-east-1")
for key in ["data/tic-flows.json","data/capital-inflows.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        print(f"\n{key}: generated_at={d.get('generated_at')}")
        print("  top-level keys:", list(d.keys())[:14])
        # heuristics for fakeness
        for k in ("regime","headline","verdict","net_flow","summary","status"):
            if k in d: print(f"  {k}:", json.dumps(d[k])[:160])
    except Exception as e:
        print(f"\n{key}: MISSING/err {str(e)[:50]}")
# is there a schedule for tic-flows?
ev=boto3.client("events","us-east-1")
for r in ev.list_rules(NamePrefix="justhodl-tic")["Rules"]:
    print("\nRULE:",r["Name"],r.get("ScheduleExpression"),r.get("State"))
print("DONE 2236")
