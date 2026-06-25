import boto3, json
s3=boto3.client("s3","us-east-1")
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
ts=b.get("top_setups") or []
tagged=[s for s in ts if s.get("cycle_phase")]
warned=[s for s in ts if s.get("cycle_warning")]
print(f"top_setups={len(ts)} cycle-tagged={len(tagged)} cycle_warning={len(warned)}")
for s in tagged[:8]:
    print(f"  {s['ticker']:<6} {s['verdict']:<14} conv {s['conviction']} | phase {s['cycle_phase']} flag {s.get('cycle_flag')} warn {s.get('cycle_warning')}")
if not tagged:
    print("  (none of the current top setups are in an accumulation-radar extreme book — expected when setups are mid-cycle names)")
    print("  sample setup keys:", list((ts[0].keys()) if ts else []))
print("DONE 2190")
