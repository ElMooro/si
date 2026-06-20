"""1960 — verify earnings-tracker now emits Benzinga authoritative surprises."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
BUCKET="justhodl-dashboard-live"; KEY="data/earnings-tracker.json"
print("invoking justhodl-earnings-tracker (sync)...")
r=lam.invoke(FunctionName="justhodl-earnings-tracker", InvocationType="RequestResponse")
print("StatusCode:", r.get("StatusCode"), "FunctionError:", r.get("FunctionError"))
print("payload:", r["Payload"].read()[:200])
time.sleep(3)
j=json.loads(s3.get_object(Bucket=BUCKET,Key=KEY)["Body"].read())
recent = j.get("recent_results") or j.get("recent") or []
print(f"\nrecent_results: {len(recent)}")
bz=[x for x in recent if x.get("surprise_source")=="benzinga"]
qoq=[x for x in recent if x.get("surprise_source")=="polygon_qoq_proxy"]
print(f"  benzinga-sourced: {len(bz)}  | qoq-proxy-fallback: {len(qoq)}")
print("\n  sample Benzinga-sourced PEAD records (ticker | surprise% | rev% | imp | 1d | label | score):")
for x in sorted(bz, key=lambda z: abs(z.get("eps_surprise_pct") or 0), reverse=True)[:8]:
    rr=x.get("returns",{})
    print(f"   {x.get('ticker'):<6} eps={x.get('eps_surprise_pct')!s:<8} rev={x.get('revenue_surprise_pct')!s:<8} imp={x.get('importance')} 1d={rr.get('1d')!s:<6} {x.get('pead_label'):<24} {x.get('pead_score')}")
# show top-level keys for schema
print("\n  top-level keys:", list(j.keys())[:12])
print("DONE 1960")
