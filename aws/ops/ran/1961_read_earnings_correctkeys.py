import boto3, json
s3=boto3.client("s3","us-east-1")
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/earnings-tracker.json")["Body"].read())
rec=j.get("recent_results_30d") or []
pead=j.get("pead_signals") or []
print("n_recent:",len(rec)," n_pead:",len(pead)," generated:",j.get("generated_at"))
print("data_sources:", j.get("data_sources"))
bz=[x for x in rec if x.get("surprise_source")=="benzinga"]
qoq=[x for x in rec if x.get("surprise_source")=="polygon_qoq_proxy"]
print(f"benzinga-sourced={len(bz)}  qoq-proxy={len(qoq)}  other={len(rec)-len(bz)-len(qoq)}")
print("\nrecent w/ surprise (ticker|date|eps_surprise%|rev%|imp|1d|label|score|src):")
for x in sorted(rec,key=lambda z:abs(z.get('eps_surprise_pct') or 0),reverse=True)[:10]:
    rr=x.get("returns",{})
    print(f"  {x.get('ticker'):<6}{str(x.get('filing_date')):<12} eps={x.get('eps_surprise_pct')!s:<8} rev={x.get('revenue_surprise_pct')!s:<7} imp={x.get('importance')!s:<3} 1d={rr.get('1d')!s:<6}{str(x.get('pead_label')):<24}{x.get('pead_score')!s:<4}{x.get('surprise_source')}")
print("\nsample pead_signals[0]:", json.dumps(pead[0]) if pead else "none")
print("DONE 1961")
