import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
j=json.loads(s3.get_object(Bucket=B,Key="etf-flows/daily.json")["Body"].read())
m=j.get("metrics") or []
print("generated_at:",j.get("generated_at"),"universe:",j.get("universe_size"),"n_ok:",j.get("n_ok"),"metrics:",len(m))
if m:
    print("\nper-ETF record fields:", list(m[0].keys()))
    # show a few with flow magnitudes
    def fv(r,k):
        v=r.get(k); return round(v/1e6,1) if isinstance(v,(int,float)) else v
    print("\nticker | daily_flow$M | 5d$M | 21d$M | aum$M | category | subcategory")
    have=[r for r in m if r.get("daily_flow_usd") is not None or r.get("fund_flow_5d_usd") is not None]
    have.sort(key=lambda r: abs(r.get("fund_flow_5d_usd") or 0), reverse=True)
    for r in have[:12]:
        print(f"  {r.get('ticker'):<6} {fv(r,'daily_flow_usd')!s:<8} {fv(r,'fund_flow_5d_usd')!s:<9} {fv(r,'fund_flow_21d_usd')!s:<9} {fv(r,'aum_usd')!s:<10} {r.get('category')}/{r.get('subcategory')}")
    print(f"\n  ETFs with usable 5d flow: {len(have)}/{len(m)}")
print("DONE 1963")
