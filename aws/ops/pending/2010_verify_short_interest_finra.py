"""ops 2010: verify justhodl-short-interest now uses FINRA SI (real DTC/change), squeeze signals fire."""
import boto3, json, time
REGION="us-east-1"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); s3=boto3.client("s3",REGION)

print("invoking justhodl-short-interest (FINRA SI pull, ~30s)…")
r=lam.invoke(FunctionName="justhodl-short-interest",InvocationType="RequestResponse")
print(" status:",r["StatusCode"]," body:",r["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/short-interest.json")["Body"].read())
print("\ndata_sources.short_interest:",d.get("data_sources",{}).get("short_interest"))
print("n_tickers_short_interest:",d.get("n_tickers_short_interest"),"| n_tickers_finra(vol):",d.get("n_tickers_finra"),"| n_with_data:",d.get("n_tickers_with_data"))
bt=d.get("by_ticker",{})
# show watchlist names that now carry REAL FINRA SI (settlement_date + days_to_cover + si_change_pct)
real=[v for v in bt.values() if v.get("settlement_date") and v.get("days_to_cover") is not None and v.get("short_interest")]
print(f"\nnames with REAL official SI (settlement+DTC+shares): {len(real)}")
real.sort(key=lambda x:-(x.get('days_to_cover') or 0))
for v in real[:12]:
    print(f"  {v['ticker']:<6} SI={v.get('short_interest'):>12,} DTC={v.get('days_to_cover')} chg={v.get('si_change_pct')}% settle={v.get('settlement_date')} sig={v.get('signal')}")
print("\nsettlement dates present:",sorted({v.get('settlement_date') for v in real if v.get('settlement_date')})[-3:])
print("top_squeeze_risk:",[(v['ticker'],v.get('days_to_cover'),v.get('trend_pct')) for v in d.get('top_squeeze_risk',[])[:6]])
print("top_high_dtc:",[(v['ticker'],v.get('days_to_cover')) for v in d.get('top_high_dtc',[])[:6]])
# AAPL spot-check vs known FINRA value (155.9M, DTC 3.38, settle 2026-05-29)
a=bt.get("AAPL",{})
print("\nAAPL:",{k:a.get(k) for k in ('short_interest','days_to_cover','si_change_pct','settlement_date','signal')})
print("DONE 2010")
