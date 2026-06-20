"""ops 2001: verify index-recon ETF-Global cross-confirm (verify-only, wait active)."""
import boto3, json, time
REGION="us-east-1"; FN="justhodl-index-recon"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); s3=boto3.client("s3",REGION)
for _ in range(36):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(5)
print("state:",c.get("State"),c.get("LastUpdateStatus"),c.get("LastModified"))
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"], r["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/index-recon.json")["Body"].read())
cc=d.get("cross_confirm") or {}
print("\n=== CROSS-CONFIRM ===")
for k in ("n_events_observed","n_additions_confirmed","n_graduations_confirmed","n_deletions_confirmed","n_demotions_confirmed","confirmed_tickers"):
    print(f"  {k}: {cc.get(k)}")
# show a few tagged add records
adds=d.get("russell_2000_additions") or []
print(f"\n  additions: {len(adds)} | sample flow_confirmed flags:")
for r in adds[:6]:
    print(f"    {r.get('symbol'):<6} conf={r.get('flow_confirmed')} etfs={r.get('confirming_etfs')}")
print("DONE 2001")
