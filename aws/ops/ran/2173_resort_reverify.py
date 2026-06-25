import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def wait(fn):
    for _ in range(40):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
# re-run both engines (they re-sort their buffers)
for fn in ["justhodl-ma200-reclaim","justhodl-crypto-ma200"]:
    wait(fn); lam.invoke(FunctionName=fn,InvocationType="RequestResponse"); print("ran",fn)
# verify both buffers now sorted
for key,lbl in [("data/_ma200/closes.json","equity"),("data/_ma200/crypto-closes.json","crypto")]:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
    d=b["dates"]; print(f"{lbl} buffer: {len(d)} days, sorted={d==sorted(d)}, range {min(d)}..{max(d)}")
# equity books sanity (dist signs correct)
eq=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ma200-reclaim.json")["Body"].read())
print("\nEQUITY counts:",json.dumps(eq.get("counts",{})))
print("  fresh_above all +dist:", all(r["dist_pct"]>0 for r in eq.get("fresh_breakouts_above",[])))
print("  fresh_below all -dist:", all(r["dist_pct"]<0 for r in eq.get("fresh_breakdowns_below",[])))
for r in eq.get("retest_held",[])[:5]: print(f"   HELD {r['ticker']:<6} {r['dist_pct']:+}% slope {r.get('ma200_slope_pct')}% age={r.get('retest_age')}")
# crypto math re-validate (index-based on now-sorted buffer)
cb=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_ma200/crypto-closes.json")["Body"].read())
dts=cb["dates"]; ser=cb["series"]; btc=ser.get("BTC")
print("\nCRYPTO excess-vs-BTC (now-sorted, 21/63d real bars):")
for tk in ["ETH","SOL","XRP"]:
    s=ser.get(tk)
    if not s: continue
    o=[]
    for w in (21,63):
        i0=len(dts)-1-w
        if i0>=0 and s[i0] and s[-1] and btc[i0] and btc[-1]:
            ex=((s[-1]/s[i0]-1)-(btc[-1]/btc[i0]-1))*100
            o.append(f"{w}d excess {ex:+.1f}%")
    print(f"   {tk}: "+" | ".join(o))
# re-run crypto-scorecard
wait("justhodl-crypto-scorecard"); lam.invoke(FunctionName="justhodl-crypto-scorecard",InvocationType="RequestResponse")
sc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-scorecard.json")["Body"].read())
print("\ncrypto-scorecard: signals",sc.get("n_signals"),"graded",sc.get("n_graded_primary"),"pending",sc.get("n_pending"))
print("DONE 2173")
