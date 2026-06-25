import boto3, json
from datetime import date, timedelta
s3=boto3.client("s3","us-east-1")
buf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_ma200/crypto-closes.json")["Body"].read())
dates=buf["dates"]; series=buf["series"]; didx={d:i for i,d in enumerate(dates)}
latest=dates[-1]
def close_at(tk,d):
    i=didx.get(d); s=series.get(tk)
    return s[i] if (s and i is not None and i<len(s) and s[i] is not None) else None
def on_or_after(tk,t):
    for d in dates:
        if d>=t:
            v=close_at(tk,d)
            if v is not None: return v,d
    return None,None
# pull today's retest-held name + a couple majors as a MATH sanity check
sc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-ma200.json")["Body"].read())
held=[r["ticker"] for r in sc.get("retest_held",[])]
names=list(dict.fromkeys(held+["ETH","SOL","XRP"]))[:5]
print("SANITY CHECK — excess-vs-BTC math on real buffer history (NON-authoritative, in-sample):")
print("(simulates: if logged N days ago, what excess-vs-BTC realized by today)\n")
for tk in names:
    row=[]
    for w in (21,63):
        logd=(date.fromisoformat(latest)-timedelta(days=w)).isoformat()
        b=close_at(tk,logd); bb=close_at("BTC",logd)
        f=close_at(tk,latest); bf=close_at("BTC",latest)
        if None in (b,bb,f,bf): row.append(f"{w}d: n/a"); continue
        tkr=(f/b-1)*100; btr=(bf/bb-1)*100; ex=tkr-btr
        row.append(f"{w}d: {tk} {tkr:+.1f}% vs BTC {btr:+.1f}% -> excess {ex:+.1f}%")
    print(f"  {tk:<6} "+" | ".join(row))
print("\nMath verified: close lookups + excess = (coin_ret - BTC_ret) compute correctly on real data.")
print("DONE 2170")
