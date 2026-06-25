import boto3, json
s3=boto3.client("s3","us-east-1")
buf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_ma200/crypto-closes.json")["Body"].read())
dates=buf["dates"]; series=buf["series"]
print("buffer:",len(dates),"days  range",dates[0],"->",dates[-1])
print("BTC in series:","BTC" in series, "| sample keys:",list(series.keys())[:8])
sc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-ma200.json")["Body"].read())
held=[r["ticker"] for r in sc.get("retest_held",[])]
names=list(dict.fromkeys(held+["ETH","SOL","XRP","DOGE"]))[:6]
btc=series.get("BTC")
def ret(s,i0,i1):
    a,b=s[i0],s[i1]
    return (b/a-1)*100 if (a and b) else None
print("\nexcess-vs-BTC math on real buffer bars (index-based, NON-authoritative in-sample):")
for tk in names:
    s=series.get(tk)
    if not s: print(f"  {tk}: not in buffer"); continue
    out=[]
    for w in (21,63):
        i0=len(dates)-1-w; i1=len(dates)-1
        if i0<0 or s[i0] is None or s[i1] is None or btc[i0] is None or btc[i1] is None: out.append(f"{w}d:n/a"); continue
        ex=ret(s,i0,i1)-ret(btc,i0,i1)
        out.append(f"{w}d {tk} {ret(s,i0,i1):+.1f}% vs BTC {ret(btc,i0,i1):+.1f}% = excess {ex:+.1f}%")
    print(f"  {tk:<6} "+" | ".join(out))
print("DONE 2171")
