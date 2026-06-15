import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
r=lam.invoke(FunctionName="justhodl-finviz-universe", InvocationType="RequestResponse")
print("invoke:", r.get("StatusCode"), "err:", r.get("FunctionError"))
import base64
try: print("payload:", r["Payload"].read().decode()[:300])
except: pass
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read())
print("universe: n_tickers=%s n_with_short_float=%s" % (d.get("n_tickers"), d.get("n_with_short_float")))
bt=d.get("by_ticker",{})
for tk in ("MU","NVDA","GME","AMC","TSLA"):
    r2=bt.get(tk,{})
    print("  %-5s short_float=%s short_ratio=%s float=%s rel_vol=%s rsi=%s perf_m=%s pe=%s" % (
        tk, r2.get("short_float_pct"), r2.get("short_ratio"), r2.get("float_shares"), r2.get("rel_volume"), r2.get("rsi"), r2.get("perf_m"), r2.get("pe")))
sh=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-short.json")["Body"].read())
print("slim short index: %s tickers" % sh.get("n"))
