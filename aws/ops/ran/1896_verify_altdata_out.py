import boto3, json, time
from botocore.config import Config
B="justhodl-dashboard-live"
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=310,connect_timeout=10,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for FN in ["justhodl-finnhub-signals","justhodl-gdelt-buzz","justhodl-stocktwits"]:
    try:
        r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print(FN,"->",r["Payload"].read().decode()[:150])
    except Exception as e:
        print(FN,"invoke err:",str(e)[:80])
time.sleep(2)
def show(key):
    try: return json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
fh=show("data/finnhub-signals.json")
print("\nFINNHUB names=%s"%fh.get("n_names"))
for r in (fh.get("summary",{}) or {}).get("top_accumulation",[])[:8]:
    print("   %-6s acc=%-6s mspr=%-6s rec_mom=%-6s surp=%-5s | %s"%(r.get("symbol"),r.get("accumulation_score"),r.get("mspr"),r.get("rec_momentum"),r.get("last_surprise_pct"),(r.get("why") or "")[:38]))
print("   insider-buying:",[(r["symbol"],r["mspr"]) for r in (fh.get("summary",{}) or {}).get("top_insider_buying",[])][:6])
gd=show("data/gdelt-buzz.json")
print("\nGDELT themes:",[(t["theme"][:24],t.get("accel_pct"),t.get("status")) for t in gd.get("themes",[])][:10])
st=show("data/stocktwits.json")
print("\nSTOCKTWITS trending:",st.get("trending_equities",[])[:12])
print("   bullish buzz:",[(b["symbol"],b["bull_pct"],b["n_msgs"]) for b in st.get("top_bullish_buzz",[])][:6])
