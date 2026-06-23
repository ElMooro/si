import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def get(k): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
def ready(fn):
    for _ in range(30):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": return
        time.sleep(3)

# 1. chokepoint first (produces structural_names + highest_conviction_book)
ready("justhodl-chokepoint")
t=time.time(); r=lam.invoke(FunctionName="justhodl-chokepoint",InvocationType="RequestResponse")
print("chokepoint:",r["Payload"].read().decode()[:120],f"({time.time()-t:.0f}s)")
d=get("data/chokepoint.json")
print("  structural names exported:",d["stats"].get("structural"),"| highest_conviction:",d["stats"].get("highest_conviction"))
print("  structural_names sample:",list(d.get("structural_names",{}).items())[:6])
hc=d.get("highest_conviction_book",[])
if hc:
    print("  ⭐ HIGHEST CONVICTION:")
    for x in hc[:8]: print(f"     {x['ticker']:<6} crit={x['criticality']} [{x['setup_type']}] {(x.get('name') or '')[:24]}")
else: print("  ⭐ highest_conviction: EMPTY (no structural name at trough/cheap right now)")

# 2. best-setups (consumes structural_names)
ready("justhodl-best-setups")
t=time.time(); r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
print("\nbest-setups:",r["Payload"].read().decode()[:140],f"({time.time()-t:.0f}s)")
b=get("data/best-setups.json")
sc=b.get("structural_chokepoints",[])
print("  setups tagged structural_chokepoint:",len(sc),"| structural_at_trough:",len(b.get("structural_at_trough",[])))
for s in sc[:5]: print(f"     {s['ticker']:<6} conv={s.get('conviction')} crit={s.get('criticality')} {s.get('verdict')}")
ex=next((s for s in sc if s.get("why")),None)
if ex: print("  example why:",ex["why"][:240])

# 3. master-ranker (consumes structural_names)
ready("justhodl-master-ranker")
t=time.time(); r=lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse")
print("\nmaster-ranker:",r["Payload"].read().decode()[:140],f"({time.time()-t:.0f}s)")
m=get("data/master-ranker.json")
print("  n_structural_chokepoints in top_tickers:",m.get("alerts",{}).get("n_structural_chokepoints"))
for t2 in [x for x in m.get("top_tickers",[]) if x.get("structural_chokepoint")][:5]:
    print(f"     {t2['ticker']:<6} score={t2.get('score')} crit={t2.get('criticality')} hiconv={t2.get('highest_conviction',False)}")
    print(f"       rationale: ...{(t2.get('rationale') or '')[-120:]}")
print("DONE 2123")
