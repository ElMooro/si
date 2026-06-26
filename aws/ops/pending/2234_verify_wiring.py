import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(28):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
# ---- master-ranker ----
ready("justhodl-master-ranker")
lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse")
mr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
tt=mr.get("top_tickers") or []
sr_hits=[t for t in tt if "scarcity_radar" in (t.get("systems") or []) or "scarcity_radar" in str(t.get("rationale",""))]
print("MASTER-RANKER: top_tickers",len(tt),"| names carrying scarcity_radar:",len(sr_hits))
for t in sr_hits[:5]:
    print(f"   #{t.get('rank')} {t.get('ticker')} score={t.get('score')} n_sys={t.get('n_systems')} :: {str(t.get('rationale'))[:90]}")
# also confirm the feed loaded by scanning any ticker's systems for scarcity_radar
allsys=set()
for t in tt:
    for s in (t.get("systems") or []): allsys.add(s)
print("   scarcity_radar present in any top_ticker systems:", "scarcity_radar" in allsys)
# ---- conviction-engine ----
ready("justhodl-conviction-engine")
lam.invoke(FunctionName="justhodl-conviction-engine",InvocationType="RequestResponse")
cv=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/conviction-engine.json")["Body"].read())
setups=cv.get("setups") or []
sc_setup=[s for s in setups if "shortage" in str(s.get("subject","")).lower() or "scarcity" in str(s.get("subject","")).lower()]
print("\nCONVICTION: setups",len(setups),"| scarcity subject present:",bool(sc_setup))
for s in sc_setup: print(f"   subject='{s.get('subject')}' dir={s.get('direction')} read={str(s.get('rationale') or s.get('read'))[:80]}")
sn=cv.get("single_names") or []
sh=[n for n in sn if str(n.get("verdict","")).startswith("SHORTAGE") or n.get("source")=="scarcity-radar"]
print("   single_names",len(sn),"| from scarcity-radar:",[(n.get('ticker'),n.get('verdict'),n.get('score')) for n in sh])
print("DONE 2234")
