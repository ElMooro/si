import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def rd(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
# cycle-clock
print("=== CYCLE-CLOCK fusion ===")
r=lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
cc=rd("data/cycle-clock.json")
st=cc.get("state") or cc
syn=cc.get("synthesis") or {}
print("capital_cycle_phases:",st.get("capital_cycle_phases"))
print("scarcity_building:",st.get("capital_cycle_scarcity_building"),"| flooding:",st.get("capital_cycle_flooding"))
print("bottleneck_early_calls:",st.get("bottleneck_early_calls"))
print("commodity_cure_setups:",st.get("commodity_cure_setups"))
bd=[d.get("label") for d in (syn.get("bullish_drivers") or [])]+[d.get("label") for d in (syn.get("bearish_drivers") or [])]
print("drivers shown:",bd)
print("capital-cycle contributor in drivers:",[x for x in bd if "apital" in str(x) or "looding" in str(x) or "cure" in str(x).lower()])
print("n_risk_on:",syn.get("n_risk_on"),"| n_risk_off:",syn.get("n_risk_off"))
# best-setups
print("\n=== BEST-SETUPS fusion ===")
r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
bs=rd("data/best-setups.json")
book=bs.get("setups") or bs.get("book") or bs.get("ranks") or []
nb=sum(1 for s in book if any(g.get("key","").startswith("BOTTLENECK") or g.get("key")=="CAPITAL_CYCLE_EARLY" for g in (s.get("signals") or [])))
print("setups total:",len(book),"| setups carrying a bottleneck/capital-cycle signal:",nb)
for s in book:
    keys=[g.get("key") for g in (s.get("signals") or [])]
    if any(k in ("BOTTLENECK_BOOM","CAPITAL_CYCLE_EARLY") for k in keys):
        print("  ",s.get("ticker"),s.get("verdict"),"conv=",s.get("conviction") or s.get("score"),"sigs=",keys[:6])
print("DONE 2454")
