import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(28):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
ready("justhodl-supply-inflection-scanner")
lam.invoke(FunctionName="justhodl-supply-inflection-scanner",InvocationType="RequestResponse")
si=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
sig=si.get("signals") or {}
new=["COPPER_SPOT","URANIUM_SPOT","ALUMINUM_SPOT","NICKEL_SPOT","IRON_ORE_SPOT","PPI_SEMIS","PPI_GRID_EQUIPMENT","DELIVERY_TIME_NY","PRICES_PAID_PHILLY","PRICES_PAID_NY"]
print("=== NEW TRUE-LEADING SIGNALS (score, flag) ===")
for n in new:
    s=sig.get(n) or {}
    sc=s.get("score"); fl=s.get("flag"); print(f"  {n:<20} score={sc} flag={fl}")
summ=si.get("summary") or {}
print("\nsummary: scored",summ.get("n_signals_scored"),"strong_tighten",summ.get("n_strong_tightening"),"tighten",summ.get("n_tightening"))
print("top tightening:", [(t.get("name"),t.get("score"),t.get("flag")) for t in (summ.get("top_signals") or [])[:8]])
# now re-run scarcity-radar with upgraded inputs
ready("justhodl-scarcity-radar")
lam.invoke(FunctionName="justhodl-scarcity-radar",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/scarcity-radar.json")["Body"].read())
print("\n=== SCARCITY-RADAR after upgrade: vertical tightness ===")
for v in (d.get("vertical_tightness") or [])[:8]:
    print(f"  {v['theme']:<5} tight {v['tightness']:<5} phase={v.get('phase')} signals={v.get('top_signals')}")
print("headline:", d.get("counts",{}).get("headline"))
for b in (d.get("headline_board") or [])[:8]:
    print(f"  {b['ticker']:<6} comp {b['composite']} scar {b['scarcity']} stlth {b['stealth']} vert={b.get('vertical')} phase={b.get('theme_phase')}")
print("DONE 2229")
