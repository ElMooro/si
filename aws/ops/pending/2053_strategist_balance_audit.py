"""ops 2053: HONEST balance check — (a) is the risk-ON side of the fleet being read & counted,
or does the read tilt risk-off because crisis engines dominate salience? (b) what did SPY
actually do since Oct 2025 — has 'risk-off since October' been right or a persistent miss?"""
import boto3, json, time, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import importlib.util
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
spec=importlib.util.spec_from_file_location("st","aws/lambdas/justhodl-strategist/source/lambda_function.py")
st=importlib.util.module_from_spec(spec); spec.loader.exec_module(st)

# ---- (a) fleet balance + risk-on representation ----
man=json.loads(s3.get_object(Bucket=B,Key="data/engine-manifest.json")["Body"].read())
feeds={(e.get("keys") or ["?"])[0]:e["engine"].replace("justhodl-","") for e in man.get("engines",[]) if e.get("keys")}
trust={}
try:
    et=json.loads(s3.get_object(Bucket=B,Key="data/engine-trust.json")["Body"].read())
    for e in et.get("engines",[]):
        nm=(e.get("signal_type") or e.get("engine") or "").replace("eng:","").replace("justhodl-","")
        if nm and e.get("effective_trust") is not None: trust[nm]=e["effective_trust"]
except Exception: pass
def probe(kv):
    key,short=kv
    try:
        o=s3.get_object(Bucket=B,Key=key); d=json.loads(o["Body"].read())
        age=(datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600
        if age>=240: return None
        info=st.extract(d)
        if not info: return None
        return {"e":short,"dir":info["direction"],"ext":info["extremity"],"v":str(info["verdict"])[:24]}
    except Exception: return None
items=[x for x in ThreadPoolExecutor(max_workers=24).map(probe,list(feeds.items())) if x]
pos=[i for i in items if i["dir"]>0]; neg=[i for i in items if i["dir"]<0]; neu=[i for i in items if i["dir"]==0]
print(f"FLEET READ: {len(items)} fresh w/ view | +{len(pos)} / -{len(neg)} / ~{len(neu)} neutral")
print(f"  pos engines: {[i['e'] for i in pos]}")
print(f"  neg engines: {[i['e'] for i in neg][:25]}")
# are the RISK-ON / trend / breadth engines being read? what do they say?
WATCH=["momentum","breadth-thrust","market-internals","ath","trend-engine","rotation-radar","smart-beta",
       "sector-tilt","ma-reversion","master-rank","master-ranker","best-setups","signal-board","altseason",
       "boom-radar","capital-flow-radar","upside-radar","activity-nowcast","regime-composite","risk-regime"]
byname={i["e"]:i for i in items}
print("\nRISK-ON / TREND / BREADTH ENGINES — read & direction (MISSING = no extractable view):")
for w in WATCH:
    hit=byname.get(w) or byname.get(w.replace("-","_"))
    print(f"  {w:<20} {'dir '+str(hit['dir'])+'  '+hit['v'] if hit else 'MISSING (NOVIEW/stale)'}")

# ---- (b) SPY actual path since Oct 1 2025 ----
def poly(url):
    req=urllib.request.Request(url,headers={"User-Agent":"jh"})
    return json.loads(urllib.request.urlopen(req,timeout=25).read())
try:
    r=poly(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2025-10-01/2026-06-20?adjusted=true&sort=asc&limit=400&apiKey={POLY}")
    res=r.get("results",[])
    if res:
        o=res[0]["c"]; last=res[-1]["c"]; hi=max(x["c"] for x in res); lo=min(x["c"] for x in res)
        # max drawdown
        peak=res[0]["c"]; mdd=0
        for x in res:
            peak=max(peak,x["c"]); mdd=min(mdd,(x["c"]-peak)/peak)
        d0=datetime.utcfromtimestamp(res[0]["t"]/1000).strftime("%Y-%m-%d")
        d1=datetime.utcfromtimestamp(res[-1]["t"]/1000).strftime("%Y-%m-%d")
        print(f"\nSPY ACTUAL {d0}→{d1}: open {o:.0f} → last {last:.0f} = {(last/o-1)*100:+.1f}% | range {lo:.0f}-{hi:.0f} | maxDD {mdd*100:.1f}%")
        print("VERDICT ON THE READ: "+("risk-off was RIGHT (SPY down)" if last<o else f"SPY ROSE {(last/o-1)*100:+.1f}% — a persistent risk-off lean would have MISSED this melt-up" if (last/o-1)>0.03 else "roughly flat — risk-off neither clearly right nor wrong"))
except Exception as e:
    print("\nSPY fetch err:",str(e)[:80])
print("DONE 2053")
