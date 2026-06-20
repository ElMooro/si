"""ops 2040: confirm outcome-row date field + excess availability + proven-set contents for strategy-portfolio build."""
import json, boto3
from collections import defaultdict
s3=boto3.client("s3","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1"); B="justhodl-dashboard-live"
print("="*60);print("engine-alpha.json — proven set + structure");print("="*60)
try:
    a=json.loads(s3.get_object(Bucket=B,Key="data/engine-alpha.json")["Body"].read())
    print("top keys:",list(a.keys())[:12])
    engs=a.get("engines") or a.get("alpha") or a.get("leaderboard") or []
    if isinstance(engs,dict): engs=list(engs.values())
    print("n engines:",len(engs))
    # show proven + top positive
    def g(e,k,*alt):
        for kk in (k,)+alt:
            if kk in e: return e[kk]
        return None
    rows=[]
    for e in engs:
        rows.append((g(e,"signal_type","engine","name"),g(e,"alpha_status"),g(e,"alpha_n","n"),
                     g(e,"net_alpha_excess_pct","alpha_mean_excess_pct_net","net_mean_excess"),
                     g(e,"alpha_mean_excess_pct","mean_excess")))
    proven=[r for r in rows if r[1]=="ALPHA_PROVEN"]
    print("\nALPHA_PROVEN:",len(proven))
    for r in proven: print("  ",r)
    pos=sorted([r for r in rows if r[4] is not None and r[4]>0 and (r[2] or 0)>=20],key=lambda x:-(x[4] or 0))[:15]
    print("\nTop positive-mean-excess (n>=20), candidate set:")
    for r in pos: print("  ",r)
except Exception as e: print("engine-alpha read err:",str(e)[:160])

print("\n"+"="*60);print("justhodl-outcomes — sample eng:* rows (date field? excess?)");print("="*60)
t=ddb.Table("justhodl-outcomes")
scanned=0; samples=[]; per_eng=defaultdict(int); date_fields=set()
kw={"Limit":400}
for _ in range(6):
    r=t.scan(**kw); items=r.get("Items",[])
    for o in items:
        st=str(o.get("signal_type") or "")
        if st.startswith("eng:"):
            per_eng[st]+=1
            if len(samples)<3: samples.append(o)
    scanned+=len(items)
    if "LastEvaluatedKey" not in r or scanned>2400: break
    kw["ExclusiveStartKey"]=r["LastEvaluatedKey"]
print("scanned:",scanned,"| distinct eng: types seen:",len(per_eng))
for s in samples:
    print("\n  SAMPLE eng row keys:",sorted(s.keys()))
    print("   signal_type:",s.get("signal_type"),"| dates:",{k:str(s.get(k)) for k in s.keys() if "date" in k.lower() or "time" in k.lower() or k in ("created_at","ts","logged_at")})
    print("   excess_return:",s.get("excess_return"),"| price_at_signal:",s.get("price_at_signal"),"price_at_check:",s.get("price_at_check"),"| predicted_dir:",s.get("predicted_dir") or s.get("predicted_direction"),"| regime_at_log:",s.get("regime_at_log"))
print("\n top eng counts (in sample):",sorted(per_eng.items(),key=lambda x:-x[1])[:12])
print("DONE 2040")
