"""ops 2046: enumerate ALL data/*.json feeds + detect each one's headline field (for the Strategist assembler)."""
import json, boto3
from collections import Counter
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
# list all top-level data/*.json
keys=[]
tok=None
while True:
    kw={"Bucket":B,"Prefix":"data/","Delimiter":"/"}
    if tok: kw["ContinuationToken"]=tok
    r=s3.list_objects_v2(**kw)
    for o in r.get("Contents",[]):
        k=o["Key"]
        if k.endswith(".json") and k.count("/")==1: keys.append((k,o["LastModified"]))
    tok=r.get("NextContinuationToken")
    if not tok: break
print("TOTAL top-level data/*.json feeds:",len(keys))
# headline field detection: common keys carrying the read
HEAD=["regime","posture","signal","verdict","score","composite_score","composite_signal","stress_regime",
      "risk_regime_score","bias","direction","status","level","defcon_name","state","label","reading","call",
      "overall","net","gauge","z","z_score","percentile"]
field_hits=Counter(); has_picks=0; has_score=0; samples=[]
import random
random.seed(2)
sample_keys=[k for k,_ in keys]
for k in sample_keys[:120]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        if not isinstance(d,dict): continue
        present=[h for h in HEAD if h in d]
        for h in present: field_hits[h]+=1
        if any(p in d for p in ("top_picks","picks","top_setups")): has_picks+=1
        if "score" in d or "composite_score" in d: has_score+=1
        if len(samples)<8 and present:
            samples.append((k.replace("data/",""),{h:str(d.get(h))[:30] for h in present[:4]}))
    except Exception: pass
print("\nHEADLINE FIELD FREQUENCY (top, across 120 feeds):")
for f,c in field_hits.most_common(16): print(f"  {f:<20} {c}")
print("\nfeeds w/ top_picks:",has_picks,"| feeds w/ score:",has_score)
print("\nSAMPLE headline extractions:")
for nm,fields in samples: print(f"  {nm:<28} {fields}")
print("DONE 2046")
