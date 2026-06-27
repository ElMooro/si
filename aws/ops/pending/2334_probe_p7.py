import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_ERR":str(e)[:50]}
def exists(fn):
    try: lam.get_function(FunctionName=fn); return True
    except Exception: return False

print("=== FEDWATCH ===")
fn="justhodl-fedwatch-rate-probability"
print("lambda exists:", exists(fn))
if exists(fn):
    try:
        lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        time.sleep(3)
        d=gj("data/fedwatch.json")
        print("fedwatch.json:", "ERR "+d["_ERR"] if "_ERR" in d else "keys="+str(list(d.keys())[:18]))
        if "_ERR" not in d:
            for k in ("next_meeting","meetings","implied_path","probabilities","cuts_priced","summary","headline","regime","expected_cuts_2026","terminal_rate","path"):
                if k in d: print(f"   {k}: {json.dumps(d[k])[:160]}")
    except Exception as e: print("invoke err:", str(e)[:120])

print("\n=== STRESS / TAIL ENGINES (S3) ===")
from datetime import datetime, timezone
now=datetime.now(timezone.utc)
for name in ["stress-scenarios","stress-simulator","stress-loadings","tail-risk","ciss-stress","correlation-breaks","inventory-drawdown","firm-stress"]:
    k=f"data/{name}.json"
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=k); age=(now-h["LastModified"]).total_seconds()/86400
        d=gj(k); keys=[x for x in d.keys() if x not in ("generated_at","version","duration_s","schema_version","method")]
        print(f"  {name}: {age:.1f}d  keys={keys[:14]}")
    except Exception as e:
        print(f"  {name}: MISSING")

print("\n=== STRESS schema details (fresh ones) ===")
for name in ["stress-scenarios","stress-simulator","tail-risk","ciss-stress","correlation-breaks"]:
    d=gj(f"data/{name}.json")
    if "_ERR" in d: continue
    print(f"\n  ■ {name}")
    for p in ["scenarios","worst_scenario","results","summary","headline","verdict","tail_risk_score","var_95","cvar","ciss","ciss_score","regime","breaks","worst_loss_pct","ranked","what_breaks_first","top_scenario"]:
        if p in d: print(f"     {p}: {json.dumps(d[p])[:170]}")

print("\n=== CFTC POSITIONING AGENT (Lambda URL) ===")
base="https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws"
for ep in ["/signals","/analysis"]:
    try:
        r=json.loads(urllib.request.urlopen(base+ep,timeout=20).read())
        print(f"  {ep}: keys={list(r.keys())[:14] if isinstance(r,dict) else 'list len '+str(len(r))}")
        if isinstance(r,dict):
            for k in ("signals","extremes","summary","headline","crowded","positioning","contracts","by_category"):
                if k in r: print(f"     {k}: {json.dumps(r[k])[:200]}")
    except Exception as e: print(f"  {ep}: ERR {str(e)[:80]}")
print("DONE 2334")
