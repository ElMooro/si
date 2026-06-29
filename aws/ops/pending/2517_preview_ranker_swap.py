import boto3, json, io, zipfile, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
FN="justhodl-master-ranker"; LIVE="data/master-ranker.json"; PREV="data/_preview/master-ranker.json"
SRC="aws/lambdas/justhodl-master-ranker/source/lambda_function.py"
def getj(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
def waitcfg():
    for _ in range(45):
        if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": return
        time.sleep(2)

before=getj(LIVE)
bt={r["ticker"]:r for r in before.get("top_tickers",[])}
print("BEFORE top_tickers:",len(bt),"as_of",before.get("as_of"))
loc=lam.get_function(FunctionName=FN)["Code"]["Location"]
old_zip=urllib.request.urlopen(loc,timeout=60).read()
print("saved live code bytes:",len(old_zip))
cfg=lam.get_function_configuration(FunctionName=FN)
env=(cfg.get("Environment") or {}).get("Variables",{}) or {}
had_key="S3_KEY_OUT" in env
env2=dict(env); env2["S3_KEY_OUT"]=PREV
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":env2}); waitcfg()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); waitcfg()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError"))
time.sleep(3)
try: after=getj(PREV)
except Exception as e: after={"top_tickers":[]}; print("PREVIEW read err",str(e)[:80])
at={r["ticker"]:r for r in after.get("top_tickers",[])}
print("AFTER top_tickers:",len(at),"as_of",after.get("as_of"))
# RESTORE exact live code + original env
lam.update_function_code(FunctionName=FN,ZipFile=old_zip); waitcfg()
env3=dict(env)
if not had_key: env3.pop("S3_KEY_OUT",None)
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":env3}); waitcfg()
print("RESTORED. live as_of still:",getj(LIVE).get("as_of"),"(unchanged = good)")
# DIFF
b_rank={t:i+1 for i,t in enumerate([r["ticker"] for r in before.get("top_tickers",[])])}
a_rank={t:i+1 for i,t in enumerate([r["ticker"] for r in after.get("top_tickers",[])])}
moved=[]
for t in a_rank:
    if t in b_rank:
        moved.append((b_rank[t]-a_rank[t],t,b_rank[t],a_rank[t],bt[t]["score"],at[t]["score"]))
print("\n=== RANK MOVES (both top-25, by |move|) ===")
for d,t,br,ar,bs,as_ in sorted(moved,key=lambda x:-abs(x[0]))[:18]:
    arrow="UP" if d>0 else ("DN" if d<0 else "--")
    print(f"  {t:6} #{br:>2}->#{ar:>2} {arrow}{abs(d) or ''}  score {bs}->{as_}")
print("ENTERED top25:",[t for t in a_rank if t not in b_rank] or "none")
print("LEFT top25:   ",[t for t in b_rank if t not in a_rank] or "none")
cl=[r for r in after.get("top_tickers",[]) if r.get("sector_mult_clamped")]
print(f"\nCLAMPED in top25: {len(cl)} of {len(at)}")
for r in cl[:12]: print(f"  {r['ticker']:6} base={r.get('base_score')} sectorMult={r.get('sector_mult_combined')} final={r.get('score')}")
print("DONE 2517")
