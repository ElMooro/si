import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-scarcity-radar")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-scarcity-radar",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/scarcity-radar.json")["Body"].read())
board=d.get("stealth_shortage_board") or []
none_v=[r["ticker"] for r in board if not r.get("vertical")]
print("counts:",json.dumps(d.get("counts")))
print(f"board names with vertical=None: {len(none_v)}/{len(board)} -> {none_v[:8]}")
print("names that previously were None (now mapped?):")
for tk in ("NTAP","JNPR","PSTG","EXTR","DGII","MKSI"):
    r=next((x for x in board if x["ticker"]==tk),None)
    if r: print(f"  {tk}: vertical={r.get('vertical')} tight={r.get('vertical_tightness')} scar={r.get('scarcity')} steal={r.get('stealth')}")
# page liveness (must check from RUNNER, sandbox is 403-blocked)
def fetch(u):
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 justhodl-verify"})
        r=urllib.request.urlopen(req,timeout=20);return r.status,r.read().decode("utf-8","replace")
    except Exception as e: return "ERR",str(e)[:80]
st,body=fetch("https://justhodl.ai/scarcity-radar.html")
print(f"\nPAGE justhodl.ai/scarcity-radar.html -> {st} | has title: {'Scarcity Radar' in str(body)} | has board key: {'stealth_shortage_board' in str(body)} | has vertical bars: {'Which shortage is building' in str(body)}")
st2,body2=fetch("https://justhodl-data-proxy.raafouis.workers.dev/data/scarcity-radar.json")
print(f"PROXY data/scarcity-radar.json -> {st2} | valid json: {body2[:1]=='{' if isinstance(body2,str) else False}")
print("DONE 2233")
