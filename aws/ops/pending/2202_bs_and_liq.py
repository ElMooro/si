import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=400,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
# verify best-setups still healthy
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-best-setups")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
try:
    lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
    print(f"best-setups OK: top_setups={len(b.get('top_setups') or [])} (earnings-confluence wired, tags appear in earnings season)")
except Exception as e: print("best-setups ERR:",str(e)[:80])
# probe liquidity cluster for risk-regime overlay
print("\n=== LIQUIDITY CLUSTER schemas ===")
import re as _re
for f in ["global-liquidity","china-liquidity","crypto-liquidity","liquidity-inflection","repo-lending","cb-injection","cb-stance"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        head={k:v for k,v in d.items() if isinstance(v,(int,float,str,bool)) and _re.search(r'score|signal|regime|state|trend|label|level|stance|impulse|direction|status',k,_re.I)}
        print(f"  {f}: keys={list(d.keys())[:8]}")
        if head: print(f"      headline={json.dumps(head)[:150]}")
    except Exception as e: print(f"  {f}: ERR {str(e)[:35]}")
print("DONE 2202")
