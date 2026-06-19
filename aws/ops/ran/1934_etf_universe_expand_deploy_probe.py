import boto3, json, time, io, zipfile, os
from botocore.config import Config
from datetime import datetime, timezone

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=320, connect_timeout=20, retries={"max_attempts": 0}))
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
FN = "justhodl-etf-fund-flows"
SRC = "aws/lambdas/justhodl-etf-fund-flows/source/lambda_function.py"

NEW = ["FNGU","FNGD","BULZ","BERZ","DDM","DXD","DOG","PSQ","SPUU","HIBL","HIBS","MIDU","MVV","MZZ","RWM","SAA",
       "ROM","REW","USD","SSG","UYG","SKF","DIG","DUG","RXL","BIB","BIS","UXI","UCC","URE","SRS","UYM","SMN","UPW",
       "EDC","EDZ","EURL","INDL","BRZU","KORU","MEXX","TPOR","UUP","UDN","EUO","ULE","YCS",
       "BITU","SBIT","ETHT","MSTX","TSLR","TSLS","NVDX","NVDU","GGLS","METD","AMZD","MSFD","CONI"]

# 1) force-deploy from disk (deploy-lambdas can miss source diffs; no shared deps -> single file zip)
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.write(SRC, arcname="lambda_function.py")
buf.seek(0)
for i in range(24):
    try:
        lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
        print("update_function_code OK")
        break
    except lam.exceptions.ResourceConflictException:
        print("conflict, retry", i); time.sleep(5)
# wait active
for _ in range(40):
    c = lam.get_function_configuration(FunctionName=FN)
    if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(5)
print("state:", c["State"], c.get("LastUpdateStatus"), "codesize:", c["CodeSize"])

# 2) invoke async, poll daily.json freshness
start = datetime.now(timezone.utc)
lam.invoke(FunctionName=FN, InvocationType="Event")
print("async invoke fired", start.isoformat())
fresh = None
for _ in range(28):
    time.sleep(15)
    try:
        h = s3.head_object(Bucket=B, Key="etf-flows/daily.json")
        lm = h["LastModified"]
        if lm > start:
            fresh = lm; break
    except Exception as e:
        pass
print("daily.json refreshed:", fresh.isoformat() if fresh else "TIMEOUT (will read latest anyway)")

# 3) coverage probe
d = json.loads(s3.get_object(Bucket=B, Key="etf-flows/daily.json")["Body"].read())
metrics = d.get("metrics", [])
by = {m["ticker"]: m for m in metrics}
print("\nTOTAL metrics in daily.json:", len(metrics))
have, miss = [], []
for t in NEW:
    m = by.get(t)
    n = (m or {}).get("n_history_points", 0) or 0
    if m and n > 0:
        have.append((t, n, m.get("flow_5d_usd"), m.get("subcategory")))
    else:
        miss.append(t)
print("\nNEW tickers WITH data: %d/%d" % (len(have), len(NEW)))
for t, n, f5, sub in sorted(have, key=lambda x: -(x[1])):
    fv = ("$%.1fM" % (f5/1e6)) if isinstance(f5,(int,float)) else "n/a"
    print("  %-5s n=%-3d flow5d=%-10s %s" % (t, n, fv, sub))
print("\nNEW tickers MISSING/empty: %d -> %s" % (len(miss), ",".join(miss)))
