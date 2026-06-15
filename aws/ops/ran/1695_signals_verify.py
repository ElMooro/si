import json, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
# confirm function exists + invoke (it self-spaces ~80s)
try:
    cfg=lam.get_function_configuration(FunctionName="justhodl-finviz-signals")
    print("function:",cfg["FunctionName"],"timeout",cfg["Timeout"],"runtime",cfg["Runtime"])
except Exception as e:
    print("get_function FAIL:",str(e)[:120]); raise SystemExit(0)
r=lam.invoke(FunctionName="justhodl-finviz-signals",InvocationType="RequestResponse")
print("invoke status:",r["StatusCode"])
print("payload:",r["Payload"].read().decode()[:400])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-signals.json")["Body"].read())
print("\ngenerated_at:",d.get("generated_at"))
c=d.get("counts",{})
for k in sorted(c, key=lambda x:-c[x]): print(f"  {k:16} {c[k]}")
gc=d["signals"].get("golden_cross",[])[:4]
print("\ngolden_cross sample:", [(x.get("ticker"),x.get("perf_m"),x.get("analyst_recom")) for x in gc])
ib=d["signals"].get("insider_buys",[])[:4]
print("insider_buys sample:", [x.get("ticker") for x in ib])
