import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-liquidity-inflection"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read()).get("backtest") or {}
print("BACKTEST",b.get("start"),"→",b.get("end"),"(",b.get("years"),"yr)")
s=b.get("strategy") or {}; h=b.get("buy_hold") or {}
print(f"  STRATEGY: tot {s.get('total_return_pct')}% · CAGR {s.get('cagr_pct')}% · Sharpe {s.get('sharpe')} · maxDD {s.get('max_drawdown_pct')}% · inMkt {s.get('time_in_market_pct')}% · {s.get('switches')}sw")
print(f"  BUY&HOLD: tot {h.get('total_return_pct')}% · CAGR {h.get('cagr_pct')}% · Sharpe {h.get('sharpe')} · maxDD {h.get('max_drawdown_pct')}%")
print("  VERDICT:",b.get("verdict"),"| better DD:",b.get("edge_on_drawdown"),"| curve pts:",len(b.get("curve") or []))
print("DONE 2626")
