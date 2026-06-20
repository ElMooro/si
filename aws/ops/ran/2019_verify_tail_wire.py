"""ops 2019: verify tail-risk WARMING + implied-prob now carries real density crash probs + page live."""
import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-tail-risk",InvocationType="RequestResponse")
print("tail-risk:",r["StatusCode"],"|",r["Payload"].read().decode()[:200])
time.sleep(1)
d=json.loads(s3.get_object(Bucket=B,Key="data/tail-risk.json")["Body"].read())
print("  valuation now:",d.get("tail_valuation"),"(should be WARMING on cold history)")
print("\ninvoking implied-prob (wire check)…")
r2=lam.invoke(FunctionName="justhodl-implied-prob",InvocationType="RequestResponse")
print("  implied-prob:",r2["StatusCode"])
time.sleep(2)
ip=json.loads(s3.get_object(Bucket=B,Key="data/implied-prob.json")["Body"].read())
tb=ip.get("tail_risk")
print("  tail_risk block:",tb)
spy=ip.get("spy",{})
print("  SPY log-normal p_down_10:",(spy.get("moves_30d") or {}).get("p_down_10"))
print("  SPY DENSITY moves_30d:",spy.get("density_moves_30d"))
print("\npage check:")
try:
    with urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/tail-risk.html?t={int(time.time())}",headers={"User-Agent":"v"}),timeout=20) as resp:
        print("  tail-risk.html HTTP",resp.getcode(),"bytes",len(resp.read()))
except Exception as e: print("  page err",str(e)[:80])
print("DONE 2019")
