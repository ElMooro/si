import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"

print("=== theme-rotation ===")
try:
    r=lam.invoke(FunctionName="justhodl-theme-rotation",InvocationType="RequestResponse")
    print("invoke:",r["Payload"].read().decode()[:120])
except Exception as e: print("invoke err",str(e)[:100])
d=json.loads(s3.get_object(Bucket=B,Key="data/theme-rotation.json")["Body"].read())
print("market_context:",json.dumps(d.get("market_context",{})))
wf=[t for t in d.get("themes",[]) if t.get("flow_z") is not None]
print("themes with real $ flow: %d / %d"%(len(wf),len(d.get("themes",[]))))
conf=[t for t in wf if t.get("flow_confirm")=="CONFIRMED"]; div=[t for t in wf if t.get("flow_confirm")=="DIVERGENT"]
print("CONFIRMED (price+money agree):",[ "%s(%s,z%.1f)"%(t["theme"],t["etf"],t["flow_z"]) for t in conf[:6]])
print("DIVERGENT (price up, money out):",[ "%s(%s,z%.1f)"%(t["theme"],t["etf"],t["flow_z"]) for t in div[:6]])
print("top 5 by combined_score:",[ "%s(rot%.0f→%.0f flow%s)"%(t["theme"],t.get("rotation_score") or 0,t.get("combined_score") or 0,t.get("flow_confirm")) for t in d.get("themes",[])[:5]])

print("\n=== sector-rotation (may take ~2min) ===")
try:
    r=lam.invoke(FunctionName="justhodl-sector-rotation",InvocationType="RequestResponse")
    print("invoke:",r["Payload"].read().decode()[:120])
except Exception as e: print("invoke err",str(e)[:120])
d=json.loads(s3.get_object(Bucket=B,Key="data/sector-rotation.json")["Body"].read())
secs=d.get("sectors",[]); wf=[s for s in secs if s.get("etf_flow_z") is not None]
print("sectors with real $ flow: %d / %d"%(len(wf),len(secs)))
print("real_money_flow ranking:",json.dumps((d.get("summary",{}) or {}).get("real_money_flow",[])[:11]))
print("per-sector (rank, score pre→post flow, confirm):")
for s in secs[:11]:
    print("  #%-2s %-5s score %s→%s  z=%s  %s"%(s.get("rank"),s.get("symbol"),
        s.get("rotation_score_preflow"),s.get("rotation_score"),s.get("etf_flow_z"),s.get("etf_flow_confirm")))
