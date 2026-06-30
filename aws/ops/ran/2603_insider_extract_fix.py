"""ops 2603 — redeploy w/ top_setups insider extraction; probe row schema; invoke; verify confluence."""
import boto3, io, zipfile, json, time, urllib.request
REGION="us-east-1"; FN="justhodl-insider-buyback-confluence"
SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION)
def get(p):
    return json.loads(urllib.request.urlopen(urllib.request.Request(f"https://justhodl-data-proxy.raafouis.workers.dev/{p}?t={int(time.time())}",headers={"User-Agent":"M"}),timeout=20).read())
ins=get("data/insider-buys-enriched.json")
ts=ins.get("top_setups") or []
print("insider top_setups:", len(ts), "rows")
if ts: print("  row[0] keys:", list(ts[0].keys())); print("  row[0] sample:", {k:ts[0].get(k) for k in list(ts[0].keys())[:10]})
def wait():
    for _ in range(25):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:160])
time.sleep(2)
j=get("data/insider-buyback-confluence.json")
print("STATE:", j.get("state"), "| n_confluences:", j.get("n_confluences"), "| n_high:", j.get("n_high_conviction"))
print("feeders insider/buyback counts:", j.get("feeders",{}).get("insider_tickers_count"), "/", j.get("feeders",{}).get("buyback_tickers_count"))
for c in (j.get("top_confluences") or [])[:10]:
    bs=c.get("buyback_stats") or {}; ins_s=c.get("insider_stats") or {}
    print(f"  {c['ticker']}: composite={c['composite_score']} (ins={c['insider_score']} byb={c['buyback_score']}) | {bs.get('class')} net{bs.get('net_yield')}% | {ins_s.get('n_buyers')} buyers")
print("DONE 2603")
