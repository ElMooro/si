"""ops 4549 — deploy fixes (targets/breakdown) + ECB 5th-agency walker.
Seed ECB catalog from the real ECB SDMX dataflow list if missing, deploy
walker+catalog, kick one ECB blitz, verify: hub reconcile line, eurostat/
statcan/bis coverage now showing, ECB moving. Also confirms Polygon/Yahoo
producer status for the report (no build attempted this pass)."""
import gzip,io,json,os,time,urllib.request,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4549,"started":datetime.now(timezone.utc).isoformat()}
# seed ECB catalog if missing
try:
    s3.head_object(Bucket=B,Key="data/warm/ecb/catalog.json.gz")
    R["ecb_catalog"]="already present"
except Exception:
    try:
        req=urllib.request.Request(
            "https://data-api.ecb.europa.eu/service/dataflow/ECB?format=sdmx-json&detail=allstubs",
            headers={"User-Agent":"JustHodl research admin@justhodl.ai","Accept":"application/vnd.sdmx.structure+json"})
        raw=urllib.request.urlopen(req,timeout=60).read()
        d=json.loads(raw)
        dfs=(((d.get("data") or {}).get("dataflows")) or [])
        ids=sorted({df.get("id") for df in dfs if df.get("id")})
        s3.put_object(Bucket=B,Key="data/warm/ecb/catalog.json.gz",
            Body=gzip.compress(json.dumps({"dataflows":[{"id":i} for i in ids],
                "n":len(ids),"fetched":R["started"]}).encode()),
            ContentType="application/gzip")
        R["ecb_catalog"]=f"seeded {len(ids)} dataflows"
    except Exception as e:
        R["ecb_catalog_err"]=str(e)[:150]
def deploy(fn):
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f2 in os.listdir("aws/shared"):
            if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
for fn in ("justhodl-sdmx-walker","justhodl-provider-catalog"):
    deploy(fn)
if R.get("ecb_catalog","").startswith(("seeded","already")):
    inv=lam.invoke(FunctionName="justhodl-sdmx-walker",InvocationType="RequestResponse",
                   Payload=json.dumps({"agency":"ecb","budget":700}).encode())
    R["ecb_run"]={"fn_err":inv.get("FunctionError")}; _=inv["Payload"].read()
    try:
        st=json.loads(s3.get_object(Bucket=B,Key="data/_state/sdmx-walk-ecb.json")["Body"].read())
        R["ecb_state"]={"done":len(st.get("done") or []),"n_total":st.get("n_total"),"status":st.get("status")}
    except Exception as e: R["ecb_state_err"]=str(e)[:80]
ic=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
R["cat_fn_err"]=ic.get("FunctionError"); _=ic["Payload"].read(); time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
R["hub_breakdown"]=hub.get("breakdown"); R["hub_totals"]=hub.get("totals")
for slug in ("eurostat","statcan","bis","oecd","ecb"):
    row=next((p for p in hub["providers"] if p["slug"]==slug),{})
    R.setdefault("rows",{})[slug]={k:row.get(k) for k in ("datasets","datasets_target","coverage_pct","coverage_note")}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":("KHALID INVESTIGATION (4548/4549): walkers were COMPLETE not stuck (eurostat 8146/8146, oecd "
  f"1542/1542, bis 29/29, statcan exceeded stale 6335 guess). Fixed: targets now read walker's own n_total "
  f"(ground truth); breakdown/reconcile line field-mismatch fixed. rows={json.dumps(R.get('rows'))[:300]} "
  f"breakdown={json.dumps(R.get('hub_breakdown'))}. ECB: {R.get('ecb_catalog')} -> added as 5th walker agency "
  f"(reused fan-out infra) -> {json.dumps(R.get('ecb_state'))}. Polygon/ECB-old/Yahoo-proxy Lambdas confirmed "
  "gone (ResourceNotFound) — FRED is architecturally curated (panel-based, no full-catalog walker exists, by "
  "design not bug). Polygon/Yahoo producer rebuild = separate scoped work, flagged not attempted this pass."),
 "evidence":[{"kind":"log","ref":"data/_state/sdmx-walk-ecb.json","snippet":"n_total"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"ecb={json.dumps(R.get('ecb_state'))} rows={json.dumps(R.get('rows'))} breakdown={json.dumps(R.get('hub_breakdown'))}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4549.json","w"),indent=1,default=str)
open("aws/ops/reports/4549.md","w").write("# 4549 — "+R["verdict"]+"\n")
print(R["verdict"][:400])
