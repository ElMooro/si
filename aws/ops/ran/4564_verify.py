"""ops 4564 — deploy buf-accounting fix + oecd-denied + midas URLs on
the RUNNING epoch (no reset), verify reconciles=true and denied surfaces."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; FN="justhodl-fred-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=60,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4564,"started":datetime.now(timezone.utc).isoformat()}
def dep(fn):
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f3 in os.listdir("aws/shared"):
            if f3.endswith(".py"): z.write("aws/shared/"+f3,f3)
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=b.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
for fn in ("justhodl-fred-catalog","justhodl-global-expansion","justhodl-provider-catalog"):
    dep(fn)
lam.invoke(FunctionName=FN,InvocationType="Event",Payload=json.dumps({"phase":"scoped_import"}).encode())
final=None
for _ in range(9):
    time.sleep(30)
    st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    if st.get("updated_at","")>R["started"] and not (st.get("lease_until") or 0)>time.time():
        final={"cats_done":len(st.get("cats_done") or []),"of":st.get("n_categories_expanded"),
               "seen":st.get("series_seen"),"imported_total":st.get("series_imported"),
               "stale":st.get("series_excluded_stale"),"disc":st.get("series_excluded_discontinued"),
               "accounting":st.get("accounting"),"status":st.get("status")}
        break
R["round"]=final
ic=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
_=ic["Payload"].read(); time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
oecd=next((p for p in hub["providers"] if p["slug"]=="oecd"),{})
R["oecd_row"]={"datasets":oecd.get("datasets"),"target":oecd.get("datasets_target"),
               "coverage_pct":oecd.get("coverage_pct"),"denied_source_side":oecd.get("denied_source_side")}
def bus(p):
    i=lam.invoke(FunctionName="justhodl-a2a-bus",InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"Perplexity remaining items: FRED accounting fix (dropped len(buf) double-count) round={json.dumps(final)[:250]} "
  f"reconciles={final.get('accounting',{}).get('reconciles') if final else '?'}. OECD walked-vs-banked-vs-denied now on hub: "
  f"{json.dumps(R['oecd_row'])}. sec-midas -> your verified marketstructure/downloads + data-research URLs (UA present). "
  "data.html labels FILES not datasets. Import epoch still running, lease-protected.")})
bus({"action":"fanout_pending"})
R["verdict"]=f"round={json.dumps(final)[:250]} reconciles={final.get('accounting',{}).get('reconciles') if final else '?'} oecd={json.dumps(R['oecd_row'])}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4564.json","w"),indent=1,default=str)
open("aws/ops/reports/4564.md","w").write("# 4564 — "+R["verdict"]+"\n")
print(R["verdict"][:350])
