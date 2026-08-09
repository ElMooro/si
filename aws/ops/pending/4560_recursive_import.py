"""ops 4560 — deploy the recursive import, verify the discontinued-tag
probe (Perplexity: verify before wiring, don't repeat the ChicagoFed
mistake), reset state PRESERVING the 1,115 already-imported ids as a
skip-set, re-enable the cron, run one Event round + read state (no long
sync sockets — the 4557 lesson). Report expansion size + real numbers."""
import io,json,os,time,urllib.request,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; FN="justhodl-fred-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=60,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4560,"started":datetime.now(timezone.utc).isoformat()}
# discontinued-tag PROBE (verify, don't wire)
try:
    req=urllib.request.Request(
        "https://api.stlouisfed.org/fred/tags/series?tag_names=discontinued&api_key=2f057499936072679d8843d7fce99989&file_type=json&limit=3",
        headers={"User-Agent":"JustHodl research admin@justhodl.ai"})
    d=json.loads(urllib.request.urlopen(req,timeout=20).read())
    R["discontinued_tag_probe"]={"exists":bool(d.get("seriess")),"count":d.get("count"),
                                  "sample":[s.get("id") for s in d.get("seriess",[])[:3]]}
except Exception as e:
    R["discontinued_tag_probe"]={"exists":False,"err":str(e)[:100]}
# preserve imported skip-set, reset the rest
try:
    old=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    keep=old.get("imported_ids") or []
except Exception: keep=[]
s3.put_object(Bucket=B,Key="data/_state/fred-scoped-import.json",
    Body=json.dumps({"cats_done":[],"series_seen":0,"series_excluded_stale":0,
                     "series_imported":len(keep),"excluded_ids":[],
                     "imported_ids":keep,"n_pages":old.get("n_pages",0) if keep else 0,
                     "buffer":[],"reset_by":"ops4560_recursive"}).encode(),
    ContentType="application/json")
R["preserved_imported"]=len(keep)
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
deploy(FN)
ev.enable_rule(Name="justhodl-fred-catalog-5min")
R["cron"]=ev.describe_rule(Name="justhodl-fred-catalog-5min").get("State")
# Event + poll (never a long sync socket — 4557 lesson)
lam.invoke(FunctionName=FN,InvocationType="Event",Payload=json.dumps({"phase":"scoped_import"}).encode())
final=None
for _ in range(10):
    time.sleep(30)
    try:
        st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
        final={"n_categories_expanded":st.get("n_categories_expanded"),
               "cats_done":len(st.get("cats_done") or []),
               "series_seen":st.get("series_seen"),
               "series_imported":st.get("series_imported"),
               "excluded_stale":st.get("series_excluded_stale"),
               "excluded_discontinued":st.get("series_excluded_discontinued"),
               "skipped_already":st.get("series_skipped_already"),
               "accounting":st.get("accounting"),
               "status":st.get("status"),"blocked_at":st.get("blocked_at")}
        if st.get("updated_at","")>R["started"]: break
    except Exception as e: final={"err":str(e)[:80]}
R["round1"]=final
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"RECURSIVE IMPORT LIVE (your fixes, Khalid's proceed-order): expansion={final.get('n_categories_expanded') if final else '?'} "
  f"categories (was 7 roots). round1={json.dumps(final)[:350]}. discontinued_tag_probe={json.dumps(R['discontinued_tag_probe'])[:150]} "
  f"— title-match wired unconditionally, tag-filter only if probe confirms. preserved_imported={R['preserved_imported']} as skip-set. "
  "60/min, sequential, recency-break paging, strict accounting, flush-before-COMPLETE. Key rotation deferred by Khalid until import done.")})
bus({"action":"fanout_pending"})
R["verdict"]=f"expansion={final.get('n_categories_expanded') if final else '?'} round1={json.dumps(final)[:300]} probe={json.dumps(R['discontinued_tag_probe'])[:120]} cron={R['cron']}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4560.json","w"),indent=1,default=str)
open("aws/ops/reports/4560.md","w").write("# 4560 — "+R["verdict"]+"\n")
print(R["verdict"][:400])
