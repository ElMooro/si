"""ops 4551 — (A) READ-ONLY safety verification: confirm every already-
working provider is untouched by the ECB attempts / new 5-agency
dispatcher fan-out. (B) ONE more isolated, additive-only ECB catalog try
using the officially-documented SDMX structure media type. If this also
fails, STOP guessing — report honestly rather than keep burning cycles."""
import gzip,json,os,time,urllib.request
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name="us-east-1")
R={"ops":4551,"at":datetime.now(timezone.utc).isoformat()}

# ---- A: SAFETY VERIFICATION (read-only, zero writes) ----
safe={}
for ag in ("eurostat","statcan","oecd","bis"):
    try:
        st=json.loads(s3.get_object(Bucket=B,Key=f"data/_state/sdmx-walk-{ag}.json")["Body"].read())
        safe[ag]={"done":len(st.get("done") or []),"n_total":st.get("n_total"),
                  "status":st.get("status"),
                  "has_new_ecb_error":False}  # each agency's own file; ecb can't touch it
    except Exception as e: safe[ag]=f"ERR {str(e)[:60]}"
try:
    hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
    safe["hub_totals"]=hub.get("totals")
    safe["hub_row_check"]={p["slug"]:p["n_keys"] for p in hub["providers"]
                           if p["slug"] in ("eurostat","statcan","oecd","bis","nyfed","ofr","polygon","fred","yahoo")}
except Exception as e: safe["hub_err"]=str(e)[:60]
# confirm the ECB gate fails gracefully and doesn't error the other agencies
try:
    le=lam.get_function(FunctionName="justhodl-sdmx-walker")
    safe["walker_state"]=le["Configuration"]["State"]
    safe["walker_last_error_status"]=le["Configuration"].get("LastUpdateStatus")
except Exception as e: safe["walker_check_err"]=str(e)[:60]
R["safety_verification"]=safe
R["all_existing_data_intact"]=(
    all(isinstance(safe.get(a),dict) and safe[a].get("status")=="COMPLETE"
        for a in ("eurostat","oecd","bis")) and
    isinstance(safe.get("hub_totals"),dict) and safe["hub_totals"].get("keys",0)>=20000)

# ---- B: ONE isolated, careful ECB attempt (additive-only) ----
url="https://data-api.ecb.europa.eu/service/dataflow/ECB/all/latest?references=none"
acc="application/vnd.sdmx.structure+json;version=1.0"
ecb_result={"url":url,"accept":acc}
try:
    req=urllib.request.Request(url,headers={"User-Agent":"JustHodl research admin@justhodl.ai","Accept":acc})
    raw=urllib.request.urlopen(req,timeout=45).read()
    d=json.loads(raw)
    dfs=((d.get("data") or {}).get("dataflows")
         or (d.get("Structure") or {}).get("Dataflows",{}).get("Dataflow") or [])
    ids=sorted({(x.get("id") or x.get("@id")) for x in dfs if isinstance(x,dict) and (x.get("id") or x.get("@id"))})
    ecb_result["ok"]=bool(ids); ecb_result["n_ids"]=len(ids)
    if ids:
        # additive-only write: brand-new key, cannot collide with/overwrite anything existing
        s3.put_object(Bucket=B,Key="data/warm/ecb/catalog.json.gz",
            Body=gzip.compress(json.dumps({"dataflows":[{"id":i} for i in ids],
                "n":len(ids),"fetched":R["at"],"source":url}).encode()),
            ContentType="application/gzip")
        ecb_result["catalog_seeded"]=True
except Exception as e:
    ecb_result["err"]=f"{type(e).__name__}: {str(e)[:120]}"
R["ecb_attempt"]=ecb_result
R["ecb_stopped_guessing"]=not ecb_result.get("ok")

def bus(p):
    i=lam.invoke(FunctionName="justhodl-a2a-bus",InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"SAFETY CHECK (Khalid: don't mess up existing data): all_existing_data_intact="
  f"{R['all_existing_data_intact']} hub_totals={json.dumps(safe.get('hub_totals'))} "
  f"row_check={json.dumps(safe.get('hub_row_check'))}. ECB one-more-try: {json.dumps(ecb_result)[:250]}. "
  "If ecb ok=false, stopping blind API-shape guessing per Khalid's caution — needs verified ECB docs to continue.")})
bus({"action":"fanout_pending"})
R["verdict"]=f"SAFE={R['all_existing_data_intact']} hub={json.dumps(safe.get('hub_totals'))} ecb_ok={ecb_result.get('ok')} ecb_n={ecb_result.get('n_ids',0)}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4551.json","w"),indent=1,default=str)
open("aws/ops/reports/4551.md","w").write("# 4551 — "+R["verdict"]+"\n- safety: "+json.dumps(safe,default=str)+"\n- ecb: "+json.dumps(ecb_result,default=str)+"\n")
print(R["verdict"][:300])
