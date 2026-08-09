"""ops 4550 — ECB catalog seed retry: the strict Accept header 406'd.
Try a small set of real candidates (content-negotiation varies by SDMX
implementation) and use whichever ECB's server actually accepts."""
import gzip,json,os,time,urllib.request
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4550,"started":datetime.now(timezone.utc).isoformat()}
CANDIDATES=[
 ("https://data-api.ecb.europa.eu/service/dataflow/ECB?format=sdmx-json","application/json"),
 ("https://data-api.ecb.europa.eu/service/dataflow/ECB/all/latest?format=sdmx-json","application/json"),
 ("https://data-api.ecb.europa.eu/service/dataflow/ECB","application/json"),
 ("https://data-api.ecb.europa.eu/service/dataflow","*/*"),
 ("https://sdw-wsrest.ecb.europa.eu/service/dataflow/ECB","application/json"),
]
ids=None; used=None
for url,acc in CANDIDATES:
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"JustHodl research admin@justhodl.ai","Accept":acc})
        raw=urllib.request.urlopen(req,timeout=45).read()
        d=json.loads(raw)
        dfs=None
        if isinstance(d,dict):
            dfs=((d.get("data") or {}).get("dataflows")
                 or (d.get("Structure") or {}).get("Dataflows",{}).get("Dataflow")
                 or d.get("dataflows"))
        if dfs:
            cand_ids=sorted({(x.get("id") or x.get("@id")) for x in dfs if isinstance(x,dict) and (x.get("id") or x.get("@id"))})
            if cand_ids:
                ids=cand_ids; used=url; break
        R.setdefault("attempts",[]).append({"url":url,"parsed_but_empty":True,"top_keys":list(d.keys())[:6] if isinstance(d,dict) else None})
    except Exception as e:
        R.setdefault("attempts",[]).append({"url":url,"err":f"{type(e).__name__}: {str(e)[:100]}"})
R["used_url"]=used; R["n_ids"]=len(ids) if ids else 0
if ids:
    s3.put_object(Bucket=B,Key="data/warm/ecb/catalog.json.gz",
        Body=gzip.compress(json.dumps({"dataflows":[{"id":i} for i in ids],
            "n":len(ids),"fetched":R["started"],"source":used}).encode()),
        ContentType="application/gzip")
    inv=lam.invoke(FunctionName="justhodl-sdmx-walker",InvocationType="RequestResponse",
                   Payload=json.dumps({"agency":"ecb","budget":700}).encode())
    R["ecb_run"]={"fn_err":inv.get("FunctionError")}; body=inv["Payload"].read().decode()
    R["ecb_run"]["body"]=body[:200]
    time.sleep(3)
    try:
        st=json.loads(s3.get_object(Bucket=B,Key="data/_state/sdmx-walk-ecb.json")["Body"].read())
        R["ecb_state"]={"done":len(st.get("done") or []),"n_total":st.get("n_total"),"status":st.get("status")}
    except Exception as e: R["ecb_state_err"]=str(e)[:80]
    ic=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
    _=ic["Payload"].read(); time.sleep(3)
    hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
    ecb_row=next((p for p in hub["providers"] if p["slug"]=="ecb"),{})
    R["ecb_hub_row"]={k:ecb_row.get(k) for k in ("datasets","datasets_target","coverage_pct")}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"ECB catalog seed retry: used={used} n_ids={R['n_ids']} state={json.dumps(R.get('ecb_state'))} "
  f"hub_row={json.dumps(R.get('ecb_hub_row'))}. attempts={json.dumps(R.get('attempts'))[:300]}"),
 "evidence":[{"kind":"log","ref":"data/warm/ecb/catalog.json.gz","snippet":"n"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"used={used} n_ids={R['n_ids']} state={json.dumps(R.get('ecb_state'))} hub={json.dumps(R.get('ecb_hub_row'))}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4550.json","w"),indent=1,default=str)
open("aws/ops/reports/4550.md","w").write("# 4550 — "+R["verdict"]+"\n- attempts: "+json.dumps(R.get("attempts"),default=str)+"\n")
print(R["verdict"][:350])
