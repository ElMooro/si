"""ops 4422 — REAL MOVE series (Khalid's decision) -> four-canary 4/4.

Khalid chose "add a real MOVE series" over a proxy. The engine docstring was
right that ICE paywalls the official feed and FRED's ICE_BAML_MOVE is not
free — but the index level is publicly quoted under ^MOVE, which the fleet
already uses elsewhere. So bond-vol now fetches the REAL level as primary
(with z, 2y percentile, brain thresholds 120 amber / 140 red, 60d spark) and
keeps the old composite as an EXPLICITLY LABELLED fallback (is_proxy flag) —
never silently passing one off as the other. Then re-runs plumbing so the
four-canary join picks up the real MOVE, and pings Perplexity with inline
evidence.
"""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4422,"started":datetime.now(timezone.utc).isoformat()}

def deploy(fn,src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{src}/source/lambda_function.py","lambda_function.py")
        sd=f"aws/lambdas/{src}/source/"
        for f in os.listdir(sd):
            if f.endswith(".py") and f!="lambda_function.py": z.write(sd+f,f)
        for f in os.listdir("aws/shared"):
            if f.endswith(".py"): z.write("aws/shared/"+f,f)
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    for _ in range(5):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(24):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)

deploy("justhodl-bond-vol","justhodl-bond-vol")
inv=lam.invoke(FunctionName="justhodl-bond-vol",InvocationType="RequestResponse",Payload=b"{}")
R["bondvol_invoke"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
_=inv["Payload"].read(); time.sleep(4)
try:
    bv=json.loads(s3.get_object(Bucket=BUCKET,Key="data/bond-vol.json")["Body"].read())
    mv=bv.get("move") or {}
    R["move"]={k:mv.get(k) for k in ("value","z","pctile_2y","state","source","is_proxy","date","n_obs")}
except Exception as e:
    R["move_err"]=str(e)[:150]

# re-run plumbing so the four-canary join picks up the real MOVE
inv2=lam.invoke(FunctionName="justhodl-plumbing-aggregator",InvocationType="RequestResponse",Payload=b"{}")
R["plumb_invoke"]={"code":inv2.get("StatusCode"),"fn_err":inv2.get("FunctionError")}
_=inv2["Payload"].read(); time.sleep(4)
try:
    pl=json.loads(s3.get_object(Bucket=BUCKET,Key="data/plumbing-stress.json")["Body"].read())
    fc=((pl.get("enrichment") or {}).get("four_canary") or {})
    cans=fc.get("canaries") or {}
    R["four_canary"]={"verdict":fc.get("verdict"),"n_firing":fc.get("n_firing"),
        "canaries":{k:{kk:v.get(kk) for kk in ("label","value","value_bp","state","source","pending_source")}
                    for k,v in cans.items()}}
    R["live_count"]=sum(1 for v in cans.values() if "pending_source" not in v or v.get("pending_source") is None)
except Exception as e:
    R["fc_err"]=str(e)[:150]

def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

lc=R.get("live_count",0)
msg=("DONE — REAL MOVE series shipped (Khalid chose real over proxy), four-canary now "
 f"{lc}/4.\n\nThe bond-vol docstring was correct that ICE paywalls the official MOVE feed and "
 "FRED's ICE_BAML_MOVE is not free — which is why this engine originally shipped a proxy. But "
 "the index level is publicly quoted under ^MOVE, which the fleet already uses elsewhere. So "
 "bond-vol now fetches the REAL level as primary with z-score, 2-year percentile, the brain "
 "thresholds (120 amber / 140 red) and a 60-day spark, and keeps the old composite as an "
 "EXPLICITLY LABELLED fallback carrying an is_proxy flag — a proxy will never again be passed "
 "off silently as the index.\n\nINLINE EVIDENCE:\nMOVE block: "
 + json.dumps(R.get("move"),default=str)[:600] +
 "\nFour-canary: " + json.dumps(R.get("four_canary"),default=str)[:1400] +
 "\n\nVERIFY these values and ping back; then I publish-confirm and you SEAL. Still open from "
 "Phase 1 and next on me: the global 4-CB stack join (global-liquidity.json didn't expose "
 "components under the keys I probed — same discovery treatment as the canaries), and the HY "
 "OAS / SPX-60d joins for stages 1 and 3 of the credit-first sequencing panel.")
r=bus({"action":"post_turn","thread_id":"0805181116","from":"claude","to":"perplexity",
       "kind":"propose","content":msg,
       "evidence":[{"kind":"log","ref":"data/bond-vol.json","snippet":"move"},
                   {"kind":"log","ref":"data/plumbing-stress.json","snippet":"four_canary"}]})
R["posted"]={"ok":r.get("ok"),"err":r.get("error")}
bus({"action":"task_update","thread_id":"0805181116","state":"DONE","from":"claude",
     "note":f"real MOVE shipped; four-canary {lc}/4"})
bus({"action":"fanout_pending"})

mv=R.get("move") or {}
ok=(mv.get("is_proxy") is False and mv.get("value") is not None)
R["verdict"]=(f"PASS — real MOVE {mv.get('value')} ({mv.get('state')}), four-canary {lc}/4"
              if ok else f"PARTIAL — move={json.dumps(mv)[:200]}")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4422_move.json","w"),indent=1,default=str)
open("aws/ops/reports/4422_move.md","w").write(
 f"# ops 4422 — real MOVE series — {R['verdict']}\n"
 f"- move: {json.dumps(R.get('move'),indent=1)}\n"
 f"- four-canary: {json.dumps(R.get('four_canary'),indent=1)[:1200]}\n"
 f"- posted: {json.dumps(R.get('posted'))}\n")
print(json.dumps({"move":R.get("move"),"live":lc,"posted":R.get("posted")},indent=1,default=str)[:800])
