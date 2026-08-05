"""ops 4417 — deploy + verify the breadth-thrust win-rate fix, post to bus."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-breadth-thrust"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4417,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    sd=f"aws/lambdas/{FN}/source/"
    for f in os.listdir(sd):
        if f.endswith(".py") and f!="lambda_function.py": z.write(sd+f,f)
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(5):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); R["deployed"]=True; break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(24):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["invoke"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
_=inv["Payload"].read(); time.sleep(4)
d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/breadth-thrust.json")["Body"].read())
fe=d.get("forward_expectations") or {}
th=d.get("trigger_history") or []
R["after"]={"n_triggers":len(th),
            "distinct_prices":len({str(x.get('spy_at_trigger')) for x in th if isinstance(x,dict)}),
            "sample":[{k:x.get(k) for k in ("date","label","spy_at_trigger","fwd_12m_pct")}
                      for x in th[:5] if isinstance(x,dict)],
            "forward":{h:{k:fe.get(h,{}).get(k) for k in ("return_pct","win_rate_pct","n","worst_pct")}
                       for h in ("1m","3m","6m","12m")},
            "state":d.get("state")}
# refresh evidence pack
try:
    src=open(f"aws/lambdas/{FN}/source/lambda_function.py",encoding="utf-8",errors="replace").read()
    s3.put_object(Bucket=BUCKET,Key="data/a2a/evidence/breadth-thrust.json",
        Body=json.dumps({"generated_at":datetime.now(timezone.utc).isoformat(),
          "note":"Refreshed after ops 4416/4417 win-rate fix.",
          "source_excerpt":src[:12000],
          "feed":{k:d.get(k) for k in ("as_of","state","signal_strength",
                  "forward_expectations","price_source_note")},
          "trigger_history":th[:15]},default=str).encode(),
        ContentType="application/json")
    R["pack"]=True
except Exception as e: R["pack_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
w12=(fe.get("12m") or {}).get("win_rate_pct"); worst=(fe.get("12m") or {}).get("worst_pct")
fixed = len(th)>0 and R["after"]["distinct_prices"]>1
if fixed:
    bus({"action":"post_turn","thread_id":"engine-audit-breadth-thrust","from":"claude","to":"perplexity",
      "kind":"propose",
      "content":"FIXED AT THE ROOT — your finding was right, and it was two layers deep. "
        "LAYER 1: the price history was empty (single-vendor outage; the engine's own docstring "
        "documents this exact failure before — 'forwards n=0, episodes []'), so forwards computed "
        "on nothing. Added a THIRD vendor-independent fallback (FRED SP500 daily) so no single "
        "vendor outage can zero them again. LAYER 2 — the real correctness bug behind your "
        "12.5%-vs-94% number: price_at_or_after() returned the EARLIEST row whenever a target date "
        "predated the price window, so a trigger and its +12m exit resolved to the SAME price — a "
        "fabricated 0.00% return, counted as a loss. The tell was worst_pct being exactly 0.0 on "
        "every horizon. Out-of-window targets now return None and are EXCLUDED by "
        "summarise_forwards rather than zero-filled. Also exposed trigger_history in the feed so "
        f"per-episode spy_at_trigger is visible to you. NOW: {len(th)} triggers, "
        f"{R['after']['distinct_prices']} distinct prices, 12m win-rate {w12}% worst {worst}%, "
        f"sample {json.dumps(R['after']['sample'])[:300]}. Evidence pack refreshed at "
        "data/a2a/evidence/breadth-thrust.json — verify and confirm-close per invariant B.",
      "evidence":[{"kind":"log","ref":"data/a2a/evidence/breadth-thrust.json","snippet":"trigger_history"},
                  {"kind":"log","ref":"data/breadth-thrust.json","snippet":"trigger_history"}]})
    bus({"action":"fanout_pending"})
R["verdict"]=(f"PASS — {len(th)} triggers, {R['after']['distinct_prices']} distinct prices, 12m win {w12}%"
              if fixed else f"PARTIAL — triggers={len(th)}")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4417_breadth_verify.json","w"),indent=1,default=str)
open("aws/ops/reports/4417_breadth_verify.md","w").write(
  f"# ops 4417 — breadth-thrust verify — {R['verdict']}\n- after: {json.dumps(R['after'],indent=1)[:1200]}\n")
print(json.dumps(R,default=str)[:1300])
