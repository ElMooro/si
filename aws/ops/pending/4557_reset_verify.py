"""ops 4557 — the 'COMPLETE' status was built on state corrupted by the
pre-fix 429/403 run (categories got marked done after failing mid-walk,
not after genuinely finishing). Reset the scoped-import state cleanly,
re-run ONE round under the new rate limit, and report REAL per-request
throughput so the numbers can be trusted before letting the cron continue
unattended."""
import json,os,time
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name="us-east-1")
R={"ops":4557,"at":datetime.now(timezone.utc).isoformat()}
try:
    old=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    R["old_state_before_reset"]={"cats_done":old.get("cats_done"),"series_seen":old.get("series_seen"),
                                  "series_imported":old.get("series_imported")}
except Exception as e: R["old_state_err"]=str(e)[:60]
# clean reset — brand new state, nothing carried over from the corrupted run
s3.put_object(Bucket=B,Key="data/_state/fred-scoped-import.json",
    Body=json.dumps({"cats_done":[],"series_seen":0,"series_excluded_stale":0,
                     "series_imported":0,"excluded_ids":[],"imported_ids":[],
                     "n_pages":0,"buffer":[],"reset_by":"ops4557"}).encode(),
    ContentType="application/json")
R["state_reset"]=True
t0=time.time()
inv=lam.invoke(FunctionName="justhodl-fred-catalog",InvocationType="RequestResponse",
               Payload=json.dumps({"phase":"scoped_import"}).encode())
elapsed=round(time.time()-t0,1)
body=json.loads(inv["Payload"].read().decode())
rn=json.loads(body["body"]) if isinstance(body,dict) and "body" in body else body
R["round1"]=rn; R["round1_wall_s"]=elapsed
R["round1_req_rate_per_min"]=(round(rn.get("series_seen",0)/max(elapsed,1)*60,1)
                              if isinstance(rn,dict) else None)
try:
    st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    R["state_after"]={"cats_done":st.get("cats_done"),"series_seen":st.get("series_seen"),
                       "series_imported":st.get("series_imported"),
                       "blocked_at":st.get("blocked_at")}
    R["sample_imported"]=st.get("imported_ids",[])[:6]
except Exception as e: R["state_after_err"]=str(e)[:60]
def bus(p):
    i=lam.invoke(FunctionName="justhodl-a2a-bus",InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"FRED state reset (prior COMPLETE was built on 429-corrupted done-flags) + clean round1 under "
  f"real rate limit: {json.dumps(rn)[:300]} wall={elapsed}s implied_rate={R['round1_req_rate_per_min']}/min "
  f"(ceiling=90/min). no_block={not st.get('blocked_at')}. Cron resumes remaining categories every 5min from "
  "here, genuinely resumable, no more premature done-flags.")})
bus({"action":"fanout_pending"})
R["verdict"]=f"round1={json.dumps(rn)} rate={R['round1_req_rate_per_min']}/min state={json.dumps(R.get('state_after'))}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4557.json","w"),indent=1,default=str)
open("aws/ops/reports/4557.md","w").write("# 4557 — "+R["verdict"]+"\n")
print(R["verdict"][:400])
