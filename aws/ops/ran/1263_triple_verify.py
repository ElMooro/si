"""1263 — re-run best-setups, verify Triple-Threat + compounder/revision tiers."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1263_triple.json"; BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(70)
try:
    r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:200]
except Exception as e: out["invoke"]=str(e)[:200]
time.sleep(2)
try:
    bs=json.loads(s3.get_object(Bucket=BUCKET,Key="data/best-setups.json")["Body"].read())
    setups=bs.get("top_setups",[])
    tt=bs.get("triple_threats",[])
    comp=[s for s in setups if "COMPOUNDER" in (s.get("signal_keys") or [])]
    rev=[s for s in setups if "REVISION_UP" in (s.get("signal_keys") or [])]
    out["result"]={"n_setups":len(setups),"triple_threats":len(tt),"compounder":len(comp),"revision":len(rev),
        "tt_sample":[{"t":s["ticker"],"conv":s["conviction"],"val":s.get("value_lenses"),"flow":(s.get("flow_lenses") or [])[:2]} for s in tt[:5]],
        "top5":[{"t":s["ticker"],"v":s["verdict"],"conv":s["conviction"],"sigs":s["signal_keys"]} for s in setups[:5]]}
    print("triple_threats:",len(tt),"| compounder:",len(comp),"| revision:",len(rev))
    for s in tt[:5]: print(f"  TT {s['ticker']} conv={s['conviction']} value={s.get('value_lenses')} flow={(s.get('flow_lenses') or [])[:2]}")
    print("Top setups:")
    for s in setups[:5]: print(f"  {s['ticker']:<6s} [{s['verdict']}] conv={s['conviction']} {s['signal_keys']}")
except Exception as e: out["result"]={"error":str(e)[:200]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print("DONE")
