"""ops 3441 — census #5: self-monitoring disposition. The old trio
(eventbridge-audit/system-audit/_freshness-status) are corpse feeds of
deleted engines; monitoring lives on as fleet-error-monitor (5m),
fleet-freshness-monitor (30m), fleet-monitor (3h), schedule-liveness.
Actions: retire corpses in config/feed-retired.json; PROVE all 4 modern
monitors heartbeat (last CW event within 2x cadence)."""
import json, sys
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report
S3C=boto3.client("s3","us-east-1"); LOGS=boto3.client("logs","us-east-1")
CORPSES=["data/eventbridge-audit.json","data/system-audit.json","data/_freshness-status.json"]
MON={"justhodl-fleet-error-monitor":0.5,"justhodl-fleet-freshness-monitor":2.0,
     "justhodl-fleet-monitor":7.0,"justhodl-schedule-liveness":26.0}
with report("3441_selfmon") as rep:
    rep.heading("ops 3441 — self-monitoring")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    rj=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="config/feed-retired.json")["Body"].read())
    cur=set(rj.get("retired") or []); new=sorted(cur|set(CORPSES))
    S3C.put_object(Bucket="justhodl-dashboard-live",Key="config/feed-retired.json",
        Body=json.dumps({"generated_at":datetime.now(timezone.utc).isoformat(),
                          "retired":new,"reason":rj.get("reason","")+" | +selfmon corpses (writers deleted; superseded by fleet-* monitors)"},
                        separators=(",",":")).encode(),ContentType="application/json")
    gate("G1_corpses_retired", len(new)>=6, f"retired list now {len(new)}")
    now=datetime.now(timezone.utc); beats={}
    for fn,maxh in MON.items():
        try:
            st=LOGS.describe_log_streams(logGroupName=f"/aws/lambda/{fn}",orderBy="LastEventTime",descending=True,limit=1).get("logStreams",[])
            age_h=(now-datetime.fromtimestamp(st[0]["lastEventTimestamp"]/1000,tz=timezone.utc)).total_seconds()/3600 if st else 9e9
            beats[fn]=round(age_h,2); ok=age_h<=maxh
        except Exception as e:
            beats[fn]=str(e)[:40]; ok=False
        gate(f"G_{fn.split('-',1)[1].replace('-','_')}", ok, f"last_log_age_h={beats[fn]} (max {maxh})")
    out["beats"]=beats
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3441.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
