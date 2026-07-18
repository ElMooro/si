"""ops 3428 — aiPanel decisive-call class resurrection (census #2, part 1).
Root cause: ai-brief-router has NO schedule — the panel rotation lost its
trigger ~June 10; only externally-invoked alerts-digest kept running.
Fix: invoke full rotation now (GLM fallback carries it) + Scheduler 6h.
Gates: registry lists the class · >=6 panel feeds regenerate today incl
regime-decisive-call · schedule exists."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
with report("3428_aipanel_resurrect") as rep:
    rep.heading("ops 3428 — aiPanel resurrection")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:320]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:280]; print(line); rep.log(line)
        if not ok: fails.append(n)
    reg=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="config/ai-brief-contexts.json")["Body"].read())
    ctx=list((reg.get("contexts") or {}).keys())
    panel=[c for c in ctx if c.endswith("-decisive-call") or c in ("ignition-names",)]
    gate("G1_registry", len(panel)>=6, f"contexts={len(ctx)} panel-class={panel[:12]}")
    t0=datetime.now(timezone.utc)
    try:
        r=LAM.invoke(FunctionName="justhodl-ai-brief-router",InvocationType="RequestResponse",Payload=b"{}")
        print("[invoke] sync",r.get("StatusCode"),r.get("FunctionError"))
    except Exception as e:
        print("[invoke] sync failed -> Event:",str(e)[:80])
        LAM.invoke(FunctionName="justhodl-ai-brief-router",InvocationType="Event",Payload=b"{}")
    fresh={}; dl=time.time()+420
    watch=panel[:14]
    while time.time()<dl:
        fresh={}
        for c in watch:
            try:
                o=S3C.head_object(Bucket="justhodl-dashboard-live",Key=f"data/{c}.json")
                fresh[c]=o["LastModified"]>=t0
            except Exception: fresh[c]=False
        if sum(fresh.values())>=6 and fresh.get("regime-decisive-call"): break
        time.sleep(20)
    gate("G2_class_regenerated", sum(fresh.values())>=6 and fresh.get("regime-decisive-call"),
         f"regenerated={sum(fresh.values())}/{len(watch)} regime={fresh.get('regime-decisive-call')} detail={ {k:('Y' if v else 'n') for k,v in fresh.items()} }")
    try:
        rc=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/regime-decisive-call.json")["Body"].read())
        out["regime_call"]={"one_liner":str(rc.get("one_liner"))[:140],"regime":rc.get("regime"),"confidence":rc.get("confidence")}
        line="LIVE CALL: "+json.dumps(out["regime_call"]); print(line); rep.log(line)
    except Exception: pass
    try:
        arn=LAM.get_function_configuration(FunctionName="justhodl-ai-brief-router")["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-ai-brief-router-6h"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-ai-brief-router-6h",ScheduleExpression="cron(20 1,7,13,19 * * ? *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:90]
    gate("G3_schedule", cr in ("exists","created"), cr)
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3428.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
