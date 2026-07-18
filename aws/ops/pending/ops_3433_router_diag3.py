"""ops 3433 — why still 22/33: deployed condition + per-context statuses of
the newest rotation + which panel feeds are the stale 11."""
import io, json, sys, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1"); LOGS=boto3.client("logs","us-east-1")
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0"}
with report("3433_router_diag3") as rep:
    info=LAM.get_function(FunctionName="justhodl-ai-brief-router")
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
        src=zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace")
    wide='startswith("ERR")' in src
    narrow='startswith("ERR_CLAUDE")' in src
    line=f"deployed condition: wide={wide} narrow_only={narrow and not wide}"
    print(line); rep.log(line)
    st=LOGS.describe_log_streams(logGroupName="/aws/lambda/justhodl-ai-brief-router",orderBy="LastEventTime",descending=True,limit=2).get("logStreams",[])
    stat={}
    for s2 in st:
        ev=LOGS.get_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",logStreamName=s2["logStreamName"],limit=250,startFromHead=False).get("events",[])
        for e in ev:
            m=e["message"].strip()
            if "[ai-brief-router] " in m and (": OK" in m or ": ERR" in m):
                part=m.split("[ai-brief-router] ",1)[1]
                cid=part.split(":",1)[0]
                stat[cid]=part.split(":",1)[1].strip()[:60]
        if stat: break
    bad={k:v for k,v in stat.items() if not v.startswith("OK")}
    line=f"statuses: total={len(stat)} ok={len(stat)-len(bad)} bad={json.dumps(bad)[:700]}"
    print(line); rep.log(line)
    now=datetime.now(timezone.utc)
    reg=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="config/ai-brief-contexts.json")["Body"].read())
    panel=[c for c in (reg.get("contexts") or {}) if c.endswith("-decisive-call")]
    stale=[]
    for c in panel:
        try:
            o=S3C.head_object(Bucket="justhodl-dashboard-live",Key=f"data/{c}.json")
            if (now-o["LastModified"]).total_seconds()>3600*6: stale.append(c)
        except Exception: stale.append(c+"(404)")
    line="stale panels: "+json.dumps(stale)[:400]; print(line); rep.log(line)
    Path("aws/ops/reports/3433.json").write_text(json.dumps({"bad":bad,"stale":stale},indent=2)); sys.exit(0)
