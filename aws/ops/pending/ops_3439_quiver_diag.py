"""ops 3439 — political desk: is Quiver auth alive? Last run tail + a direct
API probe with the engine's own token source."""
import json, sys, urllib.request
from pathlib import Path
import boto3
from ops_report import report
LOGS=boto3.client("logs","us-east-1"); SSM=boto3.client("ssm","us-east-1")
LAM=boto3.client("lambda","us-east-1")
with report("3439_quiver_diag") as rep:
    st=LOGS.describe_log_streams(logGroupName="/aws/lambda/justhodl-lobbying-intel",orderBy="LastEventTime",descending=True,limit=1).get("logStreams",[])
    if st:
        from datetime import datetime, timezone
        ts=st[0].get("lastEventTimestamp",0)/1000
        line="last_log: "+datetime.fromtimestamp(ts,tz=timezone.utc).isoformat()[:16]; print(line); rep.log(line)
        ev=LOGS.get_log_events(logGroupName="/aws/lambda/justhodl-lobbying-intel",logStreamName=st[0]["logStreamName"],limit=25,startFromHead=False).get("events",[])
        for e in ev[-8:]:
            m=e["message"].strip()[:150]; print(m); rep.log(m)
    tok=None
    for name in ("/justhodl/quiver-api-key","/justhodl/quiver/api-key","/justhodl/quiverquant-api-key"):
        try:
            tok=SSM.get_parameter(Name=name,WithDecryption=True)["Parameter"]["Value"]; src=name; break
        except Exception: pass
    if not tok:
        env=LAM.get_function_configuration(FunctionName="justhodl-lobbying-intel").get("Environment",{}).get("Variables",{})
        for k,v in env.items():
            if "QUIVER" in k.upper(): tok=v; src="env:"+k; break
    line=f"token: {'FOUND '+src if tok else 'NONE'}"; print(line); rep.log(line)
    if tok:
        try:
            req=urllib.request.Request("https://api.quiverquant.com/beta/live/lobbying?limit=1",
                headers={"Authorization":f"Bearer {tok}","User-Agent":"justhodl/1.0"})
            with urllib.request.urlopen(req,timeout=15) as r:
                line=f"probe: HTTP {r.status} body={r.read()[:120]!r}"
        except Exception as e:
            line=f"probe: {str(e)[:140]}"
        print(line); rep.log(line)
    Path("aws/ops/reports/3439.json").write_text("{}"); sys.exit(0)
