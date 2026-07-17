"""ops 3407 — raw best-setups log tail + ddb best-setup-stack count (any date)."""
import json, sys, time
from pathlib import Path
import boto3
from boto3.dynamodb.conditions import Attr
from ops_report import report
LOGS=boto3.client("logs","us-east-1")
DDB=boto3.resource("dynamodb","us-east-1")
with report("3407_selflog_diag2") as rep:
    rep.heading("ops 3407 — raw tail + ddb count")
    streams=LOGS.describe_log_streams(logGroupName="/aws/lambda/justhodl-best-setups",
        orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
    lines=[]
    for st in streams:
        ev=LOGS.get_log_events(logGroupName="/aws/lambda/justhodl-best-setups",
            logStreamName=st["logStreamName"],limit=60,startFromHead=False)
        lines += [e["message"].strip()[:200] for e in ev.get("events",[])]
    print("RAW TAIL (last 2 streams):")
    for l in lines[-40:]: print("  ",l)
    n=0; lek=None
    tbl=DDB.Table("justhodl-signals")
    while True:
        kw={"FilterExpression":Attr("signal_type").eq("best-setup-stack"),"Select":"COUNT"}
        if lek: kw["ExclusiveStartKey"]=lek
        r=tbl.scan(**kw); n+=r.get("Count",0); lek=r.get("LastEvaluatedKey")
        if not lek: break
    print("DDB best-setup-stack rows total:",n)
    rep.log(f"ddb_count={n} tail_last={' || '.join(lines[-5:])}")
    Path("aws/ops/reports/3407.json").write_text(json.dumps({"ddb_count":n,"tail":lines[-25:]},indent=2))
    sys.exit(0)
