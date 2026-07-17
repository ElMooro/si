"""ops 3404 — decisive diagnosis: sync-invoke composer (print payload/error)
+ tail CloudWatch for proven-portfolio and best-setups; surface ERROR lines."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=340))
LOGS=boto3.client("logs","us-east-1")
with report("3404_diag_logs") as rep:
    rep.heading("ops 3404 — CW diagnosis")
    r=LAM.invoke(FunctionName="justhodl-proven-portfolio",InvocationType="RequestResponse",Payload=b"{}")
    pay=r["Payload"].read().decode("utf-8","replace")[:600]
    print("composer invoke: status",r.get("StatusCode"),"err",r.get("FunctionError"))
    print("payload:",pay); rep.log(f"composer err={r.get('FunctionError')} payload={pay[:300]}")
    for fn in ("justhodl-proven-portfolio","justhodl-best-setups"):
        try:
            ev=LOGS.filter_log_events(logGroupName=f"/aws/lambda/{fn}",
                startTime=int((time.time()-2400)*1000),
                filterPattern="?ERROR ?Error ?failed ?Traceback ?self-log")
            lines=[e["message"].strip()[:220] for e in ev.get("events",[])][-14:]
            print(f"════ {fn} log tail:"); [print("  ",l) for l in lines]
            rep.log(fn+" | "+" || ".join(lines[-6:]))
        except Exception as e:
            print(fn,"logs:",str(e)[:100])
    Path("aws/ops/reports/3404.json").write_text(json.dumps({"payload":pay},indent=2))
    sys.exit(0)
