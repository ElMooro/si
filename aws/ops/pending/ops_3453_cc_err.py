"""ops 3453 — credit-composite live-path exception capture."""
import json, sys
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=180))
with report("3453_cc_err") as rep:
    r=LAM.invoke(FunctionName="justhodl-credit-composite",InvocationType="RequestResponse",Payload=b"{}")
    fe=r.get("FunctionError"); pl=r["Payload"].read().decode("utf-8","replace")[:900]
    line=f"FunctionError={fe} payload={pl}"
    print(line); rep.log(line[:500])
    Path("aws/ops/reports/3453.json").write_text("{}"); sys.exit(0)
