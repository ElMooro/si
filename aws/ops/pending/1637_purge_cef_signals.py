# ops 1637 — delete the two retracted CEF research_paper signals (stat hygiene for the new type)
import json, os
import boto3
from botocore.config import Config
ddb = boto3.resource("dynamodb", region_name="us-east-1",
                      config=Config(read_timeout=300, retries={"max_attempts": 1}))
T = ddb.Table("justhodl-signals")
out = {"ops": 1637, "deleted": []}
for sid in ("research_paper#RQI#2026-06-12", "research_paper#GAB#2026-06-12"):
    r = T.delete_item(Key={"signal_id": sid}, ReturnValues="ALL_OLD")
    out["deleted"].append({"id": sid, "existed": bool(r.get("Attributes"))})
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1637_purge_cef_signals.json", "w").write(json.dumps(out, indent=1))
print(json.dumps(out))
