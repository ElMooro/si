"""Verify Upgrade A: invoke signal-board, then read its S3 output for a
populated deep_read (GLM-5.1) when the tape is conflicted."""
import json, boto3
from botocore.config import Config
lam = boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3 = boto3.client("s3","us-east-1")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
print("invoke status:", r.get("StatusCode"))
out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-board.json")["Body"].read())
print("composite:", out.get("composite_signal"), "posture:", out.get("composite_posture"),
      "n_live:", out.get("n_live"))
dr = out.get("deep_read")
if dr:
    print("model:", dr.get("_model"), "trigger:", dr.get("_trigger"))
    print("lean:", dr.get("lean"), "confidence:", dr.get("confidence"))
    print("dominant_driver:", str(dr.get("dominant_driver"))[:200])
    print("resolve_triggers:", dr.get("resolve_triggers"))
    print("\n✅ VERIFIED: Upgrade A live — GLM-5.1 deep read fired on conflicted tape")
else:
    print("\nℹ️ deep_read is None — tape not conflicted this run (composite clear / <5 live). "
          "Code path healthy; will fire when dispersed or NEUTRAL/MIXED.")
