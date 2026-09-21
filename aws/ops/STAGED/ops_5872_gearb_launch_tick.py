"""ops 5872 -- one real Gear B tick with a patient client, then the recorded result (Claude, 2026-09-21). Direct lane."""
import json, sys
from pathlib import Path
import boto3
from botocore.config import Config
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0}))
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5872_gearb_launch_tick") as r:
        r.heading("ops 5872 -- Gear B tick (launch=true) and its recorded result")
        resp = lam.invoke(FunctionName="justhodl-ai", InvocationType="RequestResponse", Payload=json.dumps({"mode": "gearb", "body": {"launch": True}}).encode())
        out = json.loads(resp["Payload"].read()); res = out.get("result") or out
        r.kv(built=json.dumps(res.get("built"))[:300], launched=json.dumps(res.get("launched"))[:400], refusal=str(res.get("refusal"))[:400], examined=json.dumps(res.get("examined"))[:200], decided=json.dumps(res.get("decided"))[:200])
        try:
            last = json.loads(s3.get_object(Bucket=PRI, Key="factory/gearb/last_tick.json")["Body"].read())
            r.log("last_tick.json: " + json.dumps(last, default=str)[:900])
        except Exception as e:  # noqa: BLE001
            r.warn("last_tick.json: %s" % str(e)[:120])
        (r.ok if res.get("launched") else r.warn)("launched: %s" % json.dumps(res.get("launched"))[:200] if res.get("launched") else "not launched: %s" % res.get("refusal"))


if __name__ == "__main__":
    main()
