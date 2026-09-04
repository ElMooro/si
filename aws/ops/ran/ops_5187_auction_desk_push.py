"""ops_5187 -- push auction desk v1.2.2 (asset cache 3h so the late run sees the day's closes) and run once."""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

FN = "justhodl-auction-desk"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=330)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)

with report("ops_5187_auction_desk_push") as R:
    R.heading("ops 5187 -- auction desk v1.2.2 push")
    c = lam.get_function_configuration(FunctionName=FN)
    env = (c.get("Environment") or {}).get("Variables") or {}
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"), env_vars=env, timeout=300, memory=1536,
                            description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") in (None, "Successful"):
            break
        time.sleep(5)
    out = json.loads(lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"assets": True}).encode())["Payload"].read() or b"{}")
    R.log("   run -> %s" % json.dumps(out)[:300])
    if not out.get("ok"):
        sys.exit(1)
    R.ok("   v1.2.2 live")
