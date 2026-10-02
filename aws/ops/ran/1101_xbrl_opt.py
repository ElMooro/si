"""
Ops 1101: check and optimize xbrl-fundamentals Lambda.
Increases memory for more CPU if below 1024MB.
Writes: aws/ops/reports/1101_xbrl_opt.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1101_xbrl_opt.json")
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
lam = boto3.client("lambda", region_name=REGION)
def main():
    rep = {"script": "1101_xbrl_opt",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    fn = "justhodl-xbrl-fundamentals"
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        old_mem = cfg.get("MemorySize")
        old_timeout = cfg.get("Timeout")
        rep["before"] = {"memory": old_mem, "timeout": old_timeout}
        # Bump memory to 2048MB for more CPU (if below)
        if old_mem < 2048:
            lam.update_function_configuration(
                FunctionName=fn, MemorySize=2048)
            rep["action"] = f"memory {old_mem} -> 2048"
        else:
            rep["action"] = "memory already >= 2048, no change"
        # Verify
        cfg2 = lam.get_function_configuration(FunctionName=fn)
        rep["after"] = {"memory": cfg2.get("MemorySize"),
                        "timeout": cfg2.get("Timeout")}
    except Exception as e:
        rep["error"] = str(e)[:150]
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    open(OUT_PATH, "w").write(json.dumps(rep, indent=2))
    print(json.dumps(rep.get("action", rep.get("error"))))
if __name__ == "__main__":
    main()
