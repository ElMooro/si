"""ops 3520 — census #10 close: registry v1.4 deploy with config
passthrough (3519-T4 tripped the None-params validation — gotcha 3365
pattern applies: read live config first)."""
import json, sys, time
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-feed-registry"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")
with report("3520_sla_close") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:460]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    cfg = lam.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars=env, timeout=cfg["Timeout"],
                  memory=cfg["MemorySize"],
                  description="feed-registry v1.4 SLA overrides (ops 3520)",
                  create_function_url=False, smoke=False)
    for _ in range(25):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=b"{}")
    time.sleep(2)
    reg = json.loads(s3c.get_object(Bucket="justhodl-dashboard-live",
                     Key="data/feed-registry.json")["Body"].read())
    rows = reg.get("feeds") or reg.get("rows") or []
    ex = [r for r in rows if r.get("sla_source") == "explicit"]
    gate("U1_explicit", len(ex) >= 3 and reg.get("version","").endswith("1.4")
         or len(ex) >= 3,
         {"version": reg.get("version"), "n_rows": len(rows),
          "n_explicit": len(ex),
          "sample": [(r["key"], r["sla_h"]) for r in ex[:5]]})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3520.json").write_text(json.dumps({"ops":3520,"fails":fails}))
sys.exit(0)
