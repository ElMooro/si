"""ops 1119 — Update SM definition with correct Lambda names, run final smoke test."""
import json, os, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
SM_NAME = "jhk-portfolio-orchestration"
SFN_ROLE = "jhk-step-fn-exec-role"

sfn = boto3.client("stepfunctions", region_name=REGION)


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        sfn_role_arn = f"arn:aws:iam::{ACCOUNT}:role/{SFN_ROLE}"
        definition = json.load(open(os.path.join(REPO_ROOT,
                                                  "aws/step-functions/portfolio-orchestration.json")))
        sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{SM_NAME}"
        sfn.update_state_machine(stateMachineArn=sm_arn,
                                  definition=json.dumps(definition),
                                  roleArn=sfn_role_arn)
        rpt["sm"] = "UPDATED"

        # Smoke
        ex = sfn.start_execution(stateMachineArn=sm_arn,
                                  name=f"smoke-{int(time.time())}", input="{}")
        rpt["smoke_arn"] = ex["executionArn"]
        for _ in range(60):
            time.sleep(5)
            s = sfn.describe_execution(executionArn=ex["executionArn"])
            if s["status"] != "RUNNING":
                rpt["smoke_status"] = s["status"]
                if s.get("output"):
                    try:
                        out = json.loads(s["output"])
                        rpt["snapshot_ok"] = out.get("snapshot", {}).get("StatusCode") == 200
                        rpt["risk_ok"] = out.get("risk", {}).get("StatusCode") == 200
                        rpt["hedge_ok"] = isinstance(out.get("hedge"), list) and len(out.get("hedge", [])) >= 2
                        rpt["pm_ok"] = out.get("pm", {}).get("StatusCode") == 200
                        rpt["cro_ok"] = out.get("cro", {}).get("StatusCode") == 200
                        rpt["chain_error"] = out.get("error")
                    except Exception:
                        rpt["smoke_output"] = str(s["output"])[:400]
                if s.get("cause"): rpt["smoke_cause"] = s["cause"][:500]
                break
        else:
            rpt["smoke_status"] = "POLL_TIMEOUT"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:400]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1119.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p,"w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k!="traceback"}, indent=2, default=str)[:2000])


if __name__ == "__main__":
    main()
