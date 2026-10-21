"""ops 6579 - finish the runner seal: the group justhodl-runner-ecr was at its 10-managed-policy limit when ops 6578
tried to attach justhodl-deny-runner-escape.  Free a slot by detaching a policy the sealed deny makes pointless
(justhodl-runner-training-bursts: SageMaker training-job rights the runner can no longer use; or hosting-v2 if v3 is
attached), attach the escape seal, prove it by simulation.  Read-only otherwise."""
from pathlib import Path
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
GROUP, USER = "justhodl-runner-ecr", "github-actions-justhodl"
ESCAPE = "arn:aws:iam::%s:policy/justhodl-deny-runner-escape" % ACCOUNT
COMPUTE = "arn:aws:iam::%s:policy/justhodl-deny-rogue-compute" % ACCOUNT
CANDIDATES = ["justhodl-runner-training-bursts", "justhodl-runner-hosting-v2", "justhodl-runner-hosting"]
CFG = Config(retries={"max_attempts": 10, "mode": "standard"})


def main():
    iam = boto3.client("iam", region_name=REGION, config=CFG)
    with report("ops_6579_runner_escape_seal") as r:
        r.heading("ops 6579 - attach the runner escape seal (group at the policy limit)")
        attached = {p["PolicyName"]: p["PolicyArn"] for p in iam.list_attached_group_policies(GroupName=GROUP)["AttachedPolicies"]}
        r.log("group policies (%d): %s" % (len(attached), sorted(attached)))
        if "justhodl-deny-runner-escape" not in attached:
            if len(attached) >= 10:
                victim = next((n for n in CANDIDATES if n in attached and not (n == "justhodl-runner-hosting-v2" and "justhodl-runner-hosting-v3" not in attached)), None)
                if not victim:
                    r.fail("no policy safe to detach; the seal stays unattached")
                    raise SystemExit(1)
                iam.detach_group_policy(GroupName=GROUP, PolicyArn=attached[victim])
                r.log("detached %s (rights the sealed deny makes pointless)" % victim)
            iam.attach_group_policy(GroupName=GROUP, PolicyArn=ESCAPE)
            r.ok("attached justhodl-deny-runner-escape")
            time.sleep(20)
        denied = ["sts:AssumeRole", "iam:AddUserToGroup", "iam:RemoveUserFromGroup", "iam:CreateGroup", "iam:UpdateAssumeRolePolicy", "sagemaker:CreateTrainingJob"]
        allowed = ["lambda:UpdateFunctionCode", "s3:PutObject", "events:PutRule", "iam:PutRolePolicy", "iam:AttachGroupPolicy", "iam:CreateRole", "s3:CreateJob", "lambda:PutFunctionConcurrency"]
        res = iam.simulate_principal_policy(PolicySourceArn="arn:aws:iam::%s:user/%s" % (ACCOUNT, USER), ActionNames=denied + allowed,
                                            ContextEntries=[{"ContextKeyName": "iam:PolicyARN", "ContextKeyValues": [COMPUTE], "ContextKeyType": "string"}])
        by = {x["EvalActionName"]: x["EvalDecision"] for x in res["EvaluationResults"]}
        bad = [(a, by[a]) for a in denied if by.get(a) != "explicitDeny"] + [(a, by[a]) for a in allowed if by.get(a) == "explicitDeny"]
        r.log("runner: denied %s; allowed-still %s" % ([by[a] for a in denied], [by[a] for a in allowed]))
        r.kv(group_policies=len(iam.list_attached_group_policies(GroupName=GROUP)["AttachedPolicies"]), unexpected=len(bad))
        if bad:
            r.fail("unexpected: %s" % bad)
            raise SystemExit(1)
        r.ok("runner sealed: no role assumption, no group escape, no trust edits; deploys and ops rights intact")


if __name__ == "__main__":
    main()
