"""ops 6693 - read-only: who uses the six roles that hold the four AWS-managed policies ops 6691 found with Log-Stream-ARN
statements (CloudWatch Logs authorization change, 2027-04-26) - AWSServiceRoleForECS, AmazonSageMakerServiceCatalog-
ProductsLaunchRole, three AmazonSageMaker-ExecutionRole-20250528*, AWSServiceRoleForAppRunner.  For each: the trust
policy's principals, RoleLastUsed, and whether any Lambda function in the account runs on it.  AWS maintains those
policies; this states whether any JustHodl engine depends on them.  Nothing is written.  No key involved.
"""
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
POLICIES = ("AmazonECSServiceRolePolicy", "AmazonSageMakerAdmin-ServiceCatalogProductsServiceRolePolicy",
            "AmazonSageMakerCanvasDataPrepFullAccess", "AppRunnerServiceRolePolicy")


def main():
    iam = boto3.client("iam", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    with report("ops_6693_logs_arn_managed_roles") as r:
        r.heading("ops 6693 - the roles behind the four AWS-managed policies with Log-Stream-ARN statements (read-only)")
        roles = {}
        for pol in POLICIES:
            arn = ("arn:aws:iam::aws:policy/aws-service-role/%s" if pol in ("AmazonECSServiceRolePolicy", "AppRunnerServiceRolePolicy")
                   else "arn:aws:iam::aws:policy/%s") % pol
            try:
                ents = iam.list_entities_for_policy(PolicyArn=arn)
            except Exception as e:  # noqa: BLE001 - the service-role path differs per policy; try the plain one
                try:
                    arn = "arn:aws:iam::aws:policy/%s" % pol
                    ents = iam.list_entities_for_policy(PolicyArn=arn)
                except Exception as e2:  # noqa: BLE001
                    r.warn("%s: %s / %s" % (pol, type(e).__name__, type(e2).__name__))
                    continue
            for x in ents.get("PolicyRoles", []):
                roles.setdefault(x["RoleName"], []).append(pol)
        fn_roles, n = {}, 0
        for page in lam.get_paginator("list_functions").paginate():
            for f in page.get("Functions", []):
                n += 1
                fn_roles.setdefault(f["Role"].split("/")[-1], []).append(f["FunctionName"])
        r.log("lambda functions: %d on %d distinct roles" % (n, len(fn_roles)))
        used = 0
        for name, pols in sorted(roles.items()):
            role = iam.get_role(RoleName=name)["Role"]
            trust = sorted({json.dumps(s.get("Principal"), sort_keys=True) for s in (role.get("AssumeRolePolicyDocument") or {}).get("Statement", [])})
            last = role.get("RoleLastUsed") or {}
            fns = fn_roles.get(name, [])
            used += bool(fns)
            (r.warn if fns else r.log)("%s (%s): trust %s | last used %s %s | Lambda functions on it: %d %s" % (
                name, ", ".join(pols), trust, last.get("LastUsedDate"), last.get("Region") or "", len(fns), fns[:8]))
        r.log("roles: %d; used by a Lambda function: %d" % (len(roles), used))
        r.ok("ops 6693 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
