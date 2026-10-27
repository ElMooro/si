"""ops 6691 - read-only: the AWS-managed policies attached in this account, through the same Log-Stream-ARN audit as ops
6689 (CloudWatch Logs authorization change, 2027-04-26).  ops 6689 fixed the one affected customer statement
(MacroFinancialIntelligenceRole) and re-audited 121 customer policies to 0; the AWS Health event counts 2 affected
resources and its entity list needs a Business support plan, so this closes the remaining place a grant could break:
AWS-managed policies are AWS's to update before the change, but if one attached here is affected, the roles that rely
on it are named.  Nothing is written.  No key involved.
"""
from pathlib import Path
import sys

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops" / "STAGED"))
from ops_report import report  # noqa: E402
import ops_6688_logs_arn_audit as audit  # noqa: E402
import ops_6689_logs_group_arn_fix as fix  # noqa: E402


def main():
    iam = boto3.client("iam", config=audit.CFG)
    sim = lambda doc, acts, res: audit.simulate(iam, doc, acts, res)  # noqa: E731
    with report("ops_6691_logs_arn_aws_managed") as r:
        r.heading("ops 6691 - AWS-managed policies attached in this account vs the CloudWatch Logs authorization change (read-only)")
        pols = []
        for page in iam.get_paginator("get_account_authorization_details").paginate(Filter=["AWSManagedPolicy"]):
            for p in page.get("Policies", []):
                doc = next((v["Document"] for v in p.get("PolicyVersionList", []) if v.get("IsDefaultVersion")), None)
                if doc is not None and (p.get("AttachmentCount") or 0) > 0:
                    pols.append((p["PolicyName"], p["Arn"], p.get("AttachmentCount"), doc))
        r.log("attached AWS-managed policies: %d" % len(pols))
        affected = 0
        for name, arn, n, doc in sorted(pols):
            _, adds, findings = fix.plan_document(sim, doc, "aws-managed %s (attached %d)" % (name, n))
            fix.log_findings(r, findings)
            if adds:
                affected += 1
                ents = iam.list_entities_for_policy(PolicyArn=arn)
                r.warn("%s: AWS updates this policy; attached to roles %s users %s" % (
                    name, [x["RoleName"] for x in ents.get("PolicyRoles", [])], [x["UserName"] for x in ents.get("PolicyUsers", [])]))
        r.log("affected attached AWS-managed policies: %d" % affected)
        r.ok("ops 6691 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
