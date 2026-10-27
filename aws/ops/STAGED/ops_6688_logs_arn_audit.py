"""ops 6688 - read-only audit for the AWS Health planned lifecycle event "CloudWatch Logs authorization change"
(effective 2027-04-26, action required): IAM statements that authorize Log GROUP actions must use a Log Group ARN
(arn:aws:logs:<region>:<account>:log-group:<name>); a Log Stream ARN (ending in :* or :log-stream:<name>) will stop
granting them.  Four actions stay stream-scoped and keep needing the stream ARN: PutLogEvents, GetLogEvents,
CreateLogStream, DeleteLogStream.  The event lists 2 affected resources in this account.

Method (no changes made):
  1. AWS Health: the event and its affected entities (needs a Business support plan - reported either way).
  2. Every customer policy in the account via iam:GetAccountAuthorizationDetails - role / user / group inline policies
     and customer-managed policies (default version).  AWS-managed policies are AWS's to update.
  3. Every Allow statement that grants a CloudWatch Logs action on a Log-Stream-format resource is SIMULATED with
     iam:SimulateCustomPolicy for each of its group-level actions against (a) the stream-format ARN it names today and
     (b) the plain Log Group ARN the service will check after the change.  Affected = allowed (a) and denied (b).
  4. For an affected role, iam:SimulatePrincipalPolicy against the plain group ARN says whether another policy of the
     same role already covers the action (no disruption) or the role would lose it.
  5. The exact patch: per affected statement, the equivalent Allow statement on the Log Group ARN (group-level actions
     only; the original statement stays for the four stream actions), as JSON for ops 6689.
No key involved.
"""
from fnmatch import fnmatchcase
from pathlib import Path
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
STREAM_ACTIONS = {"logs:putlogevents", "logs:getlogevents", "logs:createlogstream", "logs:deletelogstream"}
# group-scoped CloudWatch Logs actions used to expand a wildcard action (logs:*, logs:Describe*, *)
GROUP_ACTIONS = ["logs:AssociateKmsKey", "logs:CreateExportTask", "logs:CreateLogGroup", "logs:DeleteDataProtectionPolicy", "logs:DeleteLogGroup",
                 "logs:DeleteMetricFilter", "logs:DeleteRetentionPolicy", "logs:DeleteSubscriptionFilter", "logs:DescribeLogStreams",
                 "logs:DescribeMetricFilters", "logs:DescribeSubscriptionFilters", "logs:DisassociateKmsKey", "logs:FilterLogEvents",
                 "logs:GetDataProtectionPolicy", "logs:GetLogGroupFields", "logs:ListTagsForResource", "logs:ListTagsLogGroup",
                 "logs:PutDataProtectionPolicy", "logs:PutMetricFilter", "logs:PutRetentionPolicy", "logs:PutSubscriptionFilter",
                 "logs:StartLiveTail", "logs:StartQuery", "logs:TagLogGroup", "logs:TagResource", "logs:UntagLogGroup", "logs:UntagResource"]


def as_list(x):
    return x if isinstance(x, list) else ([] if x is None else [x])


def statements(doc):
    st = (doc or {}).get("Statement")
    return [s for s in as_list(st) if isinstance(s, dict)]


def stream_resource(res):
    """(group_pattern, today_arn, after_arn) for a Log-Stream-format logs ARN, else None.
    group_pattern keeps the statement's wildcards (the ARN the patch grants); the two ARNs are concrete for simulation."""
    m = re.match(r"^(arn:[^:]+:logs):([^:]*):([^:]*):(.*)$", res or "")
    if not m:
        return None
    pfx, region, account, rest = m.groups()
    reg = region if region and "*" not in region else REGION
    acct = account if account and "*" not in account else ACCOUNT
    if rest == "*":                                     # account-wide wildcard: also matches every group ARN - simulated, not assumed
        base = "%s:%s:%s:log-group:example-group" % (pfx, reg, acct)
        return None, base + ":*", base
    if not rest.startswith("log-group:"):
        return None
    body = rest[len("log-group:"):]
    if ":log-stream:" in body:
        name, stream = body.split(":log-stream:", 1)
        concrete = name.replace("*", "example").replace("?", "x")
        base = "%s:%s:%s:log-group:%s" % (pfx, reg, acct, concrete)
        return "%s:%s:%s:log-group:%s" % (pfx, region, account, name), base + ":log-stream:" + (stream.replace("*", "example") or "example"), base
    if body.endswith(":*"):
        name = body[:-2]
        concrete = name.replace("*", "example").replace("?", "x")
        base = "%s:%s:%s:log-group:%s" % (pfx, reg, acct, concrete)
        return "%s:%s:%s:log-group:%s" % (pfx, region, account, name), base + ":*", base
    return None                                         # already a Log Group ARN


def group_actions(stmt):
    """The group-level logs actions this statement grants (explicit, or wildcard-expanded)."""
    out = []
    for a in as_list(stmt.get("Action")):
        if not isinstance(a, str):
            continue
        al = a.lower()
        if al == "*" or (al.startswith("logs:") and ("*" in al or "?" in al)):
            out += [g for g in GROUP_ACTIONS if fnmatchcase(g.lower(), al)]
        elif al.startswith("logs:") and al not in STREAM_ACTIONS:
            out.append(a)
    seen, res = set(), []
    for a in out:
        if a.lower() not in seen:
            seen.add(a.lower())
            res.append(a)
    return res


def collect(iam):
    """[(kind, owner, policy_name, policy_arn, document)] for every customer policy in the account."""
    out = []
    pag = iam.get_paginator("get_account_authorization_details")
    for page in pag.paginate(Filter=["Role", "User", "Group", "LocalManagedPolicy"]):
        for role in page.get("RoleDetailList", []):
            for p in role.get("RolePolicyList", []):
                out.append(("role_inline", role["RoleName"], p["PolicyName"], role["Arn"], p["PolicyDocument"]))
        for user in page.get("UserDetailList", []):
            for p in user.get("UserPolicyList", []):
                out.append(("user_inline", user["UserName"], p["PolicyName"], user["Arn"], p["PolicyDocument"]))
        for group in page.get("GroupDetailList", []):
            for p in group.get("GroupPolicyList", []):
                out.append(("group_inline", group["GroupName"], p["PolicyName"], group["Arn"], p["PolicyDocument"]))
        for pol in page.get("Policies", []):
            doc = next((v["Document"] for v in pol.get("PolicyVersionList", []) if v.get("IsDefaultVersion")), None)
            if doc is not None:
                out.append(("managed", pol.get("Path", "/"), pol["PolicyName"], pol["Arn"], doc))
    return out


def simulate(iam, stmt_doc, actions, resources):
    """{(action, resource): decision} for one single-statement policy."""
    res = {}
    for i in range(0, len(actions), 20):
        r = iam.simulate_custom_policy(PolicyInputList=[json.dumps(stmt_doc)], ActionNames=actions[i:i + 20], ResourceArns=resources)
        for ev in r.get("EvaluationResults", []):
            for rr in ev.get("ResourceSpecificResults", []) or [{"EvalResourceName": ev.get("EvalResourceName"), "EvalResourceDecision": ev.get("EvalDecision")}]:
                res[(ev["EvalActionName"], rr["EvalResourceName"])] = rr["EvalResourceDecision"]
    return res


def main():
    iam = boto3.client("iam", config=CFG)
    with report("ops_6688_logs_arn_audit") as r:
        r.heading("ops 6688 - CloudWatch Logs authorization change (2027-04-26): which IAM statements grant Log Group actions on Log Stream ARNs (read-only)")
        r.section("1. AWS Health: the event and its affected entities")
        try:
            health = boto3.client("health", region_name="us-east-1", config=CFG)
            evs = health.describe_events(filter={"eventTypeCategories": ["scheduledChange"], "services": ["CLOUDWATCH", "LOGS"]}).get("events", [])
            for e in evs[:10]:
                r.log("event %s | %s | %s | start %s | status %s" % (e.get("eventTypeCode"), e.get("service"), e.get("arn"), e.get("startTime"), e.get("statusCode")))
                ents = health.describe_affected_entities(filter={"eventArns": [e["arn"]]}).get("entities", [])
                for en in ents[:20]:
                    r.log("   affected entity: %s (%s)" % (en.get("entityValue"), en.get("entityArn", "")[:120]))
            if not evs:
                r.log("no CLOUDWATCH/LOGS scheduledChange events returned")
        except Exception as e:  # noqa: BLE001
            r.log("AWS Health API not available to this account/role (%s) - the IAM scan below is the authority" % type(e).__name__)
        r.section("2. every customer policy in the account")
        pols = collect(iam)
        r.log("policies scanned: %d (%s)" % (len(pols), {k: sum(1 for p in pols if p[0] == k) for k in ("role_inline", "user_inline", "group_inline", "managed")}))
        r.section("3. Allow statements granting logs actions on Log-Stream-format resources, simulated today vs after")
        findings, candidates = [], 0
        for kind, owner, name, arn, doc in pols:
            for idx, st in enumerate(statements(doc)):
                if st.get("Effect") != "Allow":
                    continue
                if "NotAction" in st or "NotResource" in st:
                    if any(isinstance(a, str) and a.lower().startswith("logs:") for a in as_list(st.get("NotAction"))) or any(":logs:" in str(x) for x in as_list(st.get("NotResource"))):
                        r.warn("%s %s/%s statement %d uses NotAction/NotResource with logs - review by hand" % (kind, owner, name, idx))
                    continue
                acts = group_actions(st)
                if not acts:
                    continue
                streams = [(res, stream_resource(res)) for res in as_list(st.get("Resource")) if isinstance(res, str)]
                streams = [(res, s) for res, s in streams if s is not None]
                if not streams:
                    continue
                candidates += 1
                single = {"Version": "2012-10-17", "Statement": [dict(st)]}
                lost = []
                for res, (pattern, today, after) in streams:
                    dec = simulate(iam, single, acts, [today, after])
                    for a in acts:
                        t, aft = dec.get((a, today)), dec.get((a, after))
                        if t == "allowed" and aft != "allowed":
                            lost.append({"action": a, "resource": res, "group_pattern": pattern, "today": t, "after": aft, "after_arn": after})
                if lost:
                    findings.append({"kind": kind, "owner": owner, "policy": name, "arn": arn, "statement_index": idx, "sid": st.get("Sid"),
                                     "actions": sorted({x["action"] for x in lost}), "group_resources": sorted({x["group_pattern"] for x in lost if x["group_pattern"]}),
                                     "lost": lost})
                    r.warn("AFFECTED %s %s / %s statement %d (Sid %s): %s on %s" % (kind, owner, name, idx, st.get("Sid"), sorted({x["action"] for x in lost}), sorted({x["resource"] for x in lost})))
                else:
                    r.log("ok %s %s / %s statement %d (Sid %s): %d group actions still granted after the change" % (kind, owner, name, idx, st.get("Sid"), len(acts)))
        r.log("candidate statements: %d; affected: %d" % (candidates, len(findings)))
        r.section("4. would the role lose the access (no other policy of the role covers it)?")
        for f in findings:
            if f["kind"] != "role_inline" and f["kind"] != "managed":
                continue
            roles = [f["owner"]] if f["kind"] == "role_inline" else []
            if f["kind"] == "managed":
                try:
                    roles = [x["RoleName"] for x in iam.list_entities_for_policy(PolicyArn=f["arn"], EntityFilter="Role").get("PolicyRoles", [])]
                except Exception as e:  # noqa: BLE001
                    r.warn("list_entities_for_policy %s: %s" % (f["policy"], type(e).__name__))
            f["roles"] = roles
            for role in roles:
                role_arn = "arn:aws:iam::%s:role/%s" % (ACCOUNT, role)
                try:
                    after_arns = sorted({x["after_arn"] for x in f["lost"]})
                    rs = iam.simulate_principal_policy(PolicySourceArn=role_arn, ActionNames=f["actions"], ResourceArns=after_arns).get("EvaluationResults", [])
                    verdict = {}
                    for ev in rs:
                        for rr in ev.get("ResourceSpecificResults", []) or []:
                            verdict["%s @ %s" % (ev["EvalActionName"], rr["EvalResourceName"].split(":log-group:")[-1])] = rr["EvalResourceDecision"]
                    would_break = sorted(k for k, v in verdict.items() if v != "allowed")
                    f.setdefault("principal_checks", {})[role] = {"would_lose": would_break, "still_allowed_elsewhere": sorted(k for k, v in verdict.items() if v == "allowed")}
                    r.log("%s (%s / %s): would lose %d action-resource grants %s; covered elsewhere %d" % (
                        role, f["policy"], f["sid"], len(would_break), would_break[:6], sum(1 for v in verdict.values() if v == "allowed")))
                except Exception as e:  # noqa: BLE001
                    r.warn("simulate_principal_policy %s: %s %s" % (role, type(e).__name__, str(e)[:160]))
        r.section("5. the patch (ops 6689): add these statements, keep the originals")
        plan = []
        for f in findings:
            sid = (re.sub(r"[^A-Za-z0-9]", "", f.get("sid") or "Statement%d" % f["statement_index"]) + "LogGroupArn")[:100]
            plan.append({"kind": f["kind"], "owner": f["owner"], "policy": f["policy"], "arn": f["arn"], "after_statement_index": f["statement_index"],
                         "add": {"Sid": sid, "Effect": "Allow", "Action": f["actions"], "Resource": f["group_resources"]}})
        for p in plan:
            r.log("%s %s / %s: + %s" % (p["kind"], p["owner"], p["policy"], json.dumps(p["add"])))
        r.log("PLAN_JSON " + json.dumps(plan))
        r.ok("ops 6688 complete (read-only): %d affected statement(s)" % len(findings))


if __name__ == "__main__":
    main()
    sys.exit(0)
