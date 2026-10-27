"""ops 6689 - CloudWatch Logs authorization change (AWS Health planned lifecycle event, effective 2027-04-26 00:00 UTC-7,
action required, "Affected resources: 2").  After the change a Log Stream ARN (ending in :* or :log-stream:<name>) no
longer grants Log GROUP actions; only PutLogEvents / GetLogEvents / CreateLogStream / DeleteLogStream stay stream-scoped.
AWS's recommendation: next to every such Allow statement add an equivalent statement on the Log Group ARN
(arn:aws:logs:<region>:<account>:log-group:<name>) and keep the original.  Khalid pasted the event on 2026-10-27.

ops 6688 (read-only) found 1 affected statement in 121 customer policies.  It simulated each statement exactly as written,
so a statement carrying a Condition evaluates as denied both today and after (no request context) and was counted as fine;
the event's own count is 2.  This script:

1. Re-audits every customer IAM policy (role / user / group inline, customer-managed default version) with each candidate
   statement's Condition REMOVED for the simulation - the change is about ARN matching; the Condition is copied unchanged
   into the companion.  Allow and Deny: a Deny on a Log Stream ARN would silently stop denying a group action after the
   change (the notice covers Allow; the inverse risk is checked too).  Also reads the CloudWatch Logs resource policies
   (account and log-group scope) and the policies of the account's CloudWatch Logs VPC endpoints.
2. For every affected statement not already covered by a statement of the same policy with the same Effect and Condition
   (which is also how a re-run stays idempotent) inserts, right after it: the same Effect, the statement's own Action
   entries that carry a lost group action (wildcards kept as written - an equivalent statement, not an expansion), the Log
   Group ARN(s) of its stream resources (wildcards kept), the same Condition (and Principal, for a resource policy).  The
   original statement is kept unchanged.  Quotas are checked before any write (role inline aggregate 10,240 / user 2,048 /
   group 5,120 characters, managed 6,144 and 5 versions, logs resource policy 5,120); every touched document is backed up
   to the private ops bucket first.
3. Reads every patched document back and proves: the original statements are unchanged and in order, every lost grant is
   granted on the Log Group ARN, and each companion stays inside its original (actions, log groups, Condition).
4. Re-audits the whole account: 0 affected statements may remain.
No key involved.
"""
from fnmatch import fnmatchcase
from pathlib import Path
import copy
import json
import re
import sys
import time

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops" / "STAGED"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import cadence_apply as ca  # noqa: E402
import ops_6688_logs_arn_audit as audit  # noqa: E402

REGION, ACCOUNT, CFG = audit.REGION, audit.ACCOUNT, audit.CFG
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
LIMITS = {"role_inline": 10240, "user_inline": 2048, "group_inline": 5120, "managed": 6144, "logs_resource": 5120}
HIT = {"Allow": "allowed", "Deny": "explicitDeny"}
INLINE = {"role_inline": ("RoleName", "list_role_policies", "get_role_policy", "put_role_policy"),
          "user_inline": ("UserName", "list_user_policies", "get_user_policy", "put_user_policy"),
          "group_inline": ("GroupName", "list_group_policies", "get_group_policy", "put_group_policy")}


# ------------------------------------------------------------------------------------------------ helpers --
def canon(x):
    return None if x is None else json.dumps(x, sort_keys=True, separators=(",", ":"))


def size(doc):
    return len(json.dumps(doc, separators=(",", ":")))


def bare(st):
    return {k: v for k, v in st.items() if k != "Condition"}


def policy(sts):
    return {"Version": "2012-10-17", "Statement": sts}


def one(xs):
    return xs[0] if len(xs) == 1 else xs


def wild(pattern, value, case=True):
    p = pattern.replace("[", "[[]")
    return fnmatchcase(value, p) if case else fnmatchcase(value.lower(), p.lower())


def local_simulate(doc, actions, resources):
    """IAM decision of `doc` alone for each (action, resource), Principal ignored: * and ? match any character (':' and
    '/' included), actions case-insensitive, resources case-sensitive, an explicit Deny wins.  Used for resource-based
    policies, which iam:SimulateCustomPolicy does not evaluate on their own."""
    out = {}
    for a in actions:
        for res in resources:
            dec = "implicitDeny"
            for s in audit.statements(doc):
                if "NotAction" in s or "NotResource" in s:
                    continue
                am = any(isinstance(x, str) and wild(x, a, case=False) for x in audit.as_list(s.get("Action")))
                rm = any(isinstance(x, str) and wild(x, res) for x in audit.as_list(s.get("Resource")))
                if am and rm:
                    if s.get("Effect") == "Deny":
                        dec = "explicitDeny"
                        break
                    dec = "allowed"
            out[(a, res)] = dec
    return out


def mentions_logs(st):
    blob = canon({k: st.get(k) for k in ("Action", "NotAction", "Resource", "NotResource")}).lower()
    return "logs:" in blob or ":logs:" in blob


# ------------------------------------------------------------------------------------------------- the plan --
def classify(sim, st):
    """None when the statement carries no group-level logs action on a Log-Stream-format resource; else
    {"rows": [(action, resource, today, after)], "lost": [(action, after_arn, group_pattern)], "unresolved": rows}
    - simulated with the Condition removed.  `lost` = matched today, not matched after the change."""
    acts = audit.group_actions(st)
    streams = [(res, audit.stream_resource(res)) for res in audit.as_list(st.get("Resource")) if isinstance(res, str)]
    streams = [(res, s) for res, s in streams if s is not None]
    if not acts or not streams:
        return None
    hit, single, rows, lost = HIT[st["Effect"]], policy([bare(st)]), [], []
    for res, (pattern, today, after) in streams:
        dec = sim(single, acts, [today, after])
        for a in acts:
            t, af = dec.get((a, today)), dec.get((a, after))
            rows.append((a, res, t, af))
            if t == hit and af != hit:
                lost.append((a, after, pattern))
    return {"rows": rows, "lost": lost, "unresolved": [x for x in rows if x[2] != hit]}


def peers_of(sts, st, skip=None):
    return [bare(s) for s in sts if s is not skip and s.get("Effect") == st.get("Effect")
            and canon(s.get("Condition")) == canon(st.get("Condition"))]


def covered_in_policy(sim, sts, st, lost):
    """The (action, after_arn) pairs another statement of the same document, with the same Effect and Condition, already
    matches (a companion added by an earlier run, or a group statement the author wrote)."""
    peers = peers_of(sts, st, skip=st)
    if not peers or not lost:
        return set()
    dec = sim(policy(peers), sorted({a for a, _, _ in lost}), sorted({arn for _, arn, _ in lost}))
    return {k for k, d in dec.items() if d == HIT[st["Effect"]]}


def companion_actions(st, lost_actions):
    lost = {a.lower() for a in lost_actions}
    out = []
    for a in audit.as_list(st.get("Action")):
        if not isinstance(a, str):
            continue
        is_wild = "*" in a or "?" in a
        if ((is_wild and any(wild(a, x, case=False) for x in lost)) or (not is_wild and a.lower() in lost)) and a not in out:
            out.append(a)
    return out


def companion_sid(st, idx, taken):
    base = (re.sub(r"[^A-Za-z0-9]", "", st.get("Sid") or "") or "Statement%d" % idx)[:80] + "LogGroupArn"
    sid, n = base, 2
    while sid in taken:
        sid, n = "%s%d" % (base, n), n + 1
    return sid


def plan_document(sim, doc, label):
    """(new_doc or None, additions, findings) for one policy document."""
    sts = audit.statements(doc)
    taken = {s.get("Sid") for s in sts if s.get("Sid")}
    out, additions, findings = [], [], []
    for idx, st in enumerate(sts):
        out.append(st)
        eff = st.get("Effect")
        if eff not in HIT:
            continue
        if "NotAction" in st or "NotResource" in st:
            if mentions_logs(st):
                findings.append({"label": label, "idx": idx, "sid": st.get("Sid"), "effect": eff, "condition": "Condition" in st,
                                 "rows": [], "status": "REVIEW (NotAction/NotResource naming logs)"})
            continue
        c = classify(sim, st)
        if c is None:
            continue
        f = {"label": label, "idx": idx, "sid": st.get("Sid"), "effect": eff, "condition": "Condition" in st, "rows": c["rows"]}
        findings.append(f)
        if not c["lost"]:
            f["status"] = "REVIEW (not matched today even without its Condition)" if c["unresolved"] else "unaffected (still matched after)"
            continue
        done = covered_in_policy(sim, sts, st, c["lost"])
        need = [x for x in c["lost"] if (x[0], x[1]) not in done]
        if not need:
            f["status"] = "covered (a statement of the same policy, same Effect and Condition, matches the Log Group ARN)"
            continue
        actions, resources = companion_actions(st, [a for a, _, _ in need]), sorted({p for _, _, p in need if p})
        if not actions or not resources:
            f["status"] = "REVIEW (no companion derivable)"
            continue
        comp = {"Sid": companion_sid(st, idx, taken), "Effect": eff}
        if "Principal" in st:
            comp["Principal"] = copy.deepcopy(st["Principal"])
        comp.update(Action=one(actions), Resource=one(resources))
        if "Condition" in st:
            comp["Condition"] = copy.deepcopy(st["Condition"])
        taken.add(comp["Sid"])
        out.append(comp)
        f["status"] = "AFFECTED -> + %s" % comp["Sid"]
        additions.append({"sid": comp["Sid"], "after": st.get("Sid") or idx, "statement": comp, "original": st,
                          "lost": sorted({(a, arn) for a, arn, _ in need})})
    if not additions:
        return None, [], findings
    new_doc = dict(doc)
    new_doc["Statement"] = out
    return new_doc, additions, findings


def prove(sim, before, after, additions):
    """[] when the original statements are unchanged and in order, every lost grant is matched on the Log Group ARN by the
    patched document (Condition removed), and each companion stays inside its original's actions, groups and Condition."""
    errs, b, a = [], audit.statements(before), audit.statements(after)
    added = {x["sid"] for x in additions}
    if [canon(s) for s in a if s.get("Sid") not in added] != [canon(s) for s in b]:
        errs.append("original statements changed")
    for x in additions:
        comp, orig = next((s for s in a if s.get("Sid") == x["sid"]), None), x["original"]
        if comp is None:
            errs.append("%s missing after the write" % x["sid"])
            continue
        if canon(comp.get("Condition")) != canon(orig.get("Condition")) or canon(comp.get("Principal")) != canon(orig.get("Principal")):
            errs.append("%s Condition/Principal differs from the original" % x["sid"])
        if not set(audit.as_list(comp.get("Action"))) <= set(audit.as_list(orig.get("Action"))):
            errs.append("%s grants actions outside the original" % x["sid"])
        groups = {sr[0] for sr in (audit.stream_resource(r) for r in audit.as_list(orig.get("Resource")) if isinstance(r, str)) if sr and sr[0]}
        if not set(audit.as_list(comp.get("Resource"))) <= groups:
            errs.append("%s names resources outside the original's log groups" % x["sid"])
        dec = sim(policy(peers_of(a, comp)), sorted({p for p, _ in x["lost"]}), sorted({q for _, q in x["lost"]}))
        miss = [k for k in x["lost"] if dec.get(tuple(k)) != HIT[comp["Effect"]]]
        if miss:
            errs.append("%s does not match %s" % (x["sid"], miss[:3]))
    return errs


# ------------------------------------------------------------------------------------------------- writers --
def settle(read, sids, tries=6, wait=5):
    """read() until the document carries every added Sid - IAM read-after-write is eventually consistent."""
    doc = read()
    for _ in range(tries):
        if {s.get("Sid") for s in audit.statements(doc)} >= set(sids):
            break
        time.sleep(wait)
        doc = read()
    return doc


def write_inline(iam, kind, owner, name, new_doc, sids):
    key, lister, getter, putter = INLINE[kind]
    names = []
    for page in iam.get_paginator(lister).paginate(**{key: owner}):
        names += page.get("PolicyNames", [])
    total = sum(size(new_doc) if n == name else size(getattr(iam, getter)(**{key: owner, "PolicyName": n})["PolicyDocument"]) for n in names)
    if total > LIMITS[kind]:
        return None, "inline aggregate would be %d > %d characters" % (total, LIMITS[kind])
    getattr(iam, putter)(**{key: owner, "PolicyName": name, "PolicyDocument": json.dumps(new_doc)})
    return settle(lambda: getattr(iam, getter)(**{key: owner, "PolicyName": name})["PolicyDocument"], sids), "inline aggregate %d / %d" % (total, LIMITS[kind])


def write_managed(iam, arn, name, new_doc, store, sids):
    if size(new_doc) > LIMITS["managed"]:
        return None, "%d > %d characters" % (size(new_doc), LIMITS["managed"])
    versions = []
    for page in iam.get_paginator("list_policy_versions").paginate(PolicyArn=arn):
        versions += page.get("Versions", [])
    note = "%d versions" % len(versions)
    if len(versions) >= 5:
        oldest = min((v for v in versions if not v.get("IsDefaultVersion")), key=lambda v: v["CreateDate"])
        old = iam.get_policy_version(PolicyArn=arn, VersionId=oldest["VersionId"])["PolicyVersion"]["Document"]
        store.put(ca.safe_key("managed", name, oldest["VersionId"] + ".json"), {"policy_arn": arn, "version": oldest["VersionId"], "document": old})
        iam.delete_policy_version(PolicyArn=arn, VersionId=oldest["VersionId"])
        note += ", oldest non-default %s backed up and deleted" % oldest["VersionId"]
    vid = iam.create_policy_version(PolicyArn=arn, PolicyDocument=json.dumps(new_doc), SetAsDefault=True)["PolicyVersion"]["VersionId"]
    return settle(lambda: iam.get_policy_version(PolicyArn=arn, VersionId=vid)["PolicyVersion"]["Document"], sids), note + "; new default %s" % vid


def iam_plans(iam, sim):
    plans = []
    for kind, owner, name, arn, doc in audit.collect(iam):
        new_doc, adds, findings = plan_document(sim, doc, "%s %s / %s" % (kind, owner, name))
        plans.append({"kind": kind, "owner": owner, "name": name, "arn": arn, "doc": doc, "new": new_doc, "adds": adds, "findings": findings})
    return plans


def status_word(f):
    return f["status"].split(" (")[0].split(" ->")[0]


def log_findings(r, findings):
    for f in findings:
        dec = "; ".join("%s@%s %s->%s" % (a.split(":")[-1], res.split(":log-group:")[-1] if ":log-group:" in res else res, t, af)
                        for a, res, t, af in f["rows"][:6])
        line = "%s stmt %s (Sid %s, %s%s): %s%s" % (f["label"], f["idx"], f["sid"], f["effect"], ", Condition" if f["condition"] else "",
                                                    f["status"], (" | " + dec) if dec else "")
        (r.warn if status_word(f) in ("AFFECTED", "REVIEW") else r.log)(line)


# -------------------------------------------------------------------------------------------------- main --
def main():
    iam = boto3.client("iam", config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    ec2 = boto3.client("ec2", region_name=REGION, config=CFG)
    sim = lambda doc, acts, res: audit.simulate(iam, doc, acts, res)  # noqa: E731
    stamp = ca.stamp()
    store = ca.Store(s3, PRIVATE, "backups/iam-logs-group-arn/%s" % stamp)
    result = {"patched": [], "skipped": [], "review": []}
    with report("ops_6689_logs_group_arn_fix") as r:
        r.heading("ops 6689 - CloudWatch Logs authorization change: Log Group ARN statements next to every affected statement")
        r.section("1. stricter re-audit of every customer IAM policy (Condition removed for the ARN-matching simulation)")
        plans = iam_plans(iam, sim)
        findings = [f for p in plans for f in p["findings"]]
        r.log("customer policies: %d; statements naming a Log Stream ARN with a group-level logs action: %d (with a Condition: %d; Deny: %d)" % (
            len(plans), len(findings), sum(1 for f in findings if f["condition"]), sum(1 for f in findings if f["effect"] == "Deny")))
        log_findings(r, findings)
        work = [p for p in plans if p["adds"]]
        r.log("affected IAM policies: %d (%d statements)" % (len(work), sum(len(p["adds"]) for p in work)))
        result["review"] += ["%s stmt %s" % (f["label"], f["idx"]) for f in findings if status_word(f) == "REVIEW"]

        r.section("1b. resource-based policies: CloudWatch Logs resource policies, CloudWatch Logs VPC endpoints")
        rps = []
        try:
            token = None
            while True:
                resp = logs.describe_resource_policies(**({"nextToken": token} if token else {}))
                rps += resp.get("resourcePolicies", [])
                token = resp.get("nextToken")
                if not token:
                    break
            r.log("account-scope logs resource policies: %d" % len(rps))
            try:
                more = [x for x in logs.describe_resource_policies(policyScope="RESOURCE").get("resourcePolicies", []) if x.get("resourceArn")]
                rps += more
                r.log("log-group-scope logs resource policies: %d" % len(more))
            except Exception as e:  # noqa: BLE001
                r.log("log-group-scope logs resource policies: not listable account-wide (%s) - this system never created one" % type(e).__name__)
        except Exception as e:  # noqa: BLE001
            r.warn("describe_resource_policies: %s %s" % (type(e).__name__, str(e)[:200]))
        rp_work = []
        for rp in rps:
            doc = json.loads(rp.get("policyDocument") or "{}")
            label = "logs resource policy %s%s" % (rp.get("policyName"), (" @ " + rp["resourceArn"]) if rp.get("resourceArn") else "")
            new_doc, adds, fnd = plan_document(local_simulate, doc, label)
            r.log("%s: %d statements, %d naming a Log Stream ARN with a group action, %d affected" % (label, len(audit.statements(doc)), len(fnd), len(adds)))
            log_findings(r, [f for f in fnd if status_word(f) in ("AFFECTED", "REVIEW")])
            if adds:
                rp_work.append((rp, doc, new_doc, adds))
        vpce_work = []
        try:
            eps = ec2.describe_vpc_endpoints(Filters=[{"Name": "service-name", "Values": ["com.amazonaws.%s.logs" % REGION]}]).get("VpcEndpoints", [])
            r.log("CloudWatch Logs VPC endpoints: %d" % len(eps))
            for ep in eps:
                doc = json.loads(ep.get("PolicyDocument") or "{}")
                new_doc, adds, fnd = plan_document(local_simulate, doc, "vpc endpoint %s" % ep["VpcEndpointId"])
                log_findings(r, fnd)
                if adds:
                    vpce_work.append((ep, doc, new_doc, adds))
        except Exception as e:  # noqa: BLE001
            r.warn("describe_vpc_endpoints: %s %s" % (type(e).__name__, str(e)[:200]))

        r.section("2. would a principal actually lose access without the patch (iam:SimulatePrincipalPolicy, informational)")
        for p in work:
            principals = [p["arn"]] if p["kind"] in ("role_inline", "user_inline") else []
            if p["kind"] == "managed":
                try:
                    ents = iam.list_entities_for_policy(PolicyArn=p["arn"])
                    principals = (["arn:aws:iam::%s:role/%s" % (ACCOUNT, x["RoleName"]) for x in ents.get("PolicyRoles", [])]
                                  + ["arn:aws:iam::%s:user/%s" % (ACCOUNT, x["UserName"]) for x in ents.get("PolicyUsers", [])])
                except Exception as e:  # noqa: BLE001
                    r.warn("list_entities_for_policy %s: %s" % (p["name"], type(e).__name__))
            lost = sorted({tuple(k) for a in p["adds"] for k in a["lost"]})
            for who in principals:
                try:
                    rs = iam.simulate_principal_policy(PolicySourceArn=who, ActionNames=sorted({a for a, _ in lost}),
                                                       ResourceArns=sorted({q for _, q in lost})).get("EvaluationResults", [])
                    allowed = sum(1 for ev in rs for rr in (ev.get("ResourceSpecificResults") or [])
                                  if (ev["EvalActionName"], rr["EvalResourceName"]) in lost and rr["EvalResourceDecision"] == "allowed")
                    r.log("%s: %d of %d lost grant(s) already allowed by another of its policies (would lose %d)" % (
                        who.split(":")[-1], allowed, len(lost), len(lost) - allowed))
                except Exception as e:  # noqa: BLE001
                    r.warn("simulate_principal_policy %s: %s %s" % (who, type(e).__name__, str(e)[:160]))

        r.section("3. patch: back up, check quotas, write, read back, prove")
        for p in work:
            label = "%s %s / %s" % (p["kind"], p["owner"], p["name"])
            store.put(ca.safe_key(p["kind"], p["owner"], p["name"] + ".json"),
                      {"policy": label, "arn": p["arn"], "document": p["doc"], "patched": p["new"],
                       "additions": [{k: v for k, v in a.items() if k != "original"} for a in p["adds"]]})
            sids = [a["sid"] for a in p["adds"]]
            if p["kind"] in INLINE:
                back, note = write_inline(iam, p["kind"], p["owner"], p["name"], p["new"], sids)
            else:
                back, note = write_managed(iam, p["arn"], p["name"], p["new"], store, sids)
            if back is None:
                result["skipped"].append("%s: %s" % (label, note))
                r.fail("NOT PATCHED %s: %s" % (label, note))
                continue
            errs = prove(sim, p["doc"], back, p["adds"])
            if errs:
                r.fail("%s written but the proof fails: %s" % (label, errs))
                raise SystemExit(1)
            result["patched"].append({"policy": label, "added": [a["statement"] for a in p["adds"]]})
            for a in p["adds"]:
                r.ok("%s: + %s (after statement %s) - %d lost grant(s) now matched on the Log Group ARN, original unchanged (%s)" % (
                    label, json.dumps(a["statement"]), a["after"], len(a["lost"]), note))
        for rp, doc, new_doc, adds in rp_work:
            label = "logs resource policy %s" % rp.get("policyName")
            if size(new_doc) > LIMITS["logs_resource"]:
                result["skipped"].append("%s: %d > %d" % (label, size(new_doc), LIMITS["logs_resource"]))
                r.fail("NOT PATCHED %s: %d > %d characters" % (label, size(new_doc), LIMITS["logs_resource"]))
                continue
            store.put(ca.safe_key("logs-resource-policy", rp.get("policyName") + ".json"), {"resource_policy": rp, "patched": new_doc})
            kw = {"policyName": rp["policyName"], "policyDocument": json.dumps(new_doc)}
            if rp.get("resourceArn"):
                kw.update(resourceArn=rp["resourceArn"], expectedRevisionId=rp.get("revisionId"))
            logs.put_resource_policy(**{k: v for k, v in kw.items() if v is not None})
            q = {"policyScope": "RESOURCE", "resourceArn": rp["resourceArn"]} if rp.get("resourceArn") else {}
            back = json.loads(next(x for x in logs.describe_resource_policies(**q).get("resourcePolicies", [])
                                   if x.get("policyName") == rp["policyName"])["policyDocument"])
            errs = prove(local_simulate, doc, back, adds)
            if errs:
                r.fail("%s written but the proof fails: %s" % (label, errs))
                raise SystemExit(1)
            result["patched"].append({"policy": label, "added": [a["statement"] for a in adds]})
            r.ok("%s patched and proven: %s" % (label, json.dumps([a["statement"] for a in adds])))
        for ep, doc, new_doc, adds in vpce_work:
            label = "vpc endpoint %s" % ep["VpcEndpointId"]
            store.put(ca.safe_key("vpce", ep["VpcEndpointId"] + ".json"), {"endpoint": ep["VpcEndpointId"], "document": doc, "patched": new_doc})
            ec2.modify_vpc_endpoint(VpcEndpointId=ep["VpcEndpointId"], PolicyDocument=json.dumps(new_doc))
            back = json.loads(ec2.describe_vpc_endpoints(VpcEndpointIds=[ep["VpcEndpointId"]])["VpcEndpoints"][0]["PolicyDocument"])
            errs = prove(local_simulate, doc, back, adds)
            if errs:
                r.fail("%s written but the proof fails: %s" % (label, errs))
                raise SystemExit(1)
            result["patched"].append({"policy": label, "added": [a["statement"] for a in adds]})
            r.ok("%s patched and proven: %s" % (label, json.dumps([a["statement"] for a in adds])))
        if not (work or rp_work or vpce_work):
            r.ok("nothing affected under the stricter audit - no change made")
        r.log("backups: s3://%s/backups/iam-logs-group-arn/%s/ (%d objects)" % (PRIVATE, stamp, len(store.written)))

        r.section("4. whole-account re-audit after the patch")
        plans2, remaining = [], []
        for _ in range(7):
            plans2 = iam_plans(iam, sim)
            remaining = ["%s in %s" % (a["sid"], p["name"]) for p in plans2 for a in p["adds"]]
            if not remaining or not result["patched"]:
                break
            time.sleep(10)                                  # IAM read-after-write is eventually consistent
        f2 = [f for p in plans2 for f in p["findings"]]
        r.log("statements naming a Log Stream ARN with a group-level logs action: %d -> %s" % (
            len(f2), {s: sum(1 for f in f2 if status_word(f) == s) for s in sorted({status_word(f) for f in f2})}))
        if remaining:
            r.fail("still affected after the patch: %s" % remaining)
        else:
            r.ok("0 affected statements remain in %d customer policies" % len(plans2))
        result["remaining"] = remaining
        r.log("RESULT_JSON " + json.dumps(result, default=str))
        r.ok("ops 6689 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
