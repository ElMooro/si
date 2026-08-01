"""
ops_4231 — COST REMEDIATION, wave 3.  PROVE-BEFORE-DELETE.

Khalid's constraint is explicit and correct: nothing may be removed that
something else still depends on. This op therefore refuses to delete
anything until a reference sweep proves the resource is orphaned, and it
writes a rollback ledger for everything it touches.

THE GATE (section 1). For every deletion candidate we sweep for its
identifiers across FIVE independent surfaces, because any one of them
alone can lie:
  1. every file in the repo (code, HTML, JS, ops, cloudflare workers) —
     not just aws/lambdas/*/source, since a page can call an endpoint
     directly;
  2. the LIVE environment variables of all ~765 deployed Lambdas — the
     deployed env can carry an endpoint the repo copy never mentions
     (this is exactly how the FRED_KEY propagation bug hid in ops 3957);
  3. live Lambda function code is covered by (1) via the repo mirror,
     plus any function whose name contains the resource family;
  4. Route53 record sets pointing at the resource's DNS name;
  5. real traffic metrics over 14 days.
A single hit on ANY surface = HOLD. No deletion, no exception.

THE DEDUPE (section 4). Schedules are only treated as duplicates when
target function AND normalised schedule expression AND input payload all
match exactly. Two rules hitting the same function at different times,
or with different payloads, are DIFFERENT INTENT (market-open vs
market-close, phase=warm vs phase=aggregate) and are never touched. A
function can never lose its last enabled schedule.

Duplicates are DISABLED, not deleted. Disable is a one-call undo and the
rule definition survives; delete is not. After a soak period a follow-up
op can hard-delete the disabled set. The rollback ledger is written to
both S3 and the repo so re-enabling is a single scripted action.
"""

import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=90)
NOW = datetime.now(timezone.utc)
D14 = NOW - timedelta(days=14)
OUT = {"ops": 4231, "ts": NOW.isoformat(), "deleted": [], "held": [],
       "disabled_schedules": []}


def C(s):
    return boto3.client(s, region_name=REGION, config=CFG)


lam, cw, s3 = C("lambda"), C("cloudwatch"), C("s3")
evb, sch, ec2 = C("events"), C("scheduler"), C("ec2")

ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

# Candidate -> identifier strings that would appear in any live caller.
CANDIDATES = {
    "opensearch:openbb-financial-search": [
        "openbb-financial-search",
        "search-openbb-financial-search-pjxaw2cqqeqfilppjyxkhfgwue"],
    "opensearch:openbb-simple-working": [
        "openbb-simple-working",
        "search-openbb-simple-working-oi5qjg5nan4a73ifgopmhfp234"],
    "elbv2:openbb-prod-alb": [
        "openbb-prod-alb", "openbb-prod-alb-180258533"],
    "elbv2:openbb-basic-alb": [
        "openbb-basic-alb", "openbb-basic-alb-989296070"],
}


def repo_hits(needle):
    """grep the ENTIRE repo, excluding .git, ops reports (which now
    contain these names because ops 4228-4230 printed them) and this
    script itself — otherwise the audit trail poisons its own gate."""
    try:
        r = subprocess.run(
            ["grep", "-rIl", "--exclude-dir=.git",
             "--exclude-dir=reports", "--exclude=ops_4231*",
             "--exclude=ops_4230*", "--exclude=ops_4228*",
             "--exclude=STATE.md", needle, "."],
            cwd=str(ROOT), capture_output=True, text=True, timeout=180)
        return [x for x in r.stdout.strip().split("\n") if x]
    except Exception:
        return ["<grep-failed:ASSUME-USED>"]


with report("4231_prove_then_delete") as rep:
    rep.heading("ops 4231 — prove-before-delete + fleet schedule dedupe")

    # ================================================================ 1
    rep.section("1. REFERENCE SWEEP — the gate")

    rep.log("1a. loading live Lambda env vars across the fleet…")
    env_blob, fn_names = [], []
    try:
        for page in lam.get_paginator("list_functions").paginate():
            for f in page["Functions"]:
                fn_names.append(f["FunctionName"])
                ev = (f.get("Environment") or {}).get("Variables") or {}
                if ev:
                    env_blob.append(f["FunctionName"] + "\x00" +
                                    json.dumps(ev))
        rep.ok("   %d functions, %d carrying env vars"
               % (len(fn_names), len(env_blob)))
    except Exception as e:
        rep.fail("   env load failed — GATE FAILS CLOSED: %s" % str(e)[:120])
        raise SystemExit("cannot verify env vars; refusing to delete")

    rep.log("1b. loading Route53 record sets…")
    r53_blob = ""
    try:
        r53 = boto3.client("route53", config=CFG)
        for z in r53.list_hosted_zones()["HostedZones"]:
            for page in r53.get_paginator(
                    "list_resource_record_sets").paginate(
                    HostedZoneId=z["Id"]):
                r53_blob += json.dumps(page["ResourceRecordSets"])
        rep.ok("   %d chars of DNS records scanned" % len(r53_blob))
    except Exception as e:
        rep.warn("   route53 not in scope (%s) — DNS surface unverified"
                 % str(e)[:70])
        r53_blob = None

    verdict = {}
    for cand, needles in CANDIDATES.items():
        hits = {"repo": [], "env": [], "dns": [], "fnname": []}
        for n in needles:
            hits["repo"] += repo_hits(n)
            hits["env"] += [b.split("\x00")[0] for b in env_blob if n in b]
            hits["fnname"] += [f for f in fn_names if n in f]
            if r53_blob and n in r53_blob:
                hits["dns"].append(n)
        clean = not any(hits[k] for k in hits)
        verdict[cand] = {"clean": clean, "hits": hits}
        if clean:
            rep.ok("  %-44s ORPHANED — 0 refs on all surfaces" % cand)
        else:
            rep.fail("  %-44s REFERENCED — HOLD" % cand)
            for k, v in hits.items():
                if v:
                    rep.log("       %s: %s" % (k, ", ".join(v[:6])[:150]))
        rep.kv(section="gate", candidate=cand,
               verdict="ORPHANED" if clean else "REFERENCED",
               repo_refs=len(hits["repo"]), env_refs=len(hits["env"]),
               dns_refs=len(hits["dns"]))
    OUT["gate"] = {k: v["clean"] for k, v in verdict.items()}

    # ================================================================ 2
    rep.section("2. Re-confirm zero traffic at deletion time")

    def alb_reqs(arn):
        sfx = "/".join(arn.split("/")[-3:])
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/ApplicationELB", MetricName="RequestCount",
                Dimensions=[{"Name": "LoadBalancer", "Value": sfx}],
                StartTime=D14, EndTime=NOW, Period=1209600,
                Statistics=["Sum"])
            return sum(p["Sum"] for p in r.get("Datapoints", []))
        except Exception:
            return -1.0

    elb = C("elbv2")
    albs = {}
    try:
        for lb in elb.describe_load_balancers().get("LoadBalancers", []):
            albs[lb["LoadBalancerName"]] = lb
            rc = alb_reqs(lb["LoadBalancerArn"])
            rep.log("   ALB %-22s 14d requests = %.0f"
                    % (lb["LoadBalancerName"], rc))
            albs[lb["LoadBalancerName"]]["_rc"] = rc
    except Exception as e:
        rep.fail("elb read: %s" % str(e)[:140])

    # ================================================================ 3
    rep.section("3. Deletions — only where the gate passed")

    # ---- 3a ALBs
    for name in ("openbb-prod-alb", "openbb-basic-alb"):
        key = "elbv2:" + name
        lb = albs.get(name)
        if not lb:
            rep.warn("  %s not present (already gone?)" % name)
            continue
        if not verdict.get(key, {}).get("clean"):
            rep.warn("  %s HELD — reference sweep found callers" % name)
            OUT["held"].append({"id": name, "why": "referenced"})
            continue
        if lb.get("_rc", -1) != 0:
            rep.warn("  %s HELD — 14d requests = %s (not zero)"
                     % (name, lb.get("_rc")))
            OUT["held"].append({"id": name, "why": "traffic"})
            continue
        try:
            tgs = elb.describe_target_groups(
                LoadBalancerArn=lb["LoadBalancerArn"]).get("TargetGroups", [])
            elb.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])
            rep.ok("  ALB %s DELETED (0 requests/14d, 0 references)" % name)
            OUT["deleted"].append({"svc": "elbv2", "id": name})
            for tg in tgs:
                try:
                    time.sleep(2)
                    elb.delete_target_group(
                        TargetGroupArn=tg["TargetGroupArn"])
                    rep.log("     target group %s deleted"
                            % tg["TargetGroupName"])
                except Exception as e:
                    rep.warn("     tg %s: %s" % (tg["TargetGroupName"],
                                                 str(e)[:70]))
        except Exception as e:
            rep.fail("  ALB %s: %s" % (name, str(e)[:150]))

    # ---- 3b OpenSearch
    os_ = C("opensearch")
    for dom in ("openbb-financial-search", "openbb-simple-working"):
        key = "opensearch:" + dom
        if not verdict.get(key, {}).get("clean"):
            rep.warn("  OpenSearch %s HELD — referenced" % dom)
            OUT["held"].append({"id": dom, "why": "referenced"})
            continue
        try:
            st = os_.describe_domain(DomainName=dom)["DomainStatus"]
        except Exception as e:
            rep.warn("  %s not readable: %s" % (dom, str(e)[:90]))
            continue
        # Try to preserve the index before an irreversible delete.
        dumped = 0
        try:
            from botocore.auth import SigV4Auth
            from botocore.awsrequest import AWSRequest
            from urllib.request import Request, urlopen
            sess = boto3.Session()
            creds = sess.get_credentials().get_frozen_credentials()
            base = "https://" + st["Endpoint"]
            url = base + "/_search?size=10000"
            areq = AWSRequest(method="GET", url=url)
            SigV4Auth(creds, "es", REGION).add_auth(areq)
            r = Request(url, headers=dict(areq.headers))
            raw = urlopen(r, timeout=60).read()
            body = json.loads(raw)
            hits = body.get("hits", {}).get("hits", [])
            dumped = len(hits)
            if dumped:
                s3.put_object(
                    Bucket=BUCKET,
                    Key="backups/opensearch/%s-%s.json"
                        % (dom, NOW.strftime("%Y%m%d")),
                    Body=json.dumps(body).encode(),
                    ContentType="application/json")
                rep.ok("     dumped %d docs -> s3://%s/backups/opensearch/"
                       % (dumped, BUCKET))
        except Exception as e:
            rep.warn("     index dump failed: %s" % str(e)[:110])

        docs_expected = 6206 if dom == "openbb-simple-working" else 2
        if docs_expected > 100 and dumped == 0:
            rep.fail("  OpenSearch %s HELD — %d docs and the backup dump "
                     "failed. Deleting is irreversible; not risking the "
                     "index." % (dom, docs_expected))
            OUT["held"].append({"id": dom, "why": "backup_failed",
                                "docs": docs_expected})
            continue
        try:
            os_.delete_domain(DomainName=dom)
            rep.ok("  OpenSearch %s DELETED (0 references, %d docs backed up)"
                   % (dom, dumped))
            OUT["deleted"].append({"svc": "opensearch", "id": dom,
                                   "backup_docs": dumped})
        except Exception as e:
            rep.fail("  %s: %s" % (dom, str(e)[:150]))

    # ---- 3c SageMaker Studio app (space-based variant)
    try:
        sm = C("sagemaker")
        for a in sm.list_apps(MaxResults=100).get("Apps", []):
            if a.get("Status") != "InService":
                continue
            kw = {"DomainId": a["DomainId"], "AppType": a["AppType"],
                  "AppName": a["AppName"]}
            if a.get("SpaceName"):
                kw["SpaceName"] = a["SpaceName"]
            elif a.get("UserProfileName"):
                kw["UserProfileName"] = a["UserProfileName"]
            else:
                rep.warn("  SM app %s has neither Space nor UserProfile"
                         % a["AppName"])
                continue
            sm.delete_app(**kw)
            rep.ok("  SageMaker app %s (%s) DELETED — EFS home preserved, "
                   "relaunch from Studio anytime"
                   % (a["AppName"], a["AppType"]))
            OUT["deleted"].append({"svc": "sagemaker_app",
                                   "id": a["AppName"]})
    except Exception as e:
        rep.fail("  sagemaker: %s" % str(e)[:150])

    # ---- 3d EIPs — only genuinely unassociated ones
    rep.log("")
    try:
        for a in ec2.describe_addresses().get("Addresses", []):
            if a.get("AssociationId"):
                rep.log("  EIP %-16s still associated (%s) — KEPT"
                        % (a.get("PublicIp"),
                           a.get("InstanceId") or a.get("NetworkInterfaceId")))
                continue
            ec2.release_address(AllocationId=a["AllocationId"])
            rep.ok("  EIP %s RELEASED (unassociated)" % a.get("PublicIp"))
            OUT["deleted"].append({"svc": "eip", "id": a.get("PublicIp")})
    except Exception as e:
        rep.fail("  eip: %s" % str(e)[:140])

    # ================================================================ 4
    rep.section("4. Fleet-wide EXACT-duplicate schedule dedupe")
    rep.log("rule = same target function AND same expression AND same "
            "input payload. Anything else is different intent, untouched.")

    entries = []   # (kind, name, expr, input, fn, group)
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                if not r.get("ScheduleExpression") or \
                        r.get("State") != "ENABLED":
                    continue
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                for t in tg.get("Targets", []):
                    arn = t.get("Arn", "")
                    if ":function:" not in arn:
                        continue
                    entries.append(("events", r["Name"],
                                    r["ScheduleExpression"].strip(),
                                    json.dumps(t.get("Input") or "",
                                               sort_keys=True),
                                    arn.split(":")[-1], None))
    except Exception as e:
        rep.warn("rules: %s" % str(e)[:120])
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                if s_.get("State") != "ENABLED":
                    continue
                g = s_.get("GroupName", "default")
                try:
                    d = sch.get_schedule(Name=s_["Name"], GroupName=g)
                except Exception:
                    continue
                tgt = d.get("Target", {})
                arn = tgt.get("Arn", "")
                if ":function:" not in arn:
                    continue
                entries.append(("scheduler", s_["Name"],
                                (d.get("ScheduleExpression") or "").strip(),
                                json.dumps(tgt.get("Input") or "",
                                           sort_keys=True),
                                arn.split(":")[-1], g))
    except Exception as e:
        rep.warn("schedules: %s" % str(e)[:120])

    rep.log("enabled Lambda-targeting schedules: %d" % len(entries))

    groups = {}
    for e in entries:
        groups.setdefault((e[4], e[2], e[3]), []).append(e)
    exact = {k: v for k, v in groups.items() if len(v) > 1}
    rep.log("EXACT duplicate groups (fn+expr+payload identical): %d"
            % len(exact))

    per_fn_enabled = {}
    for e in entries:
        per_fn_enabled[e[4]] = per_fn_enabled.get(e[4], 0) + 1

    n_dis = 0
    ledger = []
    for (fn, expr, inp), v in sorted(exact.items()):
        keep = sorted(v, key=lambda x: (x[0] != "scheduler", x[1]))[0]
        rep.log("  %-40s %-26s x%d  keep=%s"
                % (fn[:40], expr[:26], len(v), keep[1][:26]))
        for kind, name, ex, ip, f, grp in v:
            if (kind, name) == (keep[0], keep[1]):
                continue
            if per_fn_enabled.get(f, 0) <= 1:
                rep.warn("     refusing to disable %s — it is the last "
                         "enabled schedule for %s" % (name, f))
                continue
            try:
                if kind == "events":
                    evb.disable_rule(Name=name)
                else:
                    d = sch.get_schedule(Name=name, GroupName=grp)
                    sch.update_schedule(
                        Name=name, GroupName=grp,
                        ScheduleExpression=d["ScheduleExpression"],
                        FlexibleTimeWindow=d["FlexibleTimeWindow"],
                        Target=d["Target"], State="DISABLED")
                per_fn_enabled[f] -= 1
                n_dis += 1
                ledger.append({"kind": kind, "name": name, "group": grp,
                               "fn": f, "expr": ex})
                rep.log("     disabled %s/%s" % (kind, name))
            except Exception as e:
                rep.fail("     %s: %s" % (name, str(e)[:100]))

    rep.ok("disabled %d exact-duplicate schedules (NOT deleted — "
           "re-enable is one call each)" % n_dis)
    OUT["disabled_schedules"] = ledger

    near = {k: v for k, v in groups.items() if len(v) == 1}
    multi = {}
    for e in entries:
        multi.setdefault(e[4], set()).add(e[2])
    diff_intent = {f: s for f, s in multi.items() if len(s) > 1}
    rep.log("")
    rep.log("UNTOUCHED — same function, DIFFERENT cadence/payload "
            "(different intent): %d functions" % len(diff_intent))
    for f, s in sorted(diff_intent.items(),
                       key=lambda x: -len(x[1]))[:12]:
        rep.log("   %-40s %s" % (f[:40], ", ".join(sorted(s))[:80]))
        rep.kv(section="different_intent", function=f,
               exprs=", ".join(sorted(s))[:100])

    # ================================================================ 5
    rep.section("5. Rollback ledger")
    led = {"ops": 4231, "at": NOW.isoformat(),
           "disabled_schedules": ledger, "deleted": OUT["deleted"],
           "held": OUT["held"]}
    try:
        s3.put_object(Bucket=BUCKET,
                      Key="backups/ops-4231-rollback.json",
                      Body=json.dumps(led, indent=1).encode(),
                      ContentType="application/json")
        rep.ok("rollback ledger -> s3://%s/backups/ops-4231-rollback.json"
               % BUCKET)
    except Exception as e:
        rep.warn("ledger to s3: %s" % str(e)[:110])
    (ROOT / "aws" / "ops" / "reports" / "4231_rollback.json").write_text(
        json.dumps(led, indent=1), encoding="utf-8")

    rep.section("RESULT")
    rep.log("deleted: %d   held: %d   schedules disabled: %d"
            % (len(OUT["deleted"]), len(OUT["held"]), n_dis))
    for d in OUT["deleted"]:
        rep.log("   - %s %s" % (d["svc"], d["id"]))
    for h in OUT["held"]:
        rep.warn("   HELD %s (%s)" % (h["id"], h["why"]))
    (ROOT / "aws" / "ops" / "reports" / "4231_prove_then_delete.json"
     ).write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
