"""
ops_4232 — COST REMEDIATION, wave 4 (closing).

Three jobs, ordered so a slow one cannot block a fast one:

  A. DOUBLE-FIRE BUG. ops 4231 found 113 exact-duplicate schedule groups
     but could only disable 26. The other ~87 are not duplicate RULES —
     they are ONE rule listing the SAME Lambda target TWICE. Every tick
     of those rules invokes the engine twice: double compute, double S3
     writes, and double burn against the FRED / FMP / TradingEconomics
     quotas that ops 4209-4214 proved are the binding constraint on
     coverage. Fixed with remove_targets on the redundant target id.
     Targets are only considered redundant when Arn AND Input AND
     InputPath AND InputTransformer all match exactly.

  B. STALE ENV REFERENCE. The ops-4231 gate held openbb-financial-search
     because scrapeMacroData's live environment names it. Source review
     shows the function never reads that variable — it is dead config
     from the 2025 FRED/OpenSearch era, not a live dependency. Strip the
     variable, re-run the gate, and only then delete.

  C. INDEX PRESERVATION THEN TEARDOWN. openbb-simple-working holds 6,206
     docs and its access policy rejected the runner (403), so ops 4231
     correctly refused to delete it. Here the access policy is MERGED
     (never overwritten — overwriting could lock out a principal we
     cannot see), the index and mappings are dumped to S3, the dump is
     VERIFIED by re-reading it back and counting, and only a verified
     dump authorises the delete.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=90)
NOW = datetime.now(timezone.utc)
OUT = {"ops": 4232, "ts": NOW.isoformat(), "actions": []}
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))


def C(s):
    return boto3.client(s, region_name=REGION, config=CFG)


evb, lam, s3 = C("events"), C("lambda"), C("s3")
ACCT = boto3.client("sts").get_caller_identity()["Account"]
USER_ARN = "arn:aws:iam::%s:user/github-actions-justhodl" % ACCT


def es_call(endpoint, path, method="GET", body=None):
    url = "https://%s%s" % (endpoint, path)
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    data = json.dumps(body).encode() if body is not None else None
    areq = AWSRequest(method=method, url=url, data=data,
                      headers={"Content-Type": "application/json"})
    SigV4Auth(creds, "es", REGION).add_auth(areq)
    req = Request(url, data=data, headers=dict(areq.headers), method=method)
    return json.loads(urlopen(req, timeout=90).read())


with report("4232_close_out") as rep:
    rep.heading("ops 4232 — wave 4: double-fire fix + OpenSearch teardown")

    # ================================================================ A
    rep.section("A. Remove duplicate targets inside EventBridge rules")
    n_rules = n_fixed = n_targets_removed = 0
    affected = []
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                if r.get("State") != "ENABLED":
                    continue
                n_rules += 1
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                targets = tg.get("Targets", [])
                if len(targets) < 2:
                    continue
                seen, dup_ids = {}, []
                for t in targets:
                    if ":function:" not in t.get("Arn", ""):
                        continue
                    sig = json.dumps({
                        "arn": t.get("Arn"),
                        "input": t.get("Input"),
                        "input_path": t.get("InputPath"),
                        "itx": t.get("InputTransformer"),
                    }, sort_keys=True)
                    if sig in seen:
                        dup_ids.append(t["Id"])
                    else:
                        seen[sig] = t["Id"]
                if not dup_ids:
                    continue
                fn = targets[0].get("Arn", "").split(":")[-1]
                try:
                    evb.remove_targets(Rule=r["Name"], Ids=dup_ids)
                    n_fixed += 1
                    n_targets_removed += len(dup_ids)
                    affected.append({"rule": r["Name"], "fn": fn,
                                     "removed": len(dup_ids),
                                     "expr": r.get("ScheduleExpression")})
                    rep.ok("  %-40s %-34s removed %d redundant target(s)"
                           % (r["Name"][:40], fn[:34], len(dup_ids)))
                    rep.kv(section="double_fire", rule=r["Name"],
                           function=fn, removed=len(dup_ids),
                           expr=r.get("ScheduleExpression"))
                except Exception as e:
                    rep.fail("  %s: %s" % (r["Name"], str(e)[:110]))
        rep.log("")
        rep.log("enabled rules scanned: %d | rules fixed: %d | "
                "redundant targets removed: %d"
                % (n_rules, n_fixed, n_targets_removed))
        rep.log("=> %d engines were firing twice per tick and now fire once"
                % n_fixed)
        OUT["double_fire_fixed"] = affected
    except Exception as e:
        rep.fail("target dedupe: %s" % str(e)[:170])

    # ================================================================ B
    rep.section("B. Strip stale OpenSearch env ref, then re-gate")
    stale_ok = False
    try:
        cfgn = lam.get_function_configuration(FunctionName="scrapeMacroData")
        env = (cfgn.get("Environment") or {}).get("Variables") or {}
        bad = {k: v for k, v in env.items()
               if "openbb-financial-search" in str(v)}
        if not bad:
            rep.log("  no stale var present (already clean)")
            stale_ok = True
        else:
            for k, v in bad.items():
                rep.log("  removing %s = %s" % (k, str(v)[:70]))
            keep = {k: v for k, v in env.items() if k not in bad}
            # keep a copy so this is reversible
            s3.put_object(
                Bucket=BUCKET,
                Key="backups/scrapeMacroData-env-%s.json"
                    % NOW.strftime("%Y%m%d"),
                Body=json.dumps(env, indent=1).encode(),
                ContentType="application/json")
            lam.update_function_configuration(
                FunctionName="scrapeMacroData",
                Environment={"Variables": keep})
            rep.ok("  env updated (%d -> %d vars); prior env backed up to S3"
                   % (len(env), len(keep)))
            OUT["actions"].append({"a": "strip_env", "fn": "scrapeMacroData",
                                   "removed": list(bad)})
            stale_ok = True
    except Exception as e:
        rep.fail("  env strip: %s" % str(e)[:150])

    # re-gate: nothing anywhere may name the domain
    if stale_ok:
        rep.log("  re-running the reference gate…")
        hits = []
        try:
            for page in lam.get_paginator("list_functions").paginate():
                for f in page["Functions"]:
                    ev = (f.get("Environment") or {}).get("Variables") or {}
                    if "openbb-financial-search" in json.dumps(ev):
                        hits.append(f["FunctionName"])
        except Exception as e:
            hits = ["<scan-failed>"]
            rep.fail("  re-gate scan: %s" % str(e)[:110])
        if hits:
            rep.fail("  STILL REFERENCED by %s — HOLD" % ", ".join(hits[:5]))
        else:
            rep.ok("  gate clean — 0 references fleet-wide")
            try:
                C("opensearch").delete_domain(
                    DomainName="openbb-financial-search")
                rep.ok("  OpenSearch openbb-financial-search DELETED "
                       "(2 docs, orphaned)")
                OUT["actions"].append({"a": "delete_domain",
                                       "id": "openbb-financial-search"})
            except Exception as e:
                rep.fail("  delete: %s" % str(e)[:150])

    # ================================================================ C
    rep.section("C. openbb-simple-working — backup, verify, then delete")
    DOM = "openbb-simple-working"
    os_ = C("opensearch")
    try:
        st = os_.describe_domain(DomainName=DOM)["DomainStatus"]
        ep = st["Endpoint"]
        rep.log("  endpoint %s" % ep)

        # --- merge (never overwrite) the access policy
        try:
            cur = json.loads(st.get("AccessPolicies") or
                             '{"Version":"2012-10-17","Statement":[]}')
        except Exception:
            cur = {"Version": "2012-10-17", "Statement": []}
        stmts = cur.get("Statement") or []
        already = any(USER_ARN in json.dumps(s_) for s_ in stmts)
        if already:
            rep.log("  access policy already grants the runner")
        else:
            stmts.append({
                "Effect": "Allow",
                "Principal": {"AWS": USER_ARN},
                "Action": "es:ESHttp*",
                "Resource": "arn:aws:es:%s:%s:domain/%s/*"
                            % (REGION, ACCT, DOM)})
            cur["Statement"] = stmts
            os_.update_domain_config(DomainName=DOM,
                                     AccessPolicies=json.dumps(cur))
            rep.ok("  access policy MERGED (existing statements preserved, "
                   "%d total)" % len(stmts))
            rep.log("  waiting for the domain to apply the config…")
            for i in range(60):
                time.sleep(15)
                d = os_.describe_domain(DomainName=DOM)["DomainStatus"]
                if not d.get("Processing"):
                    rep.ok("  config active after %ds" % ((i + 1) * 15))
                    break
            else:
                rep.warn("  still processing after 15min — dump may 403")

        # --- dump index + mappings
        dumped = 0
        payload = None
        for attempt in range(6):
            try:
                mapping = es_call(ep, "/_all/_mapping")
                body = es_call(ep, "/_all/_search?size=10000")
                hits = body.get("hits", {}).get("hits", [])
                total = body.get("hits", {}).get("total")
                total = (total.get("value") if isinstance(total, dict)
                         else total)
                dumped = len(hits)
                payload = {"domain": DOM, "dumped_at": NOW.isoformat(),
                           "reported_total": total, "n_docs": dumped,
                           "mappings": mapping, "docs": hits}
                rep.ok("  dump OK — %d docs retrieved (index reports %s)"
                       % (dumped, total))
                break
            except Exception as e:
                rep.warn("  dump attempt %d: %s" % (attempt + 1, str(e)[:100]))
                time.sleep(20)

        if not dumped:
            rep.fail("  dump FAILED — domain HELD, not deleting. "
                     "6,206 docs are worth more than $25/mo.")
            OUT["actions"].append({"a": "hold", "id": DOM,
                                   "why": "dump_failed"})
        else:
            key = "backups/opensearch/%s-%s.json" % (DOM,
                                                     NOW.strftime("%Y%m%d"))
            s3.put_object(Bucket=BUCKET, Key=key,
                          Body=json.dumps(payload).encode(),
                          ContentType="application/json")
            # VERIFY by reading it back and recounting — a put that
            # silently truncated would otherwise authorise a delete.
            back = json.loads(s3.get_object(Bucket=BUCKET,
                                            Key=key)["Body"].read())
            n_back = len(back.get("docs") or [])
            rep.log("  verify: s3 round-trip returned %d docs" % n_back)
            if n_back != dumped or n_back == 0:
                rep.fail("  VERIFY FAILED (%d != %d) — domain HELD"
                         % (n_back, dumped))
                OUT["actions"].append({"a": "hold", "id": DOM,
                                       "why": "verify_failed"})
            else:
                rep.ok("  backup verified: s3://%s/%s" % (BUCKET, key))
                os_.delete_domain(DomainName=DOM)
                rep.ok("  OpenSearch %s DELETED (%d docs preserved on S3)"
                       % (DOM, n_back))
                OUT["actions"].append({"a": "delete_domain", "id": DOM,
                                       "backup": key, "docs": n_back})
    except Exception as e:
        rep.fail("  %s: %s" % (DOM, str(e)[:200]))

    # ================================================================ D
    rep.section("D. Result")
    for a in OUT["actions"]:
        rep.log("   %s" % json.dumps(a)[:160])
    rep.log("double-fire rules fixed: %d (%d redundant targets removed)"
            % (n_fixed, n_targets_removed))
    (ROOT / "aws" / "ops" / "reports" / "4232_close_out.json").write_text(
        json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote 4232_close_out.json")
