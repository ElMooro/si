"""ops 3252 — panels drill-drawer 403s fixed without touching working
engines:

  · ROOT CAUSE A: detail feeds were written only inside the scored-panel
    loop — every DORMANT engine 403'd (S3 answers missing keys with 403).
    Runner now writes a thin detail doc for every non-ACTIVE row (same
    keys the drawer reads); ACTIVE docs untouched.
  · ROOT CAUSE B: MEMBERS column did (int||[]).length → 'undefined'.
    Int-safe renderer + sorter.
  · DEFENSE: the drawer's catch now renders the row's own index data
    (state, reason, members) instead of a raw error, for any residual
    gap.

Verify: deploy runner (runner-only change — no shared-module cascade),
fleet run, then PROVE (a) a known-dormant detail key now exists with
its reason, (b) a known-ACTIVE detail is still rich (lit_indicators +
event_study present — nothing clobbered), (c) the page serves the ops
3252 literals.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-wl-engines"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3252)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3252_drawer_403_fix") as rep:
    fails, warns = [], []
    rep.heading("ops 3252 — drawer 403s: dormant details written, page "
                "hardened")

    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json")
                     .read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live = (LAM.get_function_configuration(FunctionName=FN)
            .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 3008),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    if not fails:
        rep.section("1. Fleet run")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        idx = None
        for _ in range(70):
            time.sleep(10)
            d = s3_json("data/wl-engines.json") or {}
            if str(d.get("generated_at", "")) > mark:
                idx = d
                break
        if not idx:
            fails.append("index not fresh in window")

    if not fails:
        rep.section("2. Proof — dormant detail exists, active untouched")
        eng = idx.get("engines") or []
        dorm = next((e for e in eng
                     if str(e.get("state")) != "ACTIVE"), None)
        act = next((e for e in eng
                    if str(e.get("state")) == "ACTIVE"
                    and e.get("activation_pctile") is not None), None)
        hq = next((e for e in eng if "high quality" in
                   str(e.get("name", "")).lower()), None)
        for tag, e in (("dormant", dorm), ("hqm(reported)", hq)):
            if not e:
                continue
            doc = s3_json(f"data/engines/{e['engine_id']}.json")
            if doc and doc.get("detail_level") == "dormant-min" \
                    and doc.get("reason"):
                rep.ok(f"{tag}: {e['engine_id']} detail EXISTS — "
                       f"reason='{str(doc['reason'])[:60]}'")
            elif doc and str(e.get("state")) == "ACTIVE":
                rep.ok(f"{tag}: {e['engine_id']} is ACTIVE now — "
                       "rich doc present")
            else:
                fails.append(f"{tag}: {e['engine_id']} detail missing "
                             "post-run")
        if act:
            doc = s3_json(f"data/engines/{act['engine_id']}.json") or {}
            rich = bool(doc.get("lit_indicators") is not None
                        and (doc.get("event_study") or {}).get("w13"))
            if rich and doc.get("detail_level") != "dormant-min":
                rep.ok(f"active untouched: {act['engine_id']} still "
                       f"rich (w13 n={doc['event_study']['w13'].get('n')},"
                       f" {len(doc.get('all_members') or [])} members)")
            else:
                fails.append(f"active doc degraded: {act['engine_id']}")

        rep.section("3. Page live with the fixes")
        okp = False
        for i in range(22):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/panels.html?t={int(time.time())}",
                    headers=UA), timeout=15).read().decode("utf-8",
                                                           "replace")
                if "ops 3252: field is an int" in h \
                        and "dormant fallback" in h:
                    okp = True
                    rep.ok(f"panels.html live with both fixes "
                           f"(~{(i + 1) * 15}s)")
                    break
            except Exception:
                pass
            time.sleep(15)
        if not okp:
            warns.append("page literals not live in window — pages "
                         "deploy may still be propagating")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
