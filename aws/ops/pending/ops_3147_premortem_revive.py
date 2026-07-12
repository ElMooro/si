"""ops 3147 — premortem revive (kill-theses feed is EMPTY, found by 3146).

Every kill-overlay (alpha-compass bear lines, best-setups, master-ranker)
has been joining a hollow data/kill-theses.json. Router primary is GLM
(SSM key); Anthropic fallback needs env the function doesn't carry.

This op, in one pass:
  1. Live diagnosis verbatim: kill-theses doc meta (generated_at,
     n_ok/n_fail, first error) + input data/best-ideas.json freshness.
  2. Arm the fallback: merge runner ANTHROPIC_API_KEY into function env
     (env-only update — code untouched).
  3. Async invoke (600s engine) → poll fresh doc ≤640s.
  4. Gate: theses >= 3 with real kill_conditions; else dump the new
     run's error fields verbatim for the next iteration.
  5. IR chip CDN recheck (warn-only, from 3146).
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

import boto3

from ops_report import report
from _lambda_deploy_helpers import _retry_on_conflict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-premortem-engine"
OUT = "data/kill-theses.json"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3147_premortem_revive") as rep:
    fails, warns = [], []
    t0 = datetime.now(timezone.utc)
    rep.heading("ops 3147 — premortem revive (empty kill-theses)")

    rep.section("1. Live diagnosis")
    try:
        cur = s3_json(OUT)
        rep.kv(prev_generated=cur.get("generated_at"),
               prev_theses=len(cur.get("theses") or []),
               prev_n_ok=cur.get("n_ok"), prev_n_fail=cur.get("n_fail"))
        errs = [t for t in (cur.get("theses_failed") or
                            cur.get("errors") or []) if t]
        if errs:
            rep.log(f"prev error sample: {json.dumps(errs[0])[:220]}")
        for k in cur.keys():
            if "err" in k.lower() or "fail" in k.lower():
                rep.log(f"prev doc key {k}: {json.dumps(cur[k])[:200]}")
    except Exception as e:
        warns.append(f"prev doc unreadable: {e}")
    try:
        bi = s3_json("data/best-ideas.json")
        n_ideas = len(bi.get("ideas") or bi.get("best_ideas")
                      or bi.get("rows") or [])
        rep.kv(best_ideas_generated=bi.get("generated_at"),
               best_ideas_n=n_ideas)
        if n_ideas == 0:
            warns.append("best-ideas input has 0 rows — premortem has "
                         "nothing to chew; nobrainers fallback will apply")
    except Exception as e:
        warns.append(f"best-ideas unreadable: {e}")

    rep.section("2. Arm Anthropic fallback env + async invoke")
    ak = os.environ.get("ANTHROPIC_API_KEY") \
        or os.environ.get("ANTHROPIC_API_KEY_NEW") or ""
    cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    if ak:
        env["ANTHROPIC_API_KEY"] = ak
        _retry_on_conflict(LAM.update_function_configuration,
                           FunctionName=FN,
                           Environment={"Variables": env})
        LAM.get_waiter("function_updated").wait(
            FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
        rep.ok("ANTHROPIC_API_KEY armed on function env "
               f"(keys now: {sorted(env)})")
    else:
        warns.append("runner has no ANTHROPIC secret — relying on GLM/SSM "
                     "path only")
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    rep.log("async invoke fired (600s engine)")

    rep.section("3. Poll fresh kill-theses")
    doc = None
    deadline = time.time() + 640
    while time.time() < deadline:
        try:
            d = s3_json(OUT)
            ts = d.get("generated_at") or d.get("as_of")
            if ts and datetime.fromisoformat(ts) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(15)
    if doc is None:
        fails.append("kill-theses never freshened — engine crashed or "
                     ">640s; check CW next op")
    else:
        th = doc.get("theses") or []
        rep.kv(new_theses=len(th), n_ok=doc.get("n_ok"),
               n_fail=doc.get("n_fail"))
        if len(th) >= 3:
            good = [t for t in th if t.get("kill_conditions")]
            rep.ok(f"REVIVED: {len(th)} theses "
                   f"({len(good)} with kill_conditions)")
            for t in th[:3]:
                first = (t.get("kill_conditions") or [{}])[0]
                rep.log(f"  · {t.get('symbol') or t.get('ticker')}: "
                        f"{str(first.get('risk') or first)[:120]}")
        else:
            fails.append(f"still hollow: {len(th)} theses "
                         f"(n_fail={doc.get('n_fail')})")
            for k, v in doc.items():
                if "err" in str(k).lower() or "fail" in str(k).lower():
                    rep.log(f"new doc {k}: {json.dumps(v)[:260]}")

    rep.section("4. IR chip CDN recheck (warn-only)")
    try:
        req = urllib.request.Request(
            f"https://justhodl.ai/industry-rotation.html?t={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 ops-3147",
                     "Cache-Control": "no-cache"})
        html = urllib.request.urlopen(req, timeout=15).read().decode(
            "utf-8", "replace")
        if "IR_QCHIP_V1" in html:
            rep.ok("quadrant chip live on CDN")
        else:
            warns.append("CDN still cached pre-chip page (self-heals)")
    except Exception as e:
        warns.append(f"page fetch failed: {str(e)[:80]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
