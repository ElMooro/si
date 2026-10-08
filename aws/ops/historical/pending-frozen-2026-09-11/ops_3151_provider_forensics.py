"""ops 3151 — provider forensics: which leg is dead, verbatim.

Router chain (read from source): reason-tier → GLM (SSM zai key) →
on failure trips breaker, prints "[llm_router] GLM failed (...)", falls
back to HAIKU with the env ANTHROPIC key → on failure prints "ALL
providers down (...)" → "". Both prints land in CloudWatch — 3150's run
holds the verbatim answer already.

THIS OP:
  1. Pull premortem CW lines (llm_router pattern) from the 3150 window.
  2. Runner-side probes: GLM chat w/ SSM key; Anthropic w/ BOTH runner
     secrets (API_KEY and API_KEY_NEW) — statuses + bodies verbatim.
  3. If Anthropic _NEW is the live one and the function env carries a
     dead key → swap env to the live key; nonce-recycle; invoke; gate
     ≥5 rich theses.
  4. If GLM alone is dead but Haiku fallback works after key swap →
     that's the fix; if BOTH provider families are dead (credits/keys)
     → verbatim report, no fake pass.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

from ops_report import report
from _lambda_deploy_helpers import _retry_on_conflict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-premortem-engine"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def http_json(url, payload, headers, timeout=25):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)


with report("3151_provider_forensics") as rep:
    fails, warns = [], []
    rep.heading("ops 3151 — LLM provider forensics")

    rep.section("1. CW verbatim from the 3150 run")
    try:
        ev = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}",
            startTime=int(time.time() * 1000) - 12 * 60 * 1000,
            filterPattern='"llm_router"', limit=12)
        lines = [e["message"].strip() for e in ev.get("events") or []]
        for ln in lines[-6:]:
            rep.log(f"CW: {ln[:240]}")
        if not lines:
            rep.log("CW: no llm_router lines in window")
    except Exception as e:
        warns.append(f"CW read: {str(e)[:90]}")

    rep.section("2. Provider probes (runner-side, verbatim)")
    zai = ""
    try:
        zai = SSM.get_parameter(Name="/justhodl/zai-api-key",
                                WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        rep.log(f"zai SSM: {str(e)[:100]}")
    if zai:
        st, body = http_json(
            "https://api.z.ai/api/paas/v4/chat/completions",
            {"model": "glm-5.1", "max_tokens": 20,
             "messages": [{"role": "user", "content": "reply OK"}]},
            {"Content-Type": "application/json",
             "Authorization": f"Bearer {zai}"})
        rep.log(f"GLM probe: HTTP {st} · {body[:180]}")
        glm_ok = (st == 200 and '"content"' in body)
    else:
        glm_ok = False
        rep.log("GLM probe skipped — no SSM key")

    live_anthropic = None
    for name in ("ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY_NEW"):
        k = os.environ.get(name, "")
        if not k:
            rep.log(f"{name}: runner secret empty")
            continue
        st, body = http_json(
            "https://api.anthropic.com/v1/messages",
            {"model": "claude-haiku-4-5-20251001", "max_tokens": 16,
             "messages": [{"role": "user", "content": "reply OK"}]},
            {"Content-Type": "application/json", "x-api-key": k,
             "anthropic-version": "2023-06-01"})
        ok = (st == 200)
        rep.log(f"{name}: HTTP {st} · {body[:160]}")
        if ok and not live_anthropic:
            live_anthropic = (name, k)
    rep.kv(glm_ok=glm_ok,
           anthropic_live=(live_anthropic[0] if live_anthropic else None))

    rep.section("3. Conditional fix + gate")
    if not glm_ok and not live_anthropic:
        fails.append("BOTH provider families dead (verbatim above) — key "
                     "rotation / credits are PENDING-KHALID; kill-theses "
                     "self-populates the moment either leg is funded")
    else:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        changed = False
        if live_anthropic and env.get("ANTHROPIC_API_KEY") != live_anthropic[1]:
            env["ANTHROPIC_API_KEY"] = live_anthropic[1]
            changed = True
            rep.ok(f"function env → live {live_anthropic[0]}")
        env["LLM_CFG_NONCE"] = str(int(time.time()))
        _retry_on_conflict(LAM.update_function_configuration,
                           FunctionName=FN,
                           Environment={"Variables": env})
        LAM.get_waiter("function_updated").wait(
            FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
        t0 = datetime.now(timezone.utc)
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        rep.log("cold invoke fired"
                + (" (env key swapped)" if changed else ""))
        doc = None
        deadline = time.time() + 660
        while time.time() < deadline:
            try:
                d = s3_json("data/kill-theses.json")
                ts = d.get("generated_at") or d.get("as_of")
                if ts and datetime.fromisoformat(ts) >= t0:
                    doc = d
                    break
            except Exception:
                pass
            time.sleep(15)
        if doc is None:
            fails.append("kill-theses never freshened")
        else:
            th = [t for t in (doc.get("theses") or []) if isinstance(t, dict)]
            rich = [t for t in th if t.get("kill_conditions")]
            errs = [t for t in th if t.get("error")]
            rep.kv(theses=len(th), rich=len(rich), row_errors=len(errs))
            if errs:
                rep.log(f"error sample: {json.dumps(errs[0])[:200]}")
            if len(rich) >= 5:
                rep.ok(f"KILL PIPELINE LIVE: {len(rich)}/{len(th)}")
                for t in rich[:4]:
                    kc = (t.get("kill_conditions") or [{}])[0]
                    rep.log(f"  · {t.get('symbol')}: "
                            f"{str(kc.get('risk') or kc.get('condition') or kc)[:140]}")
            else:
                fails.append(f"{len(rich)} rich with a proven-live provider "
                             "— residual is engine-side; sample above")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
