"""
ops_5260 — CHAIN GUARD ROLLOUT: no walk engine can reach AWS's 16-hop breaker again

Evidence (ops 5250, 2026-09-09): justhodl-worldbank-full tripped AWS's
recursion breaker 6× (Aug-31 ×3, Sep-7 ×3 — every Monday walk, three
16-hop lineages each). Twelve walk engines self-chain with their OWN caps of
30–220 hops, all above the 16 at which AWS drops the invocation and mails a
Health event. Cost of the event: $0.61 for worldbank-full since Sep 1. The
damage is correctness (walks silently cut at hop 16) and a Health mail
every time.

Fix (structural, fleet-wide, engines keep their protocol):
  aws/shared/chain_guard.py — every self Event-invoke now goes through
  chain_invoke(): hops 1..12 chain as before; at hop 12 the engine PARKS a
  resume ticket under data/_state/chain-parked/<fn>/ instead of invoking
  itself. justhodl-chain-resumer (Scheduler rate(5 minutes)) re-invokes it
  from a fresh lineage. AWS's breaker stays ON as the net for a real bug.
  tests/deployment/test_chain_guard.py gates every future deploy: an
  unguarded self-chain cannot ship.

This op:
  1. Deploys justhodl-chain-resumer + its schedule (Scheduler, never a new
     classic rule — the rule cap is saturated).
  2. Redeploys the 12 patched walkers from this checkout; settles each by
     the marker INSIDE the deployed zip, never by State==Active.
  3. Proves the resume path live with a synthetic ticket that resumes the
     harmless notifier ({"test": true}) — list → invoke → delete → ledger.
  4. Telegram: the ops-5250 delivery test got HTTP 401 from the SSM bot
     token. Validates the SSM token and the runner secret against getMe
     (prints bot username only, never a token) and heals SSM from the
     secret if the secret is the valid one; re-tests delivery.
  5. Error storm: ops 5250 measured account Errors p95 = 834/hour (fleet was
     13 errors/6h on Sep 2). Names the erroring functions with a sample
     error line each. READ-ONLY — many were redeployed by the Codex lane
     today; reported, not touched.
  6. S3 loop surface: justhodl-crypto-fanin (aws.s3 rule → crypto-intel):
     rule pattern vs the engine's own write keys.

Report: aws/ops/reports/latest/5260_chain_guard_rollout.md
"""
import io
import json
import os
import re
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

REGION = "us-east-1"
ACCT = "857687956942"
BUCKET = "justhodl-dashboard-live"
SCHED_ROLE = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCT
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
ssm = boto3.client("ssm", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)

ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
NOW = datetime.now(timezone.utc)
OUT = {"ops": 5260, "ts": NOW.isoformat()}
RESUMER = "justhodl-chain-resumer"
NOTIFY = "justhodl-guardrail-notify"
WALKERS = ["justhodl-worldbank-full", "justhodl-bls-full", "justhodl-ecb-deep", "justhodl-finra-full",
           "justhodl-fiscaldata-full", "justhodl-fred-catalog", "justhodl-gdelt-full", "justhodl-hist-banker",
           "justhodl-imf-full", "justhodl-polygon-full", "justhodl-sec-midas", "justhodl-trend-reversal"]
MARKER = "chain_guard.begin(event)"


def wait_settled(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return c
        time.sleep(4)
    return lam.get_function_configuration(FunctionName=fn)


def deployed_has_marker(fn, marker, tries=20):
    for _ in range(tries):
        try:
            loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
            raw = urllib.request.urlopen(loc, timeout=60).read()
            zf = zipfile.ZipFile(io.BytesIO(raw))
            src = zf.read("lambda_function.py").decode("utf-8", "ignore")
            has_shared = "chain_guard.py" in zf.namelist()
            if marker in src and has_shared:
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def getme(token):
    try:
        req = urllib.request.Request("https://api.telegram.org/bot%s/getMe" % token,
                                     headers={"User-Agent": "justhodl-ops-5260"})
        with urllib.request.urlopen(req, timeout=10) as r:
            j = json.loads(r.read())
            return j.get("ok") and (j.get("result") or {}).get("username")
    except Exception as e:  # noqa: BLE001
        return "ERR %s" % str(e)[:40]


with report("5260_chain_guard_rollout") as rep:
    rep.heading("ops 5260 — chain_guard rollout (12 walk engines) + chain resumer + Telegram heal + error-storm forensics")
    fails = []

    # ---------------------------------------------------------------- 1
    rep.section("1. justhodl-chain-resumer + Scheduler rate(5 minutes)")
    try:
        cfgj = json.loads((ROOT / "aws" / "lambdas" / RESUMER / "config.json").read_text())
        create_or_update_lambda(report=rep, function_name=RESUMER,
                                zip_bytes=build_zip(ROOT / "aws" / "lambdas" / RESUMER / "source"),
                                env_vars=dict(cfgj["env"]), timeout=int(cfgj["timeout"]), memory=int(cfgj["memory"]),
                                description=cfgj["description"][:250], reserved_concurrency=None,
                                create_function_url=False, ephemeral_storage=None)
        c = wait_settled(RESUMER)
        rarn = c["FunctionArn"]
        try:
            lam.put_function_concurrency(FunctionName=RESUMER, ReservedConcurrentExecutions=1)
            rep.ok("  reserved concurrency 1 (one resumer at a time)")
        except Exception as e:
            rep.warn("  reserved concurrency: %s" % str(e)[:100])
        sc = cfgj["scheduler"]
        tgt = {"Arn": rarn, "RoleArn": sc["role"], "Input": json.dumps(sc["input"]),
               "RetryPolicy": {"MaximumRetryAttempts": 0}}
        try:
            sch.get_schedule(Name=sc["name"], GroupName="default")
            sch.update_schedule(Name=sc["name"], GroupName="default", ScheduleExpression=sc["expr"],
                                ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"},
                                Target=tgt, State="ENABLED", Description=cfgj["description"][:200])
            rep.ok("  schedule %s updated %s" % (sc["name"], sc["expr"]))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=sc["name"], GroupName="default", ScheduleExpression=sc["expr"],
                                ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"},
                                Target=tgt, State="ENABLED", Description=cfgj["description"][:200])
            rep.ok("  schedule %s created %s" % (sc["name"], sc["expr"]))
        OUT["resumer"] = rarn
    except Exception as e:
        fails.append("resumer: %s" % str(e)[:160])
        rep.fail("  resumer: %s" % str(e)[:160])

    # ---------------------------------------------------------------- 2
    rep.section("2. Redeploy the 12 patched walkers (code only; env/config untouched)")
    deployed = {}
    for fn in WALKERS:
        try:
            src = (ROOT / "aws" / "lambdas" / fn / "source" / "lambda_function.py").read_text()
            if MARKER not in src or "chain_guard.chain_invoke(" not in src:
                fails.append("%s: checkout is not patched" % fn)
                rep.fail("  %s checkout NOT patched — skipped" % fn)
                continue
            before = lam.get_function_configuration(FunctionName=fn)
            wait_settled(fn)
            zb = build_zip(ROOT / "aws" / "lambdas" / fn / "source")
            for attempt in range(6):
                try:
                    lam.update_function_code(FunctionName=fn, ZipFile=zb)
                    break
                except lam.exceptions.ResourceConflictException:
                    time.sleep(10)
            c = wait_settled(fn)
            ok = deployed_has_marker(fn, MARKER)
            deployed[fn] = {"ok": ok, "size": len(zb), "mem": c.get("MemorySize"), "timeout": c.get("Timeout"),
                            "modified": c.get("LastModified"), "env_keys_before": len((before.get("Environment") or {}).get("Variables") or {}),
                            "env_keys_after": len((c.get("Environment") or {}).get("Variables") or {})}
            (rep.ok if ok else rep.fail)("  %-28s %s  %dKB  %sMB/%ss  env %d→%d" % (
                fn, "SETTLED (marker + chain_guard.py in zip)" if ok else "MARKER MISSING",
                len(zb) // 1024, c.get("MemorySize"), c.get("Timeout"),
                deployed[fn]["env_keys_before"], deployed[fn]["env_keys_after"]))
            if not ok:
                fails.append("%s marker never appeared" % fn)
            if deployed[fn]["env_keys_before"] != deployed[fn]["env_keys_after"]:
                fails.append("%s env changed during code deploy" % fn)
        except Exception as e:
            fails.append("%s: %s" % (fn, str(e)[:120]))
            rep.fail("  %s: %s" % (fn, str(e)[:140]))
    OUT["deployed"] = deployed
    rep.log("  deployed: %d/%d" % (sum(1 for v in deployed.values() if v["ok"]), len(WALKERS)))

    # ---------------------------------------------------------------- 3
    rep.section("3. Live proof of the resume path (synthetic ticket → notifier test)")
    try:
        key = "data/_state/chain-parked/%s/ops5260-proof.json" % NOTIFY
        s3.put_object(Bucket=BUCKET, Key=key, ContentType="application/json",
                      Body=json.dumps({"version": "1.0.0", "function": NOTIFY, "payload": {"test": True},
                                       "hops_walked": 12, "reason": "ops5260-proof",
                                       "parked_at": NOW.isoformat()}).encode())
        r = lam.invoke(FunctionName=RESUMER, InvocationType="RequestResponse", Payload=b'{"mode":"resume"}')
        body = json.loads(r["Payload"].read() or b"{}")
        rep.log("  resumer: %s" % json.dumps({k: v for k, v in body.items() if k != "version"})[:400])
        gone = False
        try:
            s3.head_object(Bucket=BUCKET, Key=key)
        except Exception:
            gone = True
        resumed = any(x.get("function") == NOTIFY for x in body.get("resumed", []))
        (rep.ok if (resumed and gone) else rep.fail)("  ticket resumed=%s deleted=%s errors=%d" % (resumed, gone, len(body.get("errors", []))))
        if not (resumed and gone):
            fails.append("resume path proof failed")
        time.sleep(4)
        led = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ops/guardrails/latest.json")["Body"].read())
        rep.log("  notifier ledger latest: alarm=%s telegram_ok=%s info=%s" % (
            (led.get("alarm") or {}).get("AlarmName"), led.get("telegram_ok"), led.get("telegram_info")))
        OUT["resume_proof"] = {"resumed": resumed, "deleted": gone, "notifier_ledger": led.get("at")}
    except Exception as e:
        fails.append("resume proof: %s" % str(e)[:140])
        rep.fail("  resume proof: %s" % str(e)[:140])

    # ---------------------------------------------------------------- 4
    rep.section("4. Telegram delivery — validate SSM token vs runner secret, heal SSM if needed")
    try:
        ssm_tok = ""
        try:
            ssm_tok = ssm.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            rep.warn("  SSM /justhodl/telegram/bot_token: %s" % str(e)[:100])
        sec_tok = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        ssm_me = getme(ssm_tok) if ssm_tok else "absent"
        sec_me = getme(sec_tok) if sec_tok else "absent"
        rep.log("  SSM token → getMe: %s   | runner secret → getMe: %s   | same value: %s" % (
            ssm_me, sec_me, bool(ssm_tok and sec_tok and ssm_tok == sec_tok)))
        healed = False
        if (not ssm_me or str(ssm_me).startswith("ERR")) and sec_me and not str(sec_me).startswith("ERR") and sec_me != "absent":
            ssm.put_parameter(Name="/justhodl/telegram/bot_token", Value=sec_tok, Type="SecureString", Overwrite=True)
            healed = True
            rep.ok("  SSM bot_token overwritten from the runner secret (bot @%s) — every Telegram consumer heals on its next call" % sec_me)
        try:
            chat = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
            rep.log("  SSM chat_id present: %s" % ("yes" if chat else "EMPTY"))
        except Exception as e:
            rep.warn("  SSM chat_id: %s" % str(e)[:80])
        r = lam.invoke(FunctionName=NOTIFY, InvocationType="RequestResponse", Payload=b'{"test": true}')
        body = json.loads(r["Payload"].read() or b"{}")
        (rep.ok if body.get("telegram_ok") else rep.warn)("  delivery test after heal: telegram_ok=%s info=%s" % (body.get("telegram_ok"), body.get("telegram_info")))
        OUT["telegram"] = {"ssm_getme": ssm_me, "secret_getme": sec_me, "healed": healed, "delivery_ok": body.get("telegram_ok")}
        if not body.get("telegram_ok"):
            rep.warn("  Telegram still failing: the bot token in BOTH SSM and the runner secret is invalid → Khalid must paste a fresh token from @BotFather into SSM /justhodl/telegram/bot_token (and GitHub secret TELEGRAM_BOT_TOKEN)")
    except Exception as e:
        rep.warn("  telegram: %s" % str(e)[:140])

    # ---------------------------------------------------------------- 5
    rep.section("5. Error storm — which functions carry the errors (24h and 7d), sample line each. READ-ONLY")
    try:
        fns = []
        for page in lam.get_paginator("list_functions").paginate():
            fns.extend(f["FunctionName"] for f in page["Functions"])
        err7, err24 = {}, {}
        for i in range(0, len(fns), 240):
            chunk = fns[i:i + 240]
            q = []
            for j, fn in enumerate(chunk):
                q.append({"Id": "e%d" % j, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Errors",
                          "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 86400, "Stat": "Sum"}, "ReturnData": True})
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=NOW - timedelta(days=7), EndTime=NOW, ScanBy="TimestampDescending")
            for x in res["MetricDataResults"]:
                fn = chunk[int(x["Id"][1:])]
                if x["Values"]:
                    err7[fn] = sum(x["Values"])
                    err24[fn] = x["Values"][0] if x["Timestamps"] and x["Timestamps"][0] > NOW - timedelta(hours=26) else 0
        tot7 = sum(err7.values())
        tot24 = sum(err24.values())
        rep.log("  fleet errors: 7d=%d  last-24h=%d  functions with errors=%d" % (tot7, tot24, sum(1 for v in err7.values() if v > 0)))
        top = sorted(err7.items(), key=lambda x: -x[1])[:15]
        for fn, v in top:
            rep.warn("  %-44s err7d=%-7d err24h=%-6d" % (fn[:44], v, err24.get(fn, 0)))
            rep.kv(section="errors", function=fn, err_7d=int(v), err_24h=int(err24.get(fn, 0)))
        samples = {}
        for fn, v in top[:8]:
            try:
                r = logs.filter_log_events(logGroupName="/aws/lambda/" + fn,
                                           startTime=int((NOW - timedelta(hours=6)).timestamp() * 1000),
                                           filterPattern="?Error ?Exception ?Traceback ?Task timed out ?errorMessage",
                                           limit=3)
                lines = [e["message"].strip().replace("\n", " | ")[:220] for e in r.get("events", [])]
                samples[fn] = lines
                for ln in lines[:2]:
                    rep.log("     %s: %s" % (fn[:30], ln))
            except Exception as e:
                rep.log("     %s: logs %s" % (fn[:30], str(e)[:60]))
        OUT["errors"] = {"tot_7d": int(tot7), "tot_24h": int(tot24), "top": [(k, int(v)) for k, v in top], "samples": samples}
    except Exception as e:
        rep.warn("  error forensics: %s" % str(e)[:140])

    # ---------------------------------------------------------------- 6
    rep.section("6. S3 loop surface — justhodl-crypto-fanin (aws.s3 rule → justhodl-crypto-intel)")
    try:
        rule = evb.describe_rule(Name="justhodl-crypto-fanin")
        pat = json.loads(rule.get("EventPattern") or "{}")
        rep.log("  rule state=%s pattern=%s" % (rule.get("State"), json.dumps(pat)[:400]))
        trig_keys = []
        det = pat.get("detail") or {}
        obj = det.get("object") or {}
        for k in obj.get("key") or []:
            trig_keys.append(k if isinstance(k, str) else json.dumps(k))
        src = (ROOT / "aws" / "lambdas" / "justhodl-crypto-intel" / "source" / "lambda_function.py").read_text(errors="ignore")
        writes = sorted(set(re.findall(r"put_object\([^)]*?Key\s*=\s*['\"]([^'\"]+)['\"]", src, re.S)))
        rep.log("  engine write keys (static): %s" % ", ".join(writes)[:300])
        overlap = [w for w in writes for t in trig_keys if (t in w) or (w in t)]
        (rep.fail if overlap else rep.ok)("  trigger keys %s  ∩ write keys → %s" % (trig_keys, overlap or "NONE (no S3 loop)"))
        OUT["crypto_fanin"] = {"trigger_keys": trig_keys, "writes": writes, "overlap": overlap}
    except Exception as e:
        rep.warn("  crypto-fanin: %s" % str(e)[:120])

    # ---------------------------------------------------------------- verdict
    rep.section("VERDICT")
    rep.log("walkers guarded: %d/12; resumer: %s; resume proof: %s; telegram: %s" % (
        sum(1 for v in deployed.values() if v["ok"]), OUT.get("resumer", "FAILED"),
        (OUT.get("resume_proof") or {}).get("resumed"), (OUT.get("telegram") or {}).get("delivery_ok")))
    rep.log("next: ops 5261 (24h gate) — RecursiveInvocationsDropped must stay 0 while the resumer ledger shows parks/resumes")
    if fails:
        for f in fails:
            rep.fail("  ! %s" % f)
    OUT["fails"] = fails
    (ROOT / "aws" / "ops" / "reports" / "5260_chain_guard_rollout.json").write_text(json.dumps(OUT, indent=1, default=str))
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
