#!/usr/bin/env python3
"""ops 2969 -- justhodl-asset-discovery: the monthly emerging-asset
discovery agent. One governed reason-tier LLM call per month over a
context built entirely from the platform's own engines (asset-compass
universe + macro forward, Finviz industry leaders/laggards, crypto
cycle), proposing up to 6 PROVISIONAL candidates that are NOT already
covered. Validator enforces ticker shape, universe exclusion and thesis
presence; the model may honestly return zero candidates; a gated router
(budget/mode) yields an honest empty document, never fabrication.

Sequence: (0) probe the three context feeds from the runner; (1) wait
for the parallel deploy-lambdas run to CREATE the function from
aws/lambdas/justhodl-asset-discovery (config.json: inherit_env=true
secrets bundle, monthly EventBridge rule asset-discovery-monthly at
06:30 UTC on the 1st) and assert env + rule + shared-module bundling;
(2) invoke synchronously (this month's first real run); (3) hard-verify
data/asset-discovery.json -- schema, llm_status in {OK, GATED_OR_DOWN}
(PARSE_FAIL fails), candidate well-formedness, universe exclusion,
monthly history object written.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=240, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
EV = boto3.client("events", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-asset-discovery"
RULE = "asset-discovery-monthly"
OUT_KEY = "data/asset-discovery.json"
CAND_KEYS = {"ticker", "name", "asset_class", "thesis",
             "structural_driver", "asymmetry_note", "risk",
             "confirming_data"}


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def fail(rep, fails, msg):
    fails.append(msg)
    rep.fail(msg)


def main():
    fails, warns = [], []
    hl = {}
    with report("2969_asset_discovery") as rep:

        rep.section("0. Context-feed probes from the runner")
        universe = set()
        try:
            ac = s3_json("data/asset-compass.json")
            universe = {a.get("ticker") for a in (ac.get("assets") or [])
                        if a.get("ticker")}
            rep.kv(compass_universe_n=len(universe))
            if len(universe) < 15:
                fail(rep, fails, "asset-compass universe thin: %d"
                     % len(universe))
        except Exception as e:
            fail(rep, fails, "cannot read asset-compass: %s" % e)
        try:
            fg = s3_json("data/finviz-groups.json")
            n = len(fg.get("industries") or [])
            rep.kv(finviz_industries=n)
            if n < 100:
                fail(rep, fails, "finviz-groups thin: %d industries" % n)
        except Exception as e:
            fail(rep, fails, "cannot read finviz-groups: %s" % e)
        try:
            cyc = s3_json("data/crypto-cycle-risk.json")
            rep.kv(crypto_cycle_present=bool(cyc))
        except Exception:
            warns.append("crypto-cycle-risk unavailable (context degrades "
                         "gracefully)")
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("1. Wait for deploy-lambdas to create %s" % FN)
        cfg = None
        for _ in range(45):
            try:
                cfg = LAM.get_function_configuration(FunctionName=FN)
                lm = datetime.fromisoformat(
                    cfg["LastModified"].replace("+0000", "+00:00"))
                age = (datetime.now(timezone.utc) - lm).total_seconds()
                if cfg.get("State") in ("Active", "Pending") and \
                        cfg.get("LastUpdateStatus") in ("Successful",
                                                        None) and age < 900:
                    break
            except LAM.exceptions.ResourceNotFoundException:
                cfg = None
            time.sleep(8)
        if not cfg:
            fail(rep, fails, "function never appeared -- deploy-lambdas "
                 "create failed or [skip-deploy] leaked into the commit")
            _write(rep, fails, warns, hl)
            return
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        rep.kv(state=cfg.get("State"), timeout_s=cfg.get("Timeout"),
               memory_mb=cfg.get("MemorySize"), env_n=len(env),
               env_keys=sorted(env.keys()))
        if not env.get("ANTHROPIC_API_KEY"):
            fail(rep, fails, "inherit_env did not deliver "
                 "ANTHROPIC_API_KEY -- router has no provider")
        try:
            r = EV.describe_rule(Name=RULE)
            rep.kv(rule=RULE, cron=r.get("ScheduleExpression"),
                   rule_state=r.get("State"))
            if "cron(30 6 1 * ? *)" not in (r.get("ScheduleExpression")
                                            or ""):
                warns.append("rule cron differs: %s"
                             % r.get("ScheduleExpression"))
        except Exception as e:
            fail(rep, fails, "monthly EventBridge rule missing: %s" % e)
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("2. Synchronous first run (this month's record)")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(invoke_seconds=round(time.time() - t0, 1),
               status=resp.get("StatusCode"), body=json.dumps(body)[:200])
        if resp.get("FunctionError"):
            fail(rep, fails, "invoke FunctionError: %s"
                 % json.dumps(body)[:300])
            _write(rep, fails, warns, hl)
            return

        rep.section("3. Hard verify %s" % OUT_KEY)
        try:
            d = s3_json(OUT_KEY)
        except Exception as e:
            fail(rep, fails, "cannot read %s: %s" % (OUT_KEY, e))
            _write(rep, fails, warns, hl)
            return
        hl["month"] = d.get("month")
        hl["llm_status"] = d.get("llm_status")
        hl["month_read"] = d.get("month_read")
        hl["candidates_n"] = len(d.get("candidates") or [])
        hl["dropped_n"] = len(d.get("dropped_by_validator") or [])
        rep.kv(**hl)
        age = None
        try:
            g = datetime.fromisoformat(
                d.get("generated_at", "").replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - g).total_seconds()
        except Exception:
            pass
        if age is None or age > 600:
            fail(rep, fails, "document stale/unfresh: age=%s" % age)
        if d.get("schema_version") != "1.0":
            fail(rep, fails, "schema %r" % d.get("schema_version"))
        if d.get("status") != "PROVISIONAL":
            fail(rep, fails, "status %r != PROVISIONAL" % d.get("status"))
        if d.get("llm_status") == "PARSE_FAIL":
            fail(rep, fails, "model returned non-JSON: %s"
                 % (d.get("raw_head") or "")[:160])
        elif d.get("llm_status") == "GATED_OR_DOWN":
            warns.append("router gated the call (budget/mode/providers) -- "
                         "document honest-empty; governance working as "
                         "designed, but no live-model proof this run")
        elif d.get("llm_status") != "OK":
            fail(rep, fails, "unexpected llm_status %r"
                 % d.get("llm_status"))
        cands = d.get("candidates") or []
        for c in cands:
            missing = sorted(CAND_KEYS - set(c.keys()))
            if missing:
                fail(rep, fails, "candidate %s missing %s"
                     % (c.get("ticker"), missing))
            if c.get("ticker") in universe:
                fail(rep, fails, "validator leak: %s is in the covered "
                     "universe" % c.get("ticker"))
        if d.get("llm_status") == "OK" and not (d.get("month_read")
                                                or "").strip():
            fail(rep, fails, "month_read empty on an OK run")
        hist_key = "discovery/history/%s.json" % (d.get("month") or "")
        try:
            hd = s3_json(hist_key)
            if hd.get("generated_at") != d.get("generated_at"):
                warns.append("history object differs from latest (multiple "
                             "runs this month -- expected on re-runs)")
            rep.kv(history=hist_key)
        except Exception as e:
            fail(rep, fails, "monthly history object missing (%s): %s"
                 % (hist_key, e))
        if cands:
            hl["tickers"] = [c["ticker"] for c in cands]
            rep.kv(tickers=hl["tickers"])
        if not fails:
            rep.ok("discovery live: %s llm=%s candidates=%d (%s) -- '%s'"
                   % (hl.get("month"), hl.get("llm_status"),
                      hl.get("candidates_n"),
                      ",".join(hl.get("tickers") or []) or "none",
                      (hl.get("month_read") or "")[:90]))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2969, "function": FN, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2969.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    rep.log("report written: %s" % rp)
    if fails:
        sys.exit(1)   # run-ops keys on the exit code (2966 convention)


main()
sys.exit(0)
