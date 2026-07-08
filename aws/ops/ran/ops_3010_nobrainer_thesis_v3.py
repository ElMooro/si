#!/usr/bin/env python3
"""ops 3010 -- Nobrainer Hunter L5, round 3 (VERIFY-ONLY, no deploy step):
round 2 (ops 3009) re-broke the Lambda by ALSO deploying via the local
ops-helper zipfile, which does not bundle aws/shared -- clobbering the
correct deploy-lambdas.yml deploy that had already succeeded seconds
earlier on the SAME push (run 28979683625, confirmed green). Structural
fix: this script does NOT deploy at all. Any fn importing aws/shared
(nearly all of them) must be deployed ONLY via deploy-lambdas.yml --
never mix that with an ops-script deploy_lambda() call in the same
push. Original round-2 root-cause analysis stands unchanged: root cause of the 0-ok/12-fail
board was TWO bugs stacked. (1) Selection: compound lane had no cap and
synthesized blank tier-0 megacap entries -- fixed round 1 (verified via
manual deploy-lambdas dispatch: real tier-2/3 mix, n_compound=1/n_real=6
of 7). (2) LLM path: llm_router.complete() returns "" BY DESIGN when the
fleet cost-governance kill-switch is in economy/off mode or the daily
budget is spent (SSM /justhodl/llm/mode) -- this engine treated that as
a hard failure instead of the fleet-standard deterministic fallback
every other engine uses. Added deterministic_thesis(): a real numeric
read built from Layer-4 factors/fundamentals/supply-signals, clearly
marked non-LLM, so the board can never render 0/N dead cards again
regardless of budget state. Verifies thesis_text length + real mix,
not LLM-specifically -- and reports the live governance mode for
context."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

SSM = boto3.client("ssm", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-nobrainer-rationale"
OUT_KEY = "data/nobrainers-rationale.json"


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    with report("3010_nobrainer_thesis_v3") as rep:
        rep.section("2. Regenerate (Event invoke + S3 poll)")
        prev_gen = ""
        try:
            prev_gen = s3_json(OUT_KEY).get("generated_at", "")
        except Exception:
            pass
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        fresh = None
        for i in range(36):                      # up to 12 min
            time.sleep(20)
            try:
                d = s3_json(OUT_KEY)
                if d.get("generated_at", "") > prev_gen:
                    fresh = d
                    break
            except Exception:
                continue
        if not fresh:
            fails.append("no fresh %s after 12min" % OUT_KEY)
            _write(rep, fails, warns, {})
            sys.exit(1)
        age_min = None
        try:
            age_min = round((datetime.now(timezone.utc) -
                             datetime.fromisoformat(
                                 fresh["generated_at"])).total_seconds()
                            / 60.0, 1)
        except Exception:
            pass

        rep.section("3. Verify the board")
        theses = fresh.get("theses") or []
        n_ok = fresh.get("n_claude_ok")
        n_fail = fresh.get("n_claude_fail")
        flags = [t.get("flag") or (t.get("candidate") or {}).get("flag")
                 for t in theses]
        n_comp = sum(1 for f in flags if f == "COMPOUND_PRIORITY")
        n_real = len(theses) - n_comp
        long_theses = sum(1 for t in theses
                          if (t.get("thesis_chars") or 0) > 300)
        errs = [t.get("error") for t in theses if t.get("error")]
        with_fund = sum(1 for t in theses
                        if ((t.get("candidate") or {}).get("fundamentals")
                            or {}).get("market_cap") is not None)
        comp_with_sys = sum(
            1 for t in theses
            if (t.get("candidate") or {}).get("_compound_priority")
            and (t.get("candidate") or {}).get("_compound_systems"))
        mu = set()
        try:
            l4 = s3_json("data/nobrainers.json")
            mu = {m.get("ticker") for m in
                  (l4.get("summary") or {}).get("mu_grade_top_15", [])}
        except Exception:
            pass
        n_mu = sum(1 for t in theses if t.get("ticker") in mu)
        rep.kv(n_theses=len(theses), n_claude_ok=n_ok, n_claude_fail=n_fail,
               n_compound=n_comp, n_real_candidates=n_real,
               n_mu_grade=n_mu, n_long_theses=long_theses,
               n_with_fundamentals=with_fund,
               comp_with_systems=comp_with_sys, age_min=age_min,
               sample_errors=json.dumps(errs[:2])[:300],
               tickers=[t.get("ticker") for t in theses])
        if not theses:
            fails.append("empty theses")
        n_det = fresh.get("n_deterministic") or 0
        n_dead = sum(1 for t in theses
                    if (t.get("thesis_chars") or 0) < 50)
        if n_dead > 0:
            fails.append("%d thesis card(s) still effectively empty "
                         "(<50 chars) despite deterministic fallback"
                         % n_dead)
        if (n_ok or 0) == 0 and (n_det or 0) == 0:
            fails.append("neither LLM nor deterministic fallback produced "
                         "any thesis text")
        rep.kv(n_deterministic=n_det)
        try:
            mode = SSM.get_parameter(
                Name="/justhodl/llm/mode")["Parameter"]["Value"]
            budget = SSM.get_parameter(
                Name="/justhodl/llm/daily-budget-usd")["Parameter"]["Value"]
            rep.kv(llm_mode=mode, llm_daily_budget=budget)
            if mode != "normal" and n_det > 0:
                warns.append("llm mode=%s explains the %d deterministic "
                             "fallback(s) -- working as designed" %
                             (mode, n_det))
        except Exception as e:
            rep.kv(llm_mode_check="unavailable: %s" % str(e)[:100])
        if n_comp > 5:
            fails.append("compound lane still owns the board (%d)" % n_comp)
        if n_real < 5:
            fails.append("too few real nobrainer candidates (%d)" % n_real)
        if n_mu < 2:
            warns.append("MU-grade representation low (%d)" % n_mu)
        if long_theses < max(1, len(theses) - 4):
            fails.append("theses too short/empty (%d long of %d)"
                         % (long_theses, len(theses)))
        if with_fund < n_real:
            warns.append("some real candidates missing fundamentals "
                         "(%d/%d)" % (with_fund, n_real))

        rep.section("4. Live page checks (warn-level, pages CDN lag)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/nobrainers.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3007",
                         "Cache-Control": "no-cache"})
            page = urllib.request.urlopen(req, timeout=25
                                          ).read().decode("utf-8", "replace")
            kit_req = urllib.request.Request(
                "https://justhodl.ai/interp-kit.js?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3007"})
            kit = urllib.request.urlopen(kit_req, timeout=25
                                         ).read().decode("utf-8", "replace")
            ok_page = "Multi-system compound signal" in page
            ok_kit = "ne.value != null" in kit
            rep.kv(page_has_compound_block=ok_page,
                   kit_has_guard=ok_kit)
            if not (ok_page and ok_kit):
                warns.append("pages not propagated yet (page=%s kit=%s)"
                             % (ok_page, ok_kit))
        except Exception as e:
            warns.append("live page check: %s" % str(e)[:120])

        rep.section("verdict")
        _write(rep, fails, warns,
               {"n_ok": n_ok, "n_fail": n_fail, "n_compound": n_comp,
                "n_real": n_real, "n_mu": n_mu})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- thesis board regenerated with real tier-2/3 mix")


def _write(rep, fails, warns, extra):
    payload = {"ops": 3010, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3010.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
