#!/usr/bin/env python3
"""ops 3008 -- Nobrainer Hunter L5 VERIFY (deploy done via deploy-lambdas.yml run 28977172591, which bundles aws/shared -- the 3007 helper-deploy shipped a shared-less package that crashed on import anthropic_shim)
and 100% occupied by synthesized tier-0 compound megacaps with blank
fundamentals. Deploys the rebalanced rationale engine (compound lane capped
at 4, MU-grade floor of 3, all_scored fundamentals join, resilient LLM
chain compat->direct->llm_router), regenerates, and hard-verifies the
board: theses actually written, real tier-2/3 mix, metrics present."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

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
    with report("3008_nobrainer_thesis_verify") as rep:
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
        if (n_ok or 0) < max(1, len(theses) - 3):
            fails.append("LLM still failing: ok=%s fail=%s errs=%s"
                         % (n_ok, n_fail, errs[:2]))
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
    payload = {"ops": 3008, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3008.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
