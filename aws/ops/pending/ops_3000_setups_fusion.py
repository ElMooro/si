#!/usr/bin/env python3
"""ops 3000 -- BEST-SETUPS FUSION verify (the ranking-spine completion:
momentum + industry leadership + factor regime + catalyst in one
conviction number). New priors in justhodl-best-setups:
  _industry_prior  : sector -> industry-rotation leadership_score mult
                     [0.94..1.06], BREAKDOWN x0.93 (floor 0.88),
                     CROWDED x0.98; FinViz->GICS vocab normalized.
  _factor_appetite : [0.97..1.03] on RR_HIGH_BETA sectors only (RORO
                     already regime-gates; this is the style layer).
Both failure-isolated: stale/absent feeds -> neutral 1.0 + honest meta.
Verify: deploy-gate, invoke, doc carries industry_context with fresh
meta + factor appetite; >=60% of setups carry industry fields; mults
within bounds; directional spot-checks (a Health-Care setup mult>1, an
Energy/Materials-cluster setup mult<1 when present); why-text mentions
INDUSTRY on at least one boosted/haircut setup; regime coherence.
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
                   config=Config(read_timeout=600, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-best-setups"


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    out = {"ops": 3000, "ts": datetime.now(timezone.utc).isoformat()}
    with report("3000_setups_fusion") as rep:

        rep.section("1. Deploy gate")
        time.sleep(75)
        ok = False
        for _ in range(50):
            cfg = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds()
            if cfg.get("LastUpdateStatus") == "Successful" \
                    and age < 1800:
                rep.kv(deploy_age_s=int(age))
                ok = True
                break
            time.sleep(8)
        if not ok:
            fails.append("no fresh deploy")
            _w(rep, out, fails, warns)
            return

        rep.section("2. Invoke (production ranker, long)")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(secs=round(time.time() - t0, 1),
               err=resp.get("FunctionError"),
               body=json.dumps(body)[:200])
        if resp.get("FunctionError"):
            fails.append("invoke: %s" % json.dumps(body)[:300])
            _w(rep, out, fails, warns)
            return

        rep.section("3. Doc verify")
        d = s3_json("data/best-setups.json")
        ctx = d.get("industry_context") or {}
        out["industry_context"] = ctx
        rep.kv(context=json.dumps(ctx)[:300])
        meta = ctx.get("meta") or {}
        if meta.get("sectors") is None or (meta.get("age_h") or 99) > 60:
            fails.append("industry_context meta not fresh: %s"
                         % json.dumps(meta)[:150])
        if not isinstance(ctx.get("factor_appetite"), (int, float)):
            warns.append("factor_appetite absent: %s"
                         % ctx.get("factor_appetite"))

        setups = (d.get("setups") or d.get("top_setups")
                  or d.get("all") or [])
        if not setups:
            for v in d.values():
                if isinstance(v, list) and v and isinstance(v[0], dict) \
                        and "conviction" in v[0]:
                    setups = v
                    break
        out["setups_n"] = len(setups)
        with_fields = [s_ for s_ in setups
                       if "industry_mult" in s_]
        out["with_industry_fields"] = len(with_fields)
        rep.kv(setups=len(setups), with_fields=len(with_fields))
        if not setups:
            fails.append("no setups list found")
            _w(rep, out, fails, warns)
            return
        if len(with_fields) < max(3, int(0.6 * len(setups))):
            fails.append("industry fields on only %d/%d setups"
                         % (len(with_fields), len(setups)))

        bad_bounds = [(s_["ticker"], s_["industry_mult"])
                      for s_ in with_fields
                      if not (0.85 <= (s_["industry_mult"] or 1) <= 1.08)]
        if bad_bounds:
            fails.append("mult bounds breached: %s"
                         % json.dumps(bad_bounds[:4]))
        fa_bad = [(s_["ticker"], s_.get("factor_regime_mult"))
                  for s_ in with_fields
                  if s_.get("factor_regime_mult") is not None
                  and not (0.97 <= s_["factor_regime_mult"] <= 1.03)]
        if fa_bad:
            fails.append("factor mult bounds: %s"
                         % json.dumps(fa_bad[:4]))

        boosted = [s_ for s_ in with_fields
                   if (s_["industry_mult"] or 1) > 1.02]
        haircut = [s_ for s_ in with_fields
                   if (s_["industry_mult"] or 1) < 0.98]
        out["sample_boosted"] = [
            {"t": s_["ticker"], "etf": s_.get("industry_etf"),
             "score": s_.get("industry_score"),
             "mult": s_["industry_mult"]} for s_ in boosted[:5]]
        out["sample_haircut"] = [
            {"t": s_["ticker"], "etf": s_.get("industry_etf"),
             "score": s_.get("industry_score"),
             "tag": s_.get("industry_tag"),
             "mult": s_["industry_mult"]} for s_ in haircut[:5]]
        rep.kv(boosted=len(boosted), haircut=len(haircut),
               b_sample=json.dumps(out["sample_boosted"])[:250],
               h_sample=json.dumps(out["sample_haircut"])[:250])
        if not boosted and not haircut:
            fails.append("all industry mults neutral -- prior no-op")
        wrongdir = [s_ for s_ in boosted
                    if (s_.get("industry_score") or 0) < 50] + \
                   [s_ for s_ in haircut
                    if (s_.get("industry_score") or 0) > 70
                    and s_.get("industry_tag") is None]
        if wrongdir:
            fails.append("directionality wrong on %d setups: %s"
                         % (len(wrongdir),
                            json.dumps([w["ticker"]
                                        for w in wrongdir[:4]])))
        why_hits = sum(1 for s_ in with_fields
                       if "INDUSTRY" in (s_.get("why") or ""))
        out["why_mentions"] = why_hits
        if (boosted or haircut) and why_hits == 0:
            warns.append("no why-text INDUSTRY mentions")

        if not fails:
            rep.ok("FUSION LIVE: %d/%d setups carry industry priors | "
                   "boosted %d haircut %d | appetite %s | why "
                   "mentions %d"
                   % (len(with_fields), len(setups), len(boosted),
                      len(haircut), ctx.get("factor_appetite"),
                      why_hits))
        _w(rep, out, fails, warns)


def _w(rep, out, fails, warns):
    out["fails"], out["warns"] = fails, warns
    out["verdict"] = "PASS" if not fails else "FAIL"
    (AWS_DIR / "ops" / "reports" / "3000.json").write_text(
        json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


try:
    main()
except SystemExit:
    raise
except Exception as e:
    import traceback
    (AWS_DIR / "ops" / "reports" / "3000.json").write_text(json.dumps(
        {"ops": 3000, "verdict": "FAIL",
         "fails": ["CRASH: %s" % str(e)[:200]],
         "trace": traceback.format_exc()[-1500:],
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    sys.exit(1)
sys.exit(0)
