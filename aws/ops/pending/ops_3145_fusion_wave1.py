"""ops 3145 — FUSION WAVE 1: cross-engine enrichment (strictly additive).

From the fleet dependency graph (610 engines / 521 feeds): high-alpha
feeds sat unread. This wave fuses them into five flagships — every change
ADDS fields/blocks; nothing existing is removed or re-scored, and each
gate PROVES legacy fields survive.

  A industry-rotation  ← etf-flows divergence: fund_flows gains quadrant,
                         flow_zscore_90d, ret_21d_pct, divergence_score
  B best-setups        ← benzinga-earnings-calendar (earnings_date /
                         earnings_in_days / earnings_flag ≤7d) +
                         squeeze-fuel (score,state) + industry flow
                         quadrant passthrough from its own IR join
  C master-ranker      ← kill-theses bear line + squeeze-fuel, as
                         post-rank context overlays (chokepoint pattern:
                         no score change)
  D convergence-radar  ← talent-migration + structural-pre-signals +
                         universe-discovery → additive early_signals block
  E alpha-daily-brief  ← alpha-compass desk-sheet (top calls + self-graded
                         track record) into context bundle + prompt
                         (deployed smoke-off; LLM run lands on schedule)

Invoke order matters: A refreshes industry-rotation.json BEFORE B reads it.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
S3 = boto3.client("s3", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def dep(rep, fn, smoke=True):
    cfg = json.loads((AWS_DIR / "lambdas" / fn / "config.json").read_text())
    lam = boto3.client("lambda", region_name=REGION)
    env = (lam.get_function_configuration(FunctionName=fn)
           .get("Environment") or {}).get("Variables") or {}
    sched = cfg.get("schedule") or {}
    deploy_lambda(
        report=rep, function_name=fn,
        source_dir=AWS_DIR / "lambdas" / fn / "source",
        env_vars=env,
        eb_rule_name=(sched.get("name") or sched.get("rule_name")),
        eb_schedule=(sched.get("expression") or sched.get("cron")),
        timeout=cfg.get("timeout", 300), memory=cfg.get("memory", 512),
        description=(cfg.get("description") or "")[:250], smoke=smoke,
    )


def wait_fresh(key, t0, secs=300):
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            d = s3_json(key)
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                return d
        except Exception:
            pass
        time.sleep(10)
    return None


with report("3145_fusion_wave1") as rep:
    fails, warns = [], []
    rep.heading("ops 3145 — Fusion Wave 1 (5 engines, additive)")

    # ── A ────────────────────────────────────────────────────────────
    rep.section("A. industry-rotation ← divergence fields")
    tA = datetime.now(timezone.utc)
    try:
        dep(rep, "justhodl-industry-rotation")
    except Exception as e:
        fails.append(f"A deploy: {str(e)[:150]}")
    ir = wait_fresh("data/industry-rotation.json", tA, 360)
    if not ir:
        fails.append("A: industry-rotation.json never freshened")
    else:
        rows = (ir.get("rows") or ir.get("industries")
                or ir.get("soldiers") or [])
        withq = [r for r in rows
                 if (r.get("fund_flows") or {}).get("quadrant")]
        legacy = [r for r in rows
                  if (r.get("fund_flows") or {}).get("flow_21d_usd")
                  is not None]
        rep.kv(ir_rows=len(rows), ir_quadrant_rows=len(withq),
               ir_legacy_flow_rows=len(legacy))
        if rows and not legacy:
            fails.append("A: legacy fund_flows fields LOST")
        if rows and not withq:
            warns.append("A: no quadrant landed — check daily.json shape")
        else:
            sample = withq[0] if withq else {}
            rep.ok(f"A live: e.g. {sample.get('etf')} → "
                   f"{(sample.get('fund_flows') or {}).get('quadrant')}")

    # ── B ────────────────────────────────────────────────────────────
    rep.section("B. best-setups ← earnings + squeeze + IR flow quadrant")
    tB = datetime.now(timezone.utc)
    try:
        dep(rep, "justhodl-best-setups")
    except Exception as e:
        fails.append(f"B deploy: {str(e)[:150]}")
    bs = wait_fresh("data/best-setups.json", tB, 420)
    if not bs:
        fails.append("B: best-setups.json never freshened")
    else:
        rows = bs.get("top_setups") or []
        if not rows:
            fails.append("B: top_setups empty")
        else:
            r0 = rows[0]
            for k in ("ticker", "conviction", "verdict", "why"):
                if k not in r0:
                    fails.append(f"B: legacy field lost: {k}")
            for k in ("earnings_date", "earnings_in_days", "earnings_flag",
                      "squeeze_fuel", "industry_flow_quadrant"):
                if k not in r0:
                    fails.append(f"B: fusion field missing: {k}")
            n_e = sum(1 for r in rows if r.get("earnings_in_days")
                      is not None)
            n_s = sum(1 for r in rows if r.get("squeeze_fuel"))
            n_q = sum(1 for r in rows if r.get("industry_flow_quadrant"))
            n_f = sum(1 for r in rows if r.get("earnings_flag"))
            rep.kv(bs_rows=len(rows), bs_with_earnings=n_e,
                   bs_earnings_within7d=n_f, bs_with_squeeze=n_s,
                   bs_with_flow_quadrant=n_q)
            if n_e == 0:
                warns.append("B: zero earnings dates joined — calendar "
                             "empty or ticker mismatch")
            if n_q == 0:
                warns.append("B: zero flow quadrants — IR join fields "
                             "not propagating")
            rep.ok(f"B live on {len(rows)} setups")

    # ── C ────────────────────────────────────────────────────────────
    rep.section("C. master-ranker ← kill-theses + squeeze-fuel overlays")
    tC = datetime.now(timezone.utc)
    try:
        dep(rep, "justhodl-master-ranker")
    except Exception as e:
        fails.append(f"C deploy: {str(e)[:150]}")
    mr = wait_fresh("data/master-ranker.json", tC, 420)
    if not mr:
        fails.append("C: master-ranker.json never freshened")
    else:
        tops = (mr.get("top_tickers") or mr.get("ranked")
                or mr.get("leaderboard") or [])
        if not tops:
            fails.append("C: ranked list empty/renamed")
        else:
            for k in ("ticker", "score", "rationale"):
                if k not in tops[0]:
                    fails.append(f"C: legacy field lost: {k}")
            n_k = sum(1 for t in tops if t.get("kill_risk"))
            n_s = sum(1 for t in tops if t.get("squeeze_fuel"))
            rep.kv(mr_rows=len(tops), mr_kill=n_k, mr_squeeze=n_s)
            if n_k == 0 and n_s == 0:
                warns.append("C: no overlay hits on today's top names — "
                             "verify sources non-empty")
            rep.ok(f"C live: kill={n_k} squeeze={n_s} on {len(tops)} names")

    # ── D ────────────────────────────────────────────────────────────
    rep.section("D. convergence-radar ← early_signals block")
    tD = datetime.now(timezone.utc)
    try:
        dep(rep, "justhodl-convergence-radar")
    except Exception as e:
        fails.append(f"D deploy: {str(e)[:150]}")
    cv = wait_fresh("data/convergence-radar.json", tD, 420)
    if not cv:
        fails.append("D: convergence-radar.json never freshened")
    else:
        for k in ("summary", "pump_candidates", "tickers"):
            if k not in cv:
                fails.append(f"D: legacy block lost: {k}")
        es = cv.get("early_signals")
        if not es:
            fails.append("D: early_signals block missing")
        else:
            rep.kv(es_counts=json.dumps(es.get("counts")))
            rep.ok(f"D live: talent={len(es.get('talent_moves') or [])} "
                   f"restruct={len(es.get('restructuring') or [])} "
                   f"universe={len(es.get('universe_new') or [])}")

    # ── E ────────────────────────────────────────────────────────────
    rep.section("E. alpha-daily-brief ← desk-sheet (smoke off)")
    try:
        dep(rep, "justhodl-alpha-daily-brief", smoke=False)
        rep.ok("E deployed — desk-sheet block lands in next scheduled "
               "brief (LLM run skipped in-op by design)")
    except Exception as e:
        fails.append(f"E deploy: {str(e)[:150]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
