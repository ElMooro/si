"""ops 3170 — REGIME GATE: do the stress panels work when the Fed isn't easing?

3169's verdict: STRESS composite ran -3.30% (SPY 13w) in 1993-2009 and
+1.67% in 2010-2026 — the sign INVERTED. Selling stress became buying
the dip. That is the policy reaction function, not a broken thesis.

So the honest question is regime-conditional. v2.2 builds a policy
regime from FRED (1990+): EASING / NEUTRAL / TIGHTENING (Fed funds 26w
change, plus balance-sheet impulse where WALCL exists), and re-runs
every family composite INSIDE each regime — each measured against that
regime's OWN base rate.

Hypothesis: Khalid's crisis panels are valid when the Fed is not
easing, and inverted when it is. If true, the signal isn't dead — it is
GATED, and the platform already computes the gate daily.

3168's finding: on 36 years, NO single thesis clears FDR — but every
stress/crisis/plumbing panel tilted the same way (SPY 13w excess -0.87%
to -2.62%, hit edge -5 to -16pp). Nine independent panels agreeing on
sign is ~1-in-500 under a coin-flip null. That is textbook weak-
correlated-signal aggregation: pool them.

v2.1 builds FAMILY COMPOSITES (STRESS / LIQUIDITY / CREDIT / GROWTH /
INFLATION / DOLLAR / BREADTH / CRYPTO):
  · families come from KHALID'S OWN LIST NAMES — never from the signs
    the study produced (that would be fitting the noise just measured)
  · composite = rolling-z of each panel's activation, averaged
  · event-studied like any signal, but now only ~8 tests instead of 56
  · SPLIT-SAMPLE: the edge must hold in BOTH halves of 1990-2026
  · binomial SIGN TEST across each family's members
  · only FDR-passing + stable + firing composites emit signals

v1's honest null (0/56 survive FDR) was a POWER problem: ~2 years of
history gave n_eff ~5 per thesis. ops 3167 proved free deep sources
(FRED 1990+, Yahoo/Stooq market chain) and mapped 2,895 symbols.

v2 rebuilds the study on a weekly grid back to 1990 (~1,900 weeks),
3y rolling z-scores, forward SPY at 4/13/26 weeks, overlap-corrected t,
BH-FDR across all theses. THIS run's numbers carry weight.

Deploy (3008MB/900s; series_source.py rides the aws/shared bundle),
invoke with force_emit, wait, then report:
  · weeks of history actually achieved
  · FDR survivors — the theses that genuinely lead
  · which are firing today
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-thesis-engine"
AWS_DIR = Path(__file__).resolve().parents[2]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3170_regime_gate") as rep:
    fails, warns = [], []
    rep.heading("ops 3170 — Thesis Engine v2 (1990-2026)")

    rep.section("1. Deploy v2")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    env = (LAM.get_function_configuration(FunctionName=FN)
           .get("Environment") or {}).get("Variables") or {}
    sched = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env,
                  eb_rule_name=sched.get("rule_name"),
                  eb_schedule=sched.get("cron"),
                  timeout=cfg.get("timeout", 900),
                  memory=cfg.get("memory", 3008),
                  description=cfg.get("description", "")[:250],
                  smoke=False)

    rep.section("2. Deep run (first pass backfills 1990→today)")
    doc, last = None, None
    for attempt in (1, 2):          # 2nd invoke continues from the cache
        t0 = datetime.now(timezone.utc)
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"force_emit": True}).encode())
        rep.log(f"invoke {attempt} fired")
        deadline = time.time() + 880
        while time.time() < deadline:
            try:
                d = s3_json("data/thesis-engine.json")
                if datetime.fromisoformat(d["generated_at"]) >= t0:
                    last = d
                    break
            except Exception:
                pass
            time.sleep(20)
        if last is None:
            fails.append(f"invoke {attempt}: doc never freshened")
            break
        rep.kv(**{f"pass{attempt}_status": last.get("status"),
                  f"pass{attempt}_weeks": last.get("n_weeks"),
                  f"pass{attempt}_series": last.get("series_cached"),
                  f"pass{attempt}_theses": last.get("n_theses")})
        if last.get("status") == "LIVE" and (last.get("n_theses") or 0) > 0:
            doc = last
            break
        rep.log(f"  status={last.get('status')} — running a second pass to "
                "finish the backfill from cache")

    if not doc:
        fails.append(f"engine never reached LIVE with theses "
                     f"(last status: {(last or {}).get('status')})")
        rep.kv(n_fails=len(fails), verdict="FAIL")
        sys.exit(1)

    rep.section("3. FAMILY COMPOSITES — the aggregated evidence")
    fams = doc.get("families") or []
    rep.kv(families=len(fams),
           fdr_pass=sum(1 for f in fams if f.get("fdr_pass")),
           stable=sum(1 for f in fams if f.get("stable")),
           firing=sum(1 for f in fams if f.get("firing")))
    for f in fams:
        e = (f.get("event_study") or {}).get("w13") or {}
        st = f.get("sign_test") or {}
        h1 = (f.get("half1") or {}).get("excess_vs_base_pct")
        h2 = (f.get("half2") or {}).get("excess_vs_base_pct")
        rep.log(f"  {f['family']:10s} n={f['n_theses']:<2d} "
                f"comp {str(f.get('composite_now')):>6} "
                f"| SPY13w {str(e.get('excess_vs_base_pct')):>6}% "
                f"hit-edge {str(e.get('hit_edge_pp')):>5}pp "
                f"t={str(e.get('t_stat')):>6} n_eff={str(e.get('n_effective')):>5} "
                f"| halves {str(h1):>6}/{str(h2):>6} "
                f"| sign {st.get('n_negative')}/{st.get('n_theses')} "
                f"p={st.get('p_value')}"
                f"{'  FDR' if f.get('fdr_pass') else ''}"
                f"{' STABLE' if f.get('stable') else ''}"
                f"{' FIRING' if f.get('firing') else ''}")
    rep.log("── REGIME-CONDITIONAL (each vs that regime's own base rate):")
    for f in fams:
        br = f.get("by_regime") or {}
        for reg in ("EASING", "NEUTRAL", "TIGHTENING"):
            r = br.get(reg)
            if not r:
                continue
            rep.log(f"  {f['family']:10s} {reg:10s} "
                    f"SPY13w {r['spy_fwd_mean_pct']:>6.2f}% vs regime base "
                    f"{r['regime_base_pct']:>6.2f}% → excess "
                    f"{r['excess_vs_regime_base_pct']:>6.2f}% "
                    f"t={r['t_stat']:>6} n_eff={r['n_effective']}")
    rep.kv(regime_now=doc.get("regime_now"),
           **{f"weeks_{k.lower()}": v
              for k, v in (doc.get("regime_weeks") or {}).items()})
    gated = []
    for f in fams:
        for reg, r in (f.get("by_regime") or {}).items():
            if abs(r.get("t_stat", 0)) >= 2 and r.get("n_effective", 0) >= 6:
                gated.append((f["family"], reg, r))
    if gated:
        rep.ok(f"{len(gated)} REGIME-GATED edges found — the signal lives "
               "inside specific policy regimes")
        for fam, reg, r in gated:
            tag = "risk-OFF" if r["excess_vs_regime_base_pct"] < 0 else "risk-ON"
            rep.log(f"  ★ {fam} under {reg} [{tag}]: SPY 13w "
                    f"{r['excess_vs_regime_base_pct']:+.2f}% vs that "
                    f"regime's base, t={r['t_stat']}, n_eff={r['n_effective']}")
    else:
        warns.append("no regime-gated edge either — the panels are context, "
                     "not timing, in every policy state")

    winners = [f for f in fams if f.get("fdr_pass") and f.get("stable")]
    if winners:
        rep.ok(f"{len(winners)} composite(s) survive FDR *and* hold in both "
               "halves — aggregation converted weak panels into real signal")
        for f in winners:
            e = f["event_study"]["w13"]
            tag = "risk-OFF" if e["excess_vs_base_pct"] < 0 else "risk-ON"
            rep.log(f"  ★ {f['family']} [{tag}]: when firing, SPY 13w "
                    f"{e['excess_vs_base_pct']:+.2f}% vs base "
                    f"(hit edge {e['hit_edge_pp']:+.1f}pp), t={e['t_stat']}, "
                    f"n_eff={e['n_effective']} — members: "
                    f"{', '.join(f['members'][:4])}"
                    f"{'   ← FIRING NOW' if f.get('firing') else ''}")
    else:
        warns.append("no composite clears FDR+stability — aggregation did "
                     "not rescue the signal; these panels describe regimes "
                     "rather than time them")

    rep.section("4. Per-thesis detail (36 years of evidence)")
    rows = doc.get("theses") or []
    surv = [r for r in rows if r.get("fdr_pass")]
    firing = [r for r in rows if r.get("firing")]
    rep.kv(history_start=doc.get("history_start"), weeks=doc.get("n_weeks"),
           series_cached=doc.get("series_cached"), theses=len(rows),
           fdr_survivors=len(surv), firing_now=len(firing),
           signals_logged=doc.get("signals_logged"),
           elapsed_s=doc.get("elapsed_s"))
    b = doc.get("spy_base_rates_pct") or {}
    rep.log(f"── SPY base rates since 1990: 4w {b.get('w4')}% · "
            f"13w {b.get('w13')}% · 26w {b.get('w26')}%")

    rep.log("── ranked by overlap-corrected t on 13w forward SPY:")
    for r in rows[:16]:
        e = (r.get("event_study") or {}).get("w13") or {}
        rep.log(f"  {str(r['name'])[:36]:36s} "
                f"hist {str(r.get('history_from')):>8} "
                f"act {str(r.get('activation_now')):>5}% "
                f"| excess {str(e.get('excess_vs_base_pct')):>6}% "
                f"hit-edge {str(e.get('hit_edge_pp')):>5}pp "
                f"t={str(e.get('t_stat')):>6} n_eff={str(e.get('n_effective')):>5}"
                f"{'  FDR' if r.get('fdr_pass') else ''}"
                f"{' FIRING' if r.get('firing') else ''}")

    if surv:
        rep.ok(f"{len(surv)} theses SURVIVE FDR on 36 years — these are "
               "real leads, not noise")
        rep.log("── THE THESES THAT ACTUALLY PREDICT:")
        for r in surv[:10]:
            e = r["event_study"]["w13"]
            tag = "risk-OFF" if e["excess_vs_base_pct"] < 0 else "risk-ON"
            rep.log(f"  · {r['name']} [{tag}] — when firing, SPY 13w "
                    f"{e['excess_vs_base_pct']:+.2f}% vs base "
                    f"(hit edge {e['hit_edge_pp']:+.1f}pp), t={e['t_stat']}, "
                    f"n_eff={e['n_effective']}, since {r['history_from']}"
                    f"{'   ← FIRING NOW' if r.get('firing') else ''}")
    else:
        warns.append("still zero FDR survivors even on 36y — the honest "
                     "read: these panels describe regimes, they do not time "
                     "SPY. Keep them as context, not as timing signals.")

    rep.section("5. Page")
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://justhodl.ai/theses.html?t={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 ops"}), timeout=15)
        if "Thesis Engine" in r.read().decode("utf-8", "replace"):
            rep.ok("theses.html live")
    except Exception as e:
        warns.append(f"page: {str(e)[:50]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    sys.exit(1 if fails else 0)
