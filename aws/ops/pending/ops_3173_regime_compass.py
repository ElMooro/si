"""ops 3173 — the silent truncation, and compass's real traceback.

ROOT CAUSE (3170-3172 all shipped 1,746 all-NEUTRAL weeks): the regime
series were appended LAST to a ~1,800-symbol fetch list, and the engine's
620s fetch budget cut the loop before reaching them. Not a bad FRED key —
a silent truncation. They are now fetched FIRST, outside the budget.

Also: alpha-compass smoke has failed twice with "FunctionError: Unhandled"
and no visible reason. This op invokes it SYNCHRONOUSLY and prints the
lambda's own stackTrace verbatim, so the next fix is aimed, not guessed.
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
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION,
                   config=boto3.session.Config(read_timeout=300,
                                               retries={"max_attempts": 0}))


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3173_regime_compass") as rep:
    fails, warns = [], []
    rep.heading("ops 3173 — regime truncation fix + compass traceback")

    rep.section("1. Compass — the verbatim failure")
    r = LAM.invoke(FunctionName="justhodl-alpha-compass",
                   InvocationType="RequestResponse", Payload=b"{}")
    payload = json.loads(r["Payload"].read().decode() or "{}")
    if r.get("FunctionError"):
        rep.log(f"errorType: {payload.get('errorType')}")
        rep.log(f"errorMessage: {str(payload.get('errorMessage'))[:220]}")
        for line in (payload.get("stackTrace") or [])[-6:]:
            rep.log(f"  {str(line).strip()[:200]}")
        fails.append(f"compass: {payload.get('errorType')}: "
                     f"{str(payload.get('errorMessage'))[:120]}")
    else:
        rep.ok(f"compass healthy: cards={payload.get('cards')} "
               f"regime={payload.get('regime')}")

    rep.section("2. Thesis engine — regime series fetched FIRST")
    cfg = json.loads((AWS_DIR / "lambdas" / "justhodl-thesis-engine"
                      / "config.json").read_text())
    env = (LAM.get_function_configuration(
        FunctionName="justhodl-thesis-engine").get("Environment")
        or {}).get("Variables") or {}
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name="justhodl-thesis-engine",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-thesis-engine"
                  / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"), timeout=cfg["timeout"],
                  memory=cfg["memory"],
                  description=cfg.get("description", "")[:250], smoke=False)
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-thesis-engine",
               InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    doc = None
    deadline = time.time() + 780
    while time.time() < deadline:
        try:
            d = s3_json("data/thesis-engine.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(20)
    if not doc:
        fails.append("thesis-engine never refreshed")
    else:
        rw = doc.get("regime_weeks") or {}
        rd = doc.get("regime_debug") or {}
        rep.kv(regime_now=doc.get("regime_now"),
               **{f"weeks_{k.lower()}": v for k, v in rw.items()})
        rep.log(f"  debug: {json.dumps(rd)}")
        if rw.get("EASING", 0) > 50 and rw.get("TIGHTENING", 0) > 50:
            rep.ok(f"REGIME SERIES LIVE — {rw['EASING']} easing / "
                   f"{rw['NEUTRAL']} neutral / {rw['TIGHTENING']} tightening "
                   "weeks since 1990. The regime-gated study is finally valid.")
            rep.log("── EACH FAMILY, INSIDE EACH POLICY REGIME:")
            for f in (doc.get("families") or []):
                for reg in ("EASING", "NEUTRAL", "TIGHTENING"):
                    rr = (f.get("by_regime") or {}).get(reg)
                    if not rr:
                        continue
                    rep.log(f"  {f['family']:10s} {reg:10s} "
                            f"SPY13w {rr['spy_fwd_mean_pct']:>6.2f}% vs base "
                            f"{rr['regime_base_pct']:>6.2f}% → excess "
                            f"{rr['excess_vs_regime_base_pct']:>6.2f}%  "
                            f"t={rr['t_stat']:>6}  n_eff={rr['n_effective']}")
            hits = [(f["family"], reg, rr)
                    for f in (doc.get("families") or [])
                    for reg, rr in (f.get("by_regime") or {}).items()
                    if abs(rr.get("t_stat", 0)) >= 2
                    and rr.get("n_effective", 0) >= 6]
            if hits:
                rep.ok(f"{len(hits)} REGIME-GATED EDGE(S) FOUND")
                for fam, reg, rr in hits:
                    tag = ("risk-OFF" if rr["excess_vs_regime_base_pct"] < 0
                           else "risk-ON")
                    rep.log(f"  ★ {fam} under {reg} [{tag}]: "
                            f"{rr['excess_vs_regime_base_pct']:+.2f}% vs that "
                            f"regime's own base, t={rr['t_stat']}, "
                            f"n_eff={rr['n_effective']}")
            else:
                rep.warn("no regime-gated edge — with the regime series now "
                         "REAL, the verdict stands: his panels are context, "
                         "not SPY timing, in every policy state")
            rep.kv(signals_logged=doc.get("signals_logged"))
        else:
            fails.append(f"regime STILL degenerate: {rw} debug={rd}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
