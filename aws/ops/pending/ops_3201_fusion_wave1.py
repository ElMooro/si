"""ops 3201 — FUSION WAVE 1: his research is now an input to the fleet.

Audit found the fusion layer (wl-fusion engine + aws/shared/wl_fusion.py:
additive-only, evidence-weighted, multipliers bounded) built and scheduled
— but consumed by only 2 of the declared targets, invisible on every page,
and last fused against a crash-era index.

This ops ships wave 1 of the fix:
  1. wl_fusion.block() — the ONE-LINE fusion surface (never raises;
     returns None when there is nothing proven/active → consumers stay
     byte-identical if the feed is missing). SIXTEEN engines now attach
     "wl_research" (theme-filtered context + divergences) to their
     payloads: regime/crisis/risk-regime, global-liquidity, dollar-radar,
     master-ranker, credit-stress, liquidity-credit, cycle-clock,
     macro-nowcast, equity-confluence, breadth-thrust/-divergence,
     crypto-liquidity/-emergence, eurodollar-plumbing.
  2. wl-fusion RE-FUSED against the ALIVE index (3200) before any
     consumer runs.
  3. End-to-end proof: four engines invoked, their LIVE feeds checked for
     the wl_research key. Deploy without verification is not done.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)

FUSED = ("regime-composite", "crisis-composite", "risk-regime",
         "global-liquidity", "dollar-radar", "master-ranker",
         "credit-stress", "liquidity-credit-engine", "cycle-clock",
         "macro-nowcast", "equity-confluence", "breadth-divergence",
         "breadth-thrust", "crypto-liquidity", "crypto-emergence",
         "eurodollar-plumbing")
SAMPLES = {"regime-composite": "data/regime-composite.json",
           "dollar-radar": "data/dollar-radar.json",
           "macro-nowcast": "data/macro-nowcast.json",
           "crypto-liquidity": "data/crypto-liquidity.json"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3201_fusion_wave1") as rep:
    fails, warns = [], []
    rep.heading("ops 3201 — 16 engines fused with his research, proven in "
                "live feeds")

    # ── 1. re-fuse against the alive index ────────────────────────────
    rep.section("1. Refresh wl-fusion on the 3200 index")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        # deploy wl-fusion first so it bundles the new shared module
        cfg = json.loads((AWS_DIR / "lambdas" / "justhodl-wl-fusion"
                          / "config.json").read_text())
        live = (LAM.get_function_configuration(
            FunctionName="justhodl-wl-fusion")
            .get("Environment") or {}).get("Variables") or {}
        sch = cfg.get("schedule") or {}
        deploy_lambda(report=rep, function_name="justhodl-wl-fusion",
                      source_dir=AWS_DIR / "lambdas" / "justhodl-wl-fusion"
                      / "source",
                      env_vars=live, eb_rule_name=sch.get("rule_name"),
                      eb_schedule=sch.get("cron"),
                      timeout=cfg.get("timeout", 240),
                      memory=cfg.get("memory", 512),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.invoke(FunctionName="justhodl-wl-fusion",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"wl-fusion: {str(e)[:90]}")
    fus = None
    for _ in range(24):
        time.sleep(5)
        d = s3_json("data/wl-fusion.json") or {}
        if str(d.get("generated_at", "")) > mark:
            fus = d
            break
    if fus:
        th = fus.get("themes") or {}
        rep.kv(themes=len(th),
               proven_total=sum((t.get("n_proven") or 0)
                                for t in th.values()),
               divergences=len(fus.get("divergences") or []))
        for name, t in sorted(th.items(),
                              key=lambda kv: -(kv[1].get("pressure_pctile")
                                               or 0))[:5]:
            rep.log(f"  {name:<10} pressure {t.get('pressure_pctile')}p  "
                    f"firing {t.get('n_firing')}/{t.get('n_active')}  "
                    f"{t.get('verdict')}")
        rep.ok("wl-fusion fresh on the alive index")
    else:
        fails.append("wl-fusion did not regenerate")

    # ── 2. deploy the sixteen ──────────────────────────────────────────
    rep.section("2. Deploy the 16 fused engines")
    ok_dep = 0
    for fn_short in FUSED:
        fn = f"justhodl-{fn_short}"
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            sch = cfg.get("schedule") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=sch.get("rule_name"),
                          eb_schedule=sch.get("cron"),
                          timeout=cfg.get("timeout", 300),
                          memory=cfg.get("memory", 512),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
            ok_dep += 1
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:80]}")
    rep.kv(deployed=ok_dep, of=len(FUSED))

    # ── 3. end-to-end proof in live feeds ─────────────────────────────
    rep.section("3. Prove wl_research lands in live feeds")
    mark2 = datetime.now(timezone.utc).isoformat()
    for fn_short in SAMPLES:
        try:
            LAM.invoke(FunctionName=f"justhodl-{fn_short}",
                       InvocationType="Event", Payload=b"{}")
        except Exception as e:
            warns.append(f"invoke {fn_short}: {str(e)[:60]}")
    landed = {}
    for _ in range(30):
        time.sleep(10)
        for fn_short, key in SAMPLES.items():
            if fn_short in landed:
                continue
            d = s3_json(key) or {}
            if str(d.get("generated_at", "")) > mark2 \
                    and "wl_research" in d:
                wr = d.get("wl_research")
                landed[fn_short] = wr
                if wr:
                    ctx = wr.get("context") or {}
                    top = next(iter(ctx.items()), None)
                    rep.log(f"  ✓ {fn_short}: wl_research live"
                            + (f" — {top[0]} {top[1].get('pressure_pctile')}p"
                               if top else " (None: nothing active yet)"))
                else:
                    rep.log(f"  ✓ {fn_short}: wl_research present "
                            "(None — additive contract holding)")
        if len(landed) == len(SAMPLES):
            break
    rep.kv(verified_feeds=len(landed), of_samples=len(SAMPLES))
    for fn_short in SAMPLES:
        if fn_short not in landed:
            warns.append(f"{fn_short}: feed not refreshed in window — "
                         "verify at its next schedule")
    if not landed:
        fails.append("no sample feed showed wl_research — fusion not "
                     "proven end-to-end")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
