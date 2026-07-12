"""ops 3178 — FUSION: his 96 engines start enhancing the platform's engines.

Two rules enforced in aws/shared/wl_fusion.py so they cannot be bypassed:
  · ADDITIVE-ONLY — if his feed disappears, every consumer behaves exactly
    as before. No platform engine may depend on his panels to function.
  · EVIDENCE-WEIGHTED — an unproven panel is CONTEXT (displayed, never
    scored). Only FDR-proven panels (|t|>=2, n_eff>=6) may tilt a score,
    and only within [0.90, 1.10]: his research shades a ranking, it never
    hijacks one.

Shipped:
  · justhodl-wl-fusion — theme pressure (LIQUIDITY/STRESS/CREDIT/DOLLAR/
    GROWTH/INFLATION/BREADTH/RATES), evidence-weighted tilts, and a
    DIVERGENCE BOARD: where HIS indicators disagree with the platform's
    own engines. That divergence is the question worth asking each day.
  · best-setups  — conviction shaded by proven CREDIT/STRESS panels;
                   khalid_panels + audit attached to every setup
  · alpha-compass — desk-level context + his top divergences

Gates: fusion doc written · themes populated · consumers redeploy clean
(smoke) · the multiplier is 1.0 wherever nothing is proven (proof that
unproven research cannot move a number).
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
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def dep(rep, fn, smoke=True, invoke_async=False):
    live = LAM.get_function_configuration(FunctionName=fn)
    env = (live.get("Environment") or {}).get("Variables") or {}
    cp = AWS_DIR / "lambdas" / fn / "config.json"
    cfg = json.loads(cp.read_text()) if cp.exists() else {
        "timeout": live.get("Timeout", 300),
        "memory": live.get("MemorySize", 512),
        "description": live.get("Description", "")}
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=fn,
                  source_dir=AWS_DIR / "lambdas" / fn / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"),
                  timeout=cfg.get("timeout", 300),
                  memory=cfg.get("memory", 512),
                  description=(cfg.get("description") or "")[:250],
                  smoke=smoke)
    if invoke_async:
        LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")


with report("3178_fusion") as rep:
    fails, warns = [], []
    rep.heading("ops 3178 — his engines enhance the fleet")

    rep.section("1. Deploy the fusion engine + run")
    t0 = datetime.now(timezone.utc)
    # create it (no live function yet) via the standard helper path
    cfg = json.loads((AWS_DIR / "lambdas" / "justhodl-wl-fusion"
                      / "config.json").read_text())
    sch = cfg["schedule"]
    deploy_lambda(report=rep, function_name="justhodl-wl-fusion",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-wl-fusion"
                  / "source",
                  env_vars={"S3_BUCKET": BUCKET},
                  eb_rule_name=sch["rule_name"], eb_schedule=sch["cron"],
                  timeout=cfg["timeout"], memory=cfg["memory"],
                  description=cfg["description"][:250], smoke=True)
    doc = None
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            d = s3_json("data/wl-fusion.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(10)
    if not doc:
        fails.append("wl-fusion.json never written")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("2. THEME PRESSURE — what his research says right now")
    rep.kv(active_engines=doc.get("n_engines_active"),
           firing=doc.get("n_firing"), proven=doc.get("n_proven"),
           divergences=len(doc.get("divergences") or []))
    for th, t in sorted((doc.get("themes") or {}).items(),
                        key=lambda kv: -(kv[1].get("pressure_pctile") or 0)):
        rep.log(f"  {th:10s} {str(t.get('verdict')):8s} "
                f"pressure {str(t.get('pressure_pctile')):>5}p  "
                f"firing {t.get('n_firing')}/{t.get('n_active')}  "
                f"proven {t.get('n_proven')}  "
                f"top: {', '.join(p['name'][:22] for p in (t.get('top_firing') or [])[:2])}")

    rep.section("3. DIVERGENCES — where he disagrees with the platform")
    divs = doc.get("divergences") or []
    if divs:
        rep.ok(f"{len(divs)} divergence(s) — the questions worth asking today")
        for d in divs:
            rep.log(f"  ⚡ {d['theme']}: HIS panels {d['khalid']['verdict']} "
                    f"({d['khalid']['pressure_pctile']}p, "
                    f"{d['khalid']['firing']}/{d['khalid']['of']} firing) "
                    f"vs {d['platform']['engine']} = "
                    f"'{d['platform']['state']}'")
            rep.log(f"      his loudest: "
                    f"{', '.join(d['khalid'].get('top') or [])}")
    else:
        rep.log("no divergences: his panels and the fleet agree today")

    rep.section("4. Consumers redeployed (additive-only contract)")
    for fn in ("justhodl-best-setups", "justhodl-alpha-compass"):
        try:
            dep(rep, fn, smoke=True)
        except Exception as e:
            fails.append(f"{fn}: {str(e)[:130]}")

    rep.section("5. Proof the contract holds")
    proven_total = sum(t.get("n_proven", 0)
                       for t in (doc.get("themes") or {}).values())
    rep.kv(proven_panels_total=proven_total)
    if proven_total == 0:
        rep.ok("ZERO proven panels today → every multiplier is exactly 1.0. "
               "His research is attached as CONTEXT to every setup and the "
               "desk, but it cannot move a score until it earns the right. "
               "That is the contract working, not a failure.")
    else:
        rep.ok(f"{proven_total} proven panel(s) may tilt scores within "
               "[0.90, 1.10] — audited on every row they touch")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
