"""ops 3244 — SERIES-LEVEL fusion: the new curated data wired as DIRECT
model inputs into the engines that natively need it (audit showed each
was a genuine gap; cycle-clock/regime skipped — they already carry
confidence inputs):

  · credit-stress      ← europe_sovereign   (BTP–Bund, OAT–BTP,
                                             Bono–BTP, €STR)
  · eurodollar-plumbing← euro_policy_corridor (€STR, ECB depo,
                                             Euribor-implied, GB 3m)
  · macro-nowcast      ← global_confidence  (12-country CCI + business
                                             conf + DE/FR/EA GDP YoY,
                                             composite z)
  · crisis-composite   ← btp_bund_canary    (THE European crisis canary)

Bridge = aws/shared/wl_series.py: reads the fleet's own weekly cache
(zero new fetch load), per-series last/z_1y/13w-change, NEVER raises,
additive-only. Deploy 4 (+ wl-engines/thesis for the shared file),
invoke each, verify the new field live with real values.
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
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
TARGETS = {
    "justhodl-credit-stress": ("data/credit-stress.json",
                               "europe_sovereign"),
    "justhodl-eurodollar-plumbing": ("data/eurodollar-plumbing.json",
                                     "euro_policy_corridor"),
    "justhodl-macro-nowcast": ("data/macro-nowcast.json",
                               "global_confidence"),
    "justhodl-crisis-composite": ("data/crisis-composite.json",
                                  "btp_bund_canary"),
}
SHARED_ALSO = ("justhodl-wl-engines", "justhodl-thesis-engine",
               "justhodl-symbol-dictionary")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def deploy(fn, rep, fails):
    try:
        cfg = {}
        p = AWS_DIR / "lambdas" / fn / "config.json"
        if p.exists():
            cfg = json.loads(p.read_text())
        sch = cfg.get("schedule")
        rule, cron = (sch.get("rule_name"), sch.get("cron")) \
            if isinstance(sch, dict) else (None, None)
        live = (LAM.get_function_configuration(FunctionName=fn)
                .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=AWS_DIR / "lambdas" / fn / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 1024),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=fn, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy {fn}: {str(e)[:80]}")


with report("3244_series_fusion") as rep:
    fails, warns = [], []
    rep.heading("ops 3244 — series-level fusion into four engines")

    rep.section("1. Deploys (targets + shared consumers)")
    for fn in list(TARGETS) + list(SHARED_ALSO):
        deploy(fn, rep, fails)
    if fails:
        for f in fails:
            rep.fail(f)
        rep.kv(verdict="FAIL")
        sys.exit(1)

    rep.section("2. Invoke + verify each new block LIVE")
    marks = {}
    for fn in TARGETS:
        marks[fn] = datetime.now(timezone.utc).isoformat()
        try:
            LAM.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=b"{}")
        except Exception as e:
            fails.append(f"invoke {fn}: {str(e)[:70]}")
    verified = 0
    for fn, (key, field) in TARGETS.items():
        got = None
        for _ in range(40):
            time.sleep(8)
            d = s3_json(key) or {}
            if str(d.get("generated_at", "")) > marks[fn]:
                got = d
                break
        if not got:
            warns.append(f"{fn}: feed not fresh in window")
            continue
        blk = got.get(field) or {}
        ser = blk.get("series") or {}
        if ser:
            verified += 1
            sample = list(ser.items())[:3]
            parts = "; ".join(f"{v['label']}={v['last']} z={v['z_1y']}"
                              for _, v in sample)
            comp = (f" composite_z={blk.get('composite_z')}"
                    f" (n={blk.get('composite_n')})"
                    if "composite_z" in blk else "")
            rep.ok(f"{fn}.{field}: {len(ser)} series LIVE — {parts}"
                   f"{comp}")
        else:
            fails.append(f"{fn}.{field}: block empty in fresh feed")

    rep.kv(engines_verified=verified, of=len(TARGETS))
    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
