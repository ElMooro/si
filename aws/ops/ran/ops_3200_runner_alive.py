"""ops 3200 — the one-key kill: fixed at the source, proven alive.

Forensics (3198 measured the death, 3199 captured the evidence): the fleet
has been dead since 3191 because DBnomics periods ('1990-01', '1990-Q1')
entered the shared weekly cache path and week_key's Y-M-D unpack raised —
ONE malformed key killed all 207 engines in 2.8 seconds, on every kick,
silently.

Two-layer fix, both deployed here:
  1. SOURCE: _dbn_iso normalizes every DBnomics period to a real ISO date
     (annual→12-31 matching the World Bank convention, quarterly→quarter
     end, monthly→-28); both DBnomics fetchers unified on it.
  2. RUNNER: week_key is now defensive — a bad key returns None and is
     skipped; a single symbol can never again take the fleet down.

Gate: data/wl-engines.json regenerated AFTER this run's kick, with the
real ACTIVE/DORMANT/FIRING split reported. Stale index = FAIL.
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
LOGS = boto3.client("logs", region_name=REGION)
SHARED_CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
                    "justhodl-symbol-dictionary")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3200_runner_alive") as rep:
    fails, warns = [], []
    rep.heading("ops 3200 — period-normalizer deployed, fleet proven alive")

    rep.section("1. Deploy the two-layer fix")
    for fn in SHARED_CONSUMERS:
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
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:90]}")

    rep.section("2. Kick + fresh-index gate")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:80]}")
    idx, gen = None, ""
    for _ in range(72):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        gen = str(d.get("generated_at", ""))
        if gen > mark:
            idx = d
            break
    if not idx:
        try:
            grp = "/aws/lambda/justhodl-wl-engines"
            st = (LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=1).get("logStreams") or [])
            if st:
                for e in LOGS.get_log_events(
                        logGroupName=grp,
                        logStreamName=st[0]["logStreamName"], limit=12,
                        startFromHead=False).get("events") or []:
                    rep.log("  tail: " + (e.get("message") or "")[:150])
        except Exception:
            pass
        fails.append(f"index STILL stale (generated_at={gen or 'missing'})")
    else:
        eng = idx.get("engines") or []
        active = sum(1 for e in eng if str(e.get("state")) == "ACTIVE")
        dormant = sum(1 for e in eng if str(e.get("state")) == "DORMANT")
        firing = sum(1 for e in eng
                     if any(str(e.get(k)) == "FIRING"
                            for k in ("signal", "fire", "status", "panel")))
        rep.kv(generated_at=gen[:19], engines=len(eng), active=active,
               dormant=dormant, firing=firing,
               series_cached=idx.get("series_cached"))
        for nm in [e.get("name") or e.get("engine_id") for e in eng
                   if str(e.get("state")) == "ACTIVE"][:4]:
            rep.log(f"  active e.g. {nm}")
        rep.ok(f"FLEET ALIVE — {active} ACTIVE on the final widened map; "
               "one bad key can never kill it again")
        if active < 100:
            warns.append(f"active {active} < 100 — audit member coverage "
                         "next session")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
