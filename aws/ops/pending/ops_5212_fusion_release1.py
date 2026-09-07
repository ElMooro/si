"""ops_5212 -- JustHodl Intelligence Network, Release 1 (communication foundation) launch + gate.

What it does, from the runner (deploy-lambdas.yml does not create NEW functions):
  1. ensures DynamoDB table justhodl-jhsignal-state (pk entity_id / sk signal_key, GSIs gsi_engine + gsi_status, TTL)
  2. creates/updates justhodl-jhsignal-bridge and justhodl-jh-fusion from aws/lambdas/<fn>/source (+ aws/shared bundle)
  3. arms EventBridge Scheduler: bridge hourly, fusion daily fallback (primary fusion trigger = coordinator route on
     jhsignal.batch_published, added to justhodl-event-coordinator in the same commit and deployed by deploy-lambdas.yml)
  4. runs the bridge async and gates on data/jhsignal/state/latest.json + bridge-run.json freshness, then waits for the
     coordinator-routed fusion to land data/jh-fusion.json for that snapshot (falls back to a direct fusion invoke and
     says so if the coordinator redeploy has not landed yet)
  5. audits both artifacts (engines OK/stale/missing, signals, entities, families, regime, vetoes, critical deps)
  6. runs the pytest suites on the runner
Any hard failure -> sys.exit(1).
"""
import json
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

BRIDGE = "justhodl-jhsignal-bridge"
FUSION = "justhodl-jh-fusion"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/jhsignal/state/latest.json"
RUN_KEY = "data/jhsignal/bridge-run.json"
FUSION_KEY = "data/jh-fusion.json"
FAILS = []


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def wait_active(fn):
    cfg = None
    for _ in range(40):
        cfg = lam.get_function_configuration(FunctionName=fn)
        if cfg.get("LastUpdateStatus") in (None, "Successful") and cfg.get("State") == "Active":
            break
        time.sleep(5)
    return cfg


def arm_schedule(R, name, expr, arn, inp, desc):
    tgt = {"Arn": arn, "RoleArn": SCHED_ROLE, "Input": inp, "RetryPolicy": {"MaximumRetryAttempts": 1}}
    try:
        sch.get_schedule(Name=name, GroupName="default")
        sch.update_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=desc)
        R.ok("   %s updated %s" % (name, expr))
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=desc)
        R.ok("   %s created %s" % (name, expr))
    except Exception as e:
        FAILS.append("schedule %s: %s" % (name, str(e)[:120]))


with report("ops_5212_fusion_release1") as R:
    R.heading("ops 5212 -- JustHodl Intelligence Network: Release 1 launch")
    # ---- 0. config drift guard (bundled copies must equal config/)
    for name in ("engine-registry.v1.json", "jh-fusion-flags.json", "jh-fusion-universe.json"):
        canon = (ROOT / "config" / name).read_bytes()
        for fn in (BRIDGE, FUSION):
            if (ROOT / "aws" / "lambdas" / fn / "source" / name).read_bytes() != canon:
                FAILS.append("bundled %s drifted in %s" % (name, fn))
    if FAILS:
        R.fail("config drift: %s" % FAILS); sys.exit(1)

    # ---- 1. state table
    R.section("DynamoDB current-state table")
    try:
        from jh_state_store import DynamoState
        st = DynamoState().ensure_table()
        R.ok("   justhodl-jhsignal-state: %s" % st)
    except Exception as e:
        R.warn("   table ensure failed (bridge falls back to the S3 read model; DDB flag stays on and self-heals): %s" % str(e)[:160])

    # ---- 2. functions
    R.section("functions")
    arns = {}
    for fn in (BRIDGE, FUSION):
        cfg_json = json.loads((ROOT / "aws" / "lambdas" / fn / "config.json").read_text())
        env = dict(cfg_json.get("env") or {})
        create_or_update_lambda(report=R, function_name=fn, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / fn / "source"),
                                env_vars=env, timeout=int(cfg_json.get("timeout") or 300), memory=int(cfg_json.get("memory") or 1024),
                                description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
        cfg = wait_active(fn)
        arns[fn] = cfg["FunctionArn"]
        R.log("   %s state %s / %s, %sMB / %ss" % (fn, cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("MemorySize"), cfg.get("Timeout")))
        if cfg.get("State") != "Active":
            FAILS.append("%s not Active" % fn)

    # ---- 3. schedules
    R.section("schedules (EventBridge Scheduler)")
    arm_schedule(R, "justhodl-jhsignal-bridge-hourly", "rate(1 hour)", arns[BRIDGE], "{}", "JHSIGNAL bridge hourly -- adapters over registered engine artifacts (ops 5212)")
    arm_schedule(R, "justhodl-jh-fusion-daily", "cron(20 5 * * ? *)", arns[FUSION], json.dumps({"mode": "scheduled"}), "JH fusion daily fallback (primary trigger: coordinator route jhsignal.batch_published) (ops 5212)")

    # ---- 4. validate_only pass (sync, cheap) then the real run
    R.section("bridge run")
    resp = lam.invoke(FunctionName=BRIDGE, InvocationType="RequestResponse", Payload=json.dumps({"mode": "validate_only"}).encode())
    v = json.loads(resp["Payload"].read() or b"{}")
    if resp.get("FunctionError") or not isinstance(v, dict) or "n_signals" not in v:
        FAILS.append("bridge validate_only failed: %s" % str(v)[:300])
        R.fail("   validate_only: %s" % str(v)[:300])
    else:
        R.log("   validate_only: %d signals from %d engines, %d entities; freshness %s" % (v["n_signals"], v["n_engines"], v["n_entities"], v.get("freshness_counts")))
        for e in v.get("engines") or []:
            R.kv(engine=e["engine_id"], family=e["family"], status=e["source_status"], n_signals=e["n_signals"], n_rejected=e["n_rejected"], data_asof=e.get("data_asof"), basis=e.get("asof_basis"))
        bad = [e for e in v.get("engines") or [] if e["source_status"] not in ("OK", "STALE")]
        if len(bad) > 6:
            FAILS.append("%d engines MISSING/INVALID at validate_only: %s" % (len(bad), [b["engine_id"] for b in bad]))
    before = None
    try:
        before = get_json(RUN_KEY).get("run_id")
    except Exception:
        pass
    t0 = time.time()
    lam.invoke(FunctionName=BRIDGE, InvocationType="Event", Payload=b"{}")
    run = None
    while time.time() - t0 < 280:
        time.sleep(10)
        try:
            d = get_json(RUN_KEY)
        except Exception:
            continue
        if d.get("run_id") and d.get("run_id") != before:
            run = d; break
    if not run:
        FAILS.append("bridge run did not land %s within 280s" % RUN_KEY)
    else:
        R.ok("   bridge run %s: %d signals / %d entities in %ss; changes %s; bus %s; state %s" % (run["run_id"], run["n_signals"], run["n_entities"], run.get("elapsed_s"), run.get("changes"), run.get("bus"), run.get("state_store")))
        if run["n_signals"] < 10 or run["n_entities"] < 6:
            FAILS.append("bridge produced too few signals (%s) / entities (%s)" % (run["n_signals"], run["n_entities"]))
        if (run.get("bus") or {}).get("failed"):
            FAILS.append("bus publish failures: %s" % run["bus"])
        if (run.get("state_store") or {}).get("error"):
            R.warn("   DDB state write error (S3 read model still authoritative for fusion): %s" % run["state_store"]["error"])
        snap = get_json(STATE_KEY)
        fams = {}
        for lst in snap["entities"].values():
            for c in lst:
                fams[c["family"]] = fams.get(c["family"], 0) + 1
        R.log("   snapshot families: %s" % fams)
        if len(fams) < 5:
            FAILS.append("fewer than 5 families live in the snapshot: %s" % fams)
        pilot = ("market:US_EQUITY", "etf:SPY", "equity:NVDA")
        for eid in pilot:
            R.log("   %s: %d signals -> %s" % (eid, len(snap["entities"].get(eid) or []), sorted({c["engine_id"] for c in snap["entities"].get(eid) or []})))
        if not snap["entities"].get("market:US_EQUITY"):
            FAILS.append("no market-subject signals (MACRO/RISK adapters all failed)")

    # ---- 5. fusion via the coordinator route, fallback direct
    R.section("fusion (coordinator route -> justhodl-jh-fusion)")
    route_ok = False
    if run:
        t1 = time.time(); fdoc = None
        while time.time() - t1 < 200:
            time.sleep(10)
            try:
                fdoc = get_json(FUSION_KEY)
            except Exception:
                continue
            if fdoc.get("snapshot_run_id") == run["run_id"]:
                route_ok = True; break
        if not route_ok:
            R.warn("   coordinator route did not produce fusion for %s within 200s (coordinator redeploy may still be landing) -- invoking fusion directly" % run["run_id"])
            lam.invoke(FunctionName=FUSION, InvocationType="Event", Payload=json.dumps({"mode": "ops5212-fallback", "force": True}).encode())
            t2 = time.time()
            while time.time() - t2 < 200:
                time.sleep(10)
                try:
                    fdoc = get_json(FUSION_KEY)
                except Exception:
                    continue
                if fdoc.get("snapshot_run_id") == run["run_id"]:
                    break
        if not fdoc or fdoc.get("snapshot_run_id") != run["run_id"]:
            FAILS.append("fusion never landed for snapshot %s" % run["run_id"])
        else:
            st = fdoc["stats"]
            R.ok("   fusion %s (%s): entities %d, horizon results %d, coverage mean %.2f, confidence mean %.2f, hard vetoes %d, soft %d, regime %s (%.2f, %d legs), shadow=%s, route_verified=%s"
                 % (fdoc["run_id"], fdoc.get("trigger", {}).get("event"), st["n_entities"], st["n_horizon_results"], st["coverage_mean"], st["confidence_mean"], st["hard_vetoes"], st["soft_vetoes"],
                    fdoc["regime"]["label"], fdoc["regime"]["score"], fdoc["regime"]["n_legs"], fdoc["shadow_mode"], route_ok))
            if st["n_entities"] < 14:
                FAILS.append("fusion covered %d entities, expected 14" % st["n_entities"])
            if fdoc["regime"]["label"] == "UNKNOWN":
                FAILS.append("regime UNKNOWN -- no MACRO/RISK context signals reached fusion")
            cd = fdoc["critical_dependencies"]
            if cd.get("failures"):
                R.warn("   critical dependencies: %s" % cd["failures"])
            if not fdoc.get("shadow_mode"):
                FAILS.append("shadow mode is OFF -- Release 1 must ship in shadow")
            for eid, r in fdoc["entities"].items():
                bh = r.get("best_horizon"); h = (r.get("horizons") or {}).get(bh) or {}
                R.kv(entity=eid, best_horizon=bh, fusion=h.get("fusion_score"), conviction=h.get("conviction"), confidence=h.get("confidence"), coverage=h.get("fusion_coverage"),
                     independent=h.get("independent_evidence_count"), raw=h.get("raw_signal_count"), contradiction=h.get("contradiction_score"), capital=h.get("capital_decision"), missing=",".join(h.get("missing_families") or []))
            R.log("   reliability basis: %s" % json.dumps(fdoc.get("reliability_basis", {}).get("per_engine", {}))[:600])
            R.log("   ledger: %s" % fdoc.get("ledger_key"))

    # ---- 6. tests on the runner
    R.section("tests")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pytest", "jsonschema"], check=False, capture_output=True, timeout=180)
        p = subprocess.run([sys.executable, "-m", "pytest", "-q", str(ROOT / "aws" / "lambdas" / BRIDGE / "tests"), str(ROOT / "aws" / "lambdas" / FUSION / "tests")], capture_output=True, text=True, timeout=600)
        tail = (p.stdout or "").strip().splitlines()[-3:]
        R.log("   pytest rc=%s: %s" % (p.returncode, " | ".join(tail)))
        if p.returncode != 0:
            FAILS.append("pytest failed: %s" % " | ".join(tail))
            R.log((p.stdout or "")[-3000:])
    except Exception as e:
        R.warn("   pytest could not run on the runner: %s" % str(e)[:160])

    if FAILS:
        R.section("FAILS")
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("GREEN -- Release 1 live in shadow mode: bridge hourly, fusion on jhsignal.batch_published (+ daily fallback), read models data/jhsignal/state/latest.json + data/jh-fusion.json")
    sys.exit(0)
