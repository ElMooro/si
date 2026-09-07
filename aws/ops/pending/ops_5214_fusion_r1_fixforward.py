"""ops_5214 -- fusion R1 fix-forward after the first live run (ops 5212 GREEN, ops 5213 diagnostics).

Findings -> fixes shipped in this commit:
  * global_business_cycle emitted 0 signals: v3 publishes downturn_probability_6m as a calibration block
    (probability_now) and countries_with_fresh_data/countries_total -- adapter reads those now.
  * etf_flows emitted 0 signals: dvol_z_score is null while avg_60d volume is unavailable -- adapter falls back to the
    engine's own dvol_5d_vs_20d_pct rotation measure (flagged in confidence_basis).
  * fortress / catalyst timestamps live in `as_of` -> registry timestamp_paths; fortress TTL 72h (Tue-Sat cadence),
    regime_composite / tail_risk TTL 30h (daily cadence).
  * bridge run report now carries per-engine skip reasons.
This op re-deploys both functions from source, re-runs the bridge (async + freshness gate), checks the fixes on the
live artifacts, waits for the coordinator-routed fusion, and audits the fan-out '15min' tick that justhodl-regime-composite
depends on (it had not run for 23h at 16:58 UTC). Hard failures -> sys.exit(1).
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

BRIDGE = "justhodl-jhsignal-bridge"
FUSION = "justhodl-jh-fusion"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
logs = boto3.client("logs", region_name="us-east-1", config=CFG)
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


def last_log(fn):
    try:
        st = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True, limit=1).get("logStreams") or []
        if st and st[0].get("lastEventTimestamp"):
            return datetime.fromtimestamp(st[0]["lastEventTimestamp"] / 1000, tz=timezone.utc)
    except Exception:
        return None
    return None


with report("ops_5214_fusion_r1_fixforward") as R:
    R.heading("ops 5214 -- fusion R1 fix-forward (GBC, etf-flows, timestamps, TTLs) + fan-out audit")
    for name in ("engine-registry.v1.json", "jh-fusion-flags.json", "jh-fusion-universe.json"):
        canon = (ROOT / "config" / name).read_bytes()
        for fn in (BRIDGE, FUSION):
            if (ROOT / "aws" / "lambdas" / fn / "source" / name).read_bytes() != canon:
                FAILS.append("bundled %s drifted in %s" % (name, fn))
    if FAILS:
        R.fail("config drift: %s" % FAILS); sys.exit(1)

    R.section("functions")
    for fn in (BRIDGE, FUSION):
        cfg_json = json.loads((ROOT / "aws" / "lambdas" / fn / "config.json").read_text())
        create_or_update_lambda(report=R, function_name=fn, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / fn / "source"),
                                env_vars=dict(cfg_json.get("env") or {}), timeout=int(cfg_json.get("timeout") or 300), memory=int(cfg_json.get("memory") or 1024),
                                description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
        cfg = wait_active(fn)
        R.log("   %s %s / %s" % (fn, cfg.get("State"), cfg.get("LastUpdateStatus")))

    R.section("bridge re-run")
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
        FAILS.append("bridge run did not land within 280s")
    else:
        R.ok("   run %s (bridge v%s): %d signals / %d entities in %ss; freshness %s; changes %s; bus %s; state %s"
             % (run["run_id"], run.get("version"), run["n_signals"], run["n_entities"], run.get("elapsed_s"), run.get("freshness_counts"), run.get("changes"), run.get("bus"), run.get("state_store")))
        eng = {e["engine_id"]: e for e in run.get("engines") or []}
        for eid, e in eng.items():
            R.kv(engine=eid, family=e.get("family"), status=e.get("source_status"), n=e.get("n_signals"), skipped=e.get("n_skipped"), asof=e.get("data_asof"), basis=e.get("asof_basis"),
                 skip_reasons=json.dumps(e.get("skip_reasons") or {})[:140], diag=("; ".join(e.get("diagnostics") or [])[:140]))
        # the four fixes, checked on live data
        if (eng.get("global_business_cycle") or {}).get("n_signals", 0) < 1:
            FAILS.append("global_business_cycle still emits 0 signals: %s" % json.dumps(eng.get("global_business_cycle"))[:300])
        if (eng.get("etf_flows") or {}).get("n_signals", 0) < 1:
            FAILS.append("etf_flows still emits 0 signals: %s" % json.dumps(eng.get("etf_flows"))[:300])
        for eid in ("fortress", "catalyst"):
            if (eng.get(eid) or {}).get("asof_basis") != "engine":
                FAILS.append("%s asof_basis is %s (expected engine)" % (eid, (eng.get(eid) or {}).get("asof_basis")))
        snap = get_json(STATE_KEY)
        mkt = sorted({c["engine_id"] for c in snap["entities"].get("market:US_EQUITY") or []})
        R.log("   market:US_EQUITY engines: %s" % mkt)
        if "global_business_cycle" not in mkt:
            FAILS.append("market subject still lacks global_business_cycle")
        fams = {}
        for lst in snap["entities"].values():
            for c in lst:
                fams[c["family"]] = fams.get(c["family"], 0) + 1
        R.log("   snapshot families: %s; duplicates dropped: %s" % (fams, snap.get("n_duplicates_dropped")))
        for eid in ("etf:SPY", "etf:QQQ", "equity:NVDA", "equity:AAPL"):
            R.log("   %s: %s" % (eid, sorted({c["engine_id"] for c in snap["entities"].get(eid) or []})))

    R.section("fusion via coordinator route")
    if run:
        fdoc = None; route_ok = False; t1 = time.time()
        while time.time() - t1 < 200:
            time.sleep(10)
            try:
                fdoc = get_json(FUSION_KEY)
            except Exception:
                continue
            if fdoc.get("snapshot_run_id") == run["run_id"]:
                route_ok = True; break
        if not route_ok:
            R.warn("   route did not land in 200s -- invoking fusion directly")
            lam.invoke(FunctionName=FUSION, InvocationType="Event", Payload=json.dumps({"mode": "ops5214-fallback", "force": True}).encode())
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
            R.ok("   fusion %s via %s: entities %d, coverage mean %.2f, confidence mean %.2f, hard %d / soft %d, regime %s (%.2f, %d legs), route_verified=%s"
                 % (fdoc["run_id"], fdoc.get("trigger", {}).get("event"), st["n_entities"], st["coverage_mean"], st["confidence_mean"], st["hard_vetoes"], st["soft_vetoes"], fdoc["regime"]["label"], fdoc["regime"]["score"], fdoc["regime"]["n_legs"], route_ok))
            if fdoc["regime"]["n_legs"] < 5:
                FAILS.append("regime legs %d < 5 (expected regime_composite, LCE, GBC, risk_gate, crisis_composite)" % fdoc["regime"]["n_legs"])
            for eid, r in fdoc["entities"].items():
                bh = r.get("best_horizon"); h = (r.get("horizons") or {}).get(bh) or {}
                R.kv(entity=eid, best_horizon=bh, fusion=h.get("fusion_score"), conviction=h.get("conviction"), confidence=h.get("confidence"), coverage=h.get("fusion_coverage"),
                     independent=h.get("independent_evidence_count"), raw=h.get("raw_signal_count"), contradiction=h.get("contradiction_score"), capital=h.get("capital_decision"), missing=",".join(h.get("missing_families") or []))

    R.section("fan-out '15min' tick audit (justhodl-regime-composite had not run for 23h)")
    try:
        man = get_json("config/fanout-manifest.json")
    except Exception as exc:
        man = None; R.warn("   fanout manifest unreadable: %s" % str(exc)[:120])
    if man:
        ticks = man.get("ticks") or {}
        members = ticks.get("15min") or []
        R.log("   ticks: %s" % {k: len(v) for k, v in ticks.items()})
        now = datetime.now(timezone.utc)
        rl = last_log("justhodl-scheduler")
        R.log("   router justhodl-scheduler last log event: %s" % (rl.isoformat()[:19] if rl else None))
        try:
            names = [s["Name"] for s in sch.list_schedules(NamePrefix="justhodl-scheduler").get("Schedules") or []]
            R.log("   router schedules: %s" % names)
        except Exception as exc:
            R.warn("   scheduler list: %s" % str(exc)[:100])
        silent = []
        for m in members[:40]:
            ll = last_log(m)
            age_h = (now - ll).total_seconds() / 3600 if ll else None
            R.kv(member=m, last_log=(ll.isoformat()[:19] if ll else None), age_h=(round(age_h, 1) if age_h is not None else None))
            if age_h is None or age_h > 6:
                silent.append((m, age_h))
        R.log("   15min members silent > 6h: %d of %d -> %s" % (len(silent), len(members), silent[:12]))
        if members and len(silent) >= max(3, len(members) // 2):
            R.warn("   the 15min fan-out tick looks dead again (majority silent) -- needs its own op; fusion treats regime_composite as STALE meanwhile")

    if FAILS:
        R.section("FAILS")
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("GREEN -- fix-forward verified on live data")
    sys.exit(0)
