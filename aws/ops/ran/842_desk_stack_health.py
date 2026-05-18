"""
ops/842 - desk-stack health audit.

WHY
---
Two build pipelines ship engines into this repo fast. The seven strategy
desks, the Desk Allocator capstone and the desk-return feed are now a
single dependency chain - if any desk silently goes stale, the allocator
keeps sizing a dead book and the firm allocation drifts off real data. A
pod shop runs a daily operational checklist over the whole book; this is
that checklist, as a read-only audit.

WHAT IT CHECKS  (read-only - no deploys, no invokes)
----------------------------------------------------
For each of the 9 desk-stack Lambdas:
  - S3 output sidecar exists and is fresh (age under the per-engine bar);
  - its schedule is wired and ENABLED - EventBridge Scheduler for the
    desks built on the new path, the classic EventBridge rule for the
    older ones (best-ideas).
Then it cross-reads the two capstone sidecars:
  - data/desk-allocator.json   - every desk's status / active_count /
    freshness / capital weight, and flags any desk OFFLINE or DRY;
  - data/desk-returns.json     - per-desk realized-return observation
    count, i.e. how warmed-up each desk's Bayesian shrinkage is
    (archetype prior governs until 20 observations).

Writes aws/ops/reports/842_desk_stack_health.json.
"""
import json
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

# desk-stack Lambdas, in dependency order. out_key is data/{name}.json;
# fresh_h is the per-engine staleness bar in hours.
STACK = [
    ("justhodl-best-ideas", "data/best-ideas.json", 30),
    ("justhodl-pairs-arb", "data/pairs-arb.json", 30),
    ("justhodl-trend-engine", "data/trend-engine.json", 30),
    ("justhodl-merger-arb", "data/merger-arb.json", 30),
    ("justhodl-spinoff-desk", "data/spinoff-desk.json", 30),
    ("justhodl-index-recon", "data/index-recon.json", 30),
    ("justhodl-risk-radar", "data/risk-radar.json", 30),
    ("justhodl-desk-returns", "data/desk-returns.json", 80),   # weekday-only
    ("justhodl-desk-allocator", "data/desk-allocator.json", 30),
]

cfg = Config(read_timeout=120, connect_timeout=20,
             retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
evt = boto3.client("events", region_name=REGION, config=cfg)

now = datetime.now(timezone.utc)
rep = {
    "ops": 842,
    "ts": now.isoformat(),
    "subject": "Desk-stack health audit - 7 strategy desks + Desk "
               "Allocator + desk-return feed (read-only)",
    "checks": [],
    "engines": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def load_config(fn):
    try:
        return json.load(open(f"aws/lambdas/{fn}/config.json"))
    except Exception:
        return {}


def schedule_state(conf):
    """Resolve the schedule for an engine across both schedule mechanisms.

    Returns (kind, name, state, expr) - kind is 'scheduler', 'rule' or
    'none'; state is the ENABLED/DISABLED string or an error marker.
    """
    sb = conf.get("eventbridge_scheduler")
    if isinstance(sb, dict) and sb.get("schedule_name"):
        nm = sb["schedule_name"]
        try:
            sd = sch.get_schedule(Name=nm)
            return ("scheduler", nm, sd.get("State"),
                    sd.get("ScheduleExpression"))
        except Exception as e:
            return ("scheduler", nm, f"ERR {type(e).__name__}", "")
    rb = conf.get("schedule")
    if isinstance(rb, dict) and rb.get("rule_name"):
        nm = rb["rule_name"]
        try:
            rd = evt.describe_rule(Name=nm)
            return ("rule", nm, rd.get("State"),
                    rd.get("ScheduleExpression"))
        except Exception as e:
            return ("rule", nm, f"ERR {type(e).__name__}", "")
    return ("none", None, "MISSING", "")


# ---- 1) per-engine freshness + schedule ------------------------------------
for fn, out_key, fresh_bar in STACK:
    conf = load_config(fn)
    row = {"engine": fn, "out_key": out_key}

    # S3 freshness
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=out_key)
        age_h = round((now - head["LastModified"]).total_seconds() / 3600, 1)
        row["output_age_hours"] = age_h
        row["output_fresh"] = age_h <= fresh_bar
    except Exception as e:
        row["output_age_hours"] = None
        row["output_fresh"] = False
        row["output_error"] = f"{type(e).__name__}: {e}"[:160]
    check(f"{fn}__output_fresh", row["output_fresh"],
          f"{row.get('output_age_hours')}h (bar {fresh_bar}h)")

    # schedule
    kind, nm, state, expr = schedule_state(conf)
    row["schedule_kind"] = kind
    row["schedule_name"] = nm
    row["schedule_state"] = state
    row["schedule_expr"] = expr
    check(f"{fn}__schedule_enabled", state == "ENABLED",
          f"{kind}:{nm} = {state} {expr}")

    rep["engines"].append(row)

# ---- 2) cross-read the Desk Allocator --------------------------------------
alloc = {}
try:
    alloc = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key="data/desk-allocator.json")["Body"].read())
except Exception as e:
    check("allocator_readable", False, f"{type(e).__name__}: {e}")

desks = alloc.get("desks") or []
firm = alloc.get("firm") or {}
if desks:
    check("allocator_readable", True, f"{len(desks)} desks")
    offline = [d.get("key") for d in desks if d.get("status") == "OFFLINE"]
    dry = [d.get("key") for d in desks if d.get("status") == "DRY"]
    firing = [d for d in desks if d.get("status") == "FIRING"]
    check("all_desks_firing", not offline and not dry,
          f"firing={len(firing)} dry={dry} offline={offline}")
    rep["desk_book"] = [
        {"desk": d.get("name"), "key": d.get("key"),
         "status": d.get("status"),
         "active": d.get("active_count"),
         "weight_pct": d.get("capital_weight_pct"),
         "freshness_h": d.get("freshness_hours")}
        for d in sorted(desks,
                        key=lambda x: -(x.get("capital_weight_pct") or 0))]
    rep["firm"] = {
        "desks_firing": firm.get("desks_firing"),
        "desks_offline": firm.get("desks_offline"),
        "net_equity_beta": firm.get("net_equity_beta"),
        "diversification_ratio": firm.get("diversification_ratio"),
        "dominant_desk": firm.get("dominant_desk"),
    }

# ---- 3) cross-read the desk-return feed ------------------------------------
dr = {}
try:
    dr = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key="data/desk-returns.json")["Body"].read())
except Exception as e:
    check("desk_returns_readable", False, f"{type(e).__name__}: {e}")

dr_desks = dr.get("desks") or {}
if dr_desks:
    obs = {}
    for k, v in dr_desks.items():
        series = (v or {}).get("returns") or []
        obs[k] = len(series)
    check("desk_returns_readable", True, f"{len(dr_desks)} desks tracked")
    warmed = sum(1 for n in obs.values() if n >= 20)
    rep["return_feed"] = {
        "desks_tracked": len(dr_desks),
        "observations_per_desk": obs,
        "desks_warmed_up": warmed,
        "note": ("a desk needs 20 daily observations before realized vol "
                 "shrinks in; until then the archetype prior governs - "
                 "expected for a recently-seeded feed"),
    }
    # not a pass/fail gate: warm-up is a multi-week process by design
    check("return_feed_present", len(dr_desks) == 9 or len(dr_desks) == 7,
          f"{len(dr_desks)} desks, {warmed} warmed up")

# ---- verdict ---------------------------------------------------------------
rep["all_pass"] = all(c["ok"] for c in rep["checks"])
fails = [c["check"] for c in rep["checks"] if not c["ok"]]
n_eng = len(STACK)
n_fresh = sum(1 for e in rep["engines"] if e.get("output_fresh"))
n_sched = sum(1 for e in rep["engines"]
              if e.get("schedule_state") == "ENABLED")
rep["verdict"] = (
    f"DESK STACK HEALTHY - {n_fresh}/{n_eng} engines fresh, "
    f"{n_sched}/{n_eng} schedules ENABLED, all 7 desks firing into the "
    f"allocator (diversification ratio "
    f"{rep.get('firm', {}).get('diversification_ratio')}). Return feed "
    f"seeded and warming."
    if rep["all_pass"]
    else f"REVIEW - {len(fails)} check(s) failed: {fails[:8]}")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/842_desk_stack_health.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
