"""
ops 957 -- FINAL 10-edge verification (all edges 1-10)
========================================================

Definitive verification after all fixes land. Covers all 10 institutional
roadmap engines in a single pass.

Edges 1-4 use legacy schemas (FIRED/ARMED/etc), edges 5-10 use the
canonical schema with state machine + forward_expectations.

Expected: 100% pass after Edge #5 forward_expectations fix (commit f38358ed).
"""
import datetime as dt
import json
import os
import time
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES_BASE = "https://justhodl.ai"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=620, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

# Re-invoke Edge #5 since its source was just updated
def reinvoke_edge_5():
    print("Re-invoking Edge #5 after forward_expectations source fix...")
    try:
        r = lam.invoke(FunctionName="justhodl-russell-recon-frontrun",
                       InvocationType="RequestResponse", Payload=b"{}")
        payload = r["Payload"].read().decode()
        print(f"  Edge #5 invoke status={r['StatusCode']} body[:200]={payload[:200]}")
        return r["StatusCode"] == 200 and not r.get("FunctionError")
    except ClientError as ex:
        print(f"  invoke failed: {ex}")
        return False


# Lightweight per-edge schema check
EDGES = [
    # Edges 1-4 (legacy schemas)
    {"edge": 1, "engine": "vix-backwardation-trigger",
     "lambda": "justhodl-vix-backwardation-trigger",
     "s3_key": "data/vix-backwardation-trigger.json",
     "page": "vix-capitulation.html",
     "state_field": "state",
     "valid_states": ("FIRED", "ARMED", "WARM", "NULL", "COOLDOWN")},
    {"edge": 2, "engine": "insider-buys-enriched",
     "lambda": "justhodl-insider-buys-enriched",
     "s3_key": "data/insider-buys-enriched.json",
     "page": "insider-buys.html",
     "state_field": None,  # may not have explicit state
     "valid_states": ()},
    {"edge": 3, "engine": "breadth-thrust",
     "lambda": "justhodl-breadth-thrust",
     "s3_key": "data/breadth-thrust.json",
     "page": "breadth-thrust.html",
     "state_field": "state",
     "valid_states": ("FIRED", "ARMED", "NULL", "COOLDOWN")},
    {"edge": 4, "engine": "vol-target-unwind",
     "lambda": "justhodl-vol-target-unwind",
     "s3_key": "data/vol-target-unwind.json",
     "page": "vol-target-unwind.html",
     "state_field": "state",
     "valid_states": None},  # accept any state
    # Edges 5-10 (canonical)
    {"edge": 5, "engine": "russell-recon-frontrun",
     "lambda": "justhodl-russell-recon-frontrun",
     "s3_key": "data/russell-recon-frontrun.json",
     "page": "russell-recon.html",
     "state_field": "calendar_phase",
     "valid_states": ("DORMANT", "EARLY_MONITORING", "POST_RANK_SNAPSHOT",
                      "PRE_ANNOUNCEMENT", "ANNOUNCED_HIGH_CONVICTION",
                      "FINAL_WEEK", "POST_REBAL_FADE"),
     "forward_horizons": ("1m", "3m", "12m")},
    {"edge": 6, "engine": "buyback-scanner",
     "lambda": "justhodl-buyback-scanner",
     "s3_key": "data/buyback-scanner.json",
     "page": "buyback-scanner.html",
     "state_field": "state",
     "valid_states": ("QUIET", "ELEVATED", "MEGA_AUTH_WAVE", "DRIFT_HUNTING",
                      "CROSS_CONFIRMED_HOT", "NORMAL", "LOW_ACTIVITY",
                      "WAVE", "MEGA_AUTH_DETECTED"),
     "forward_horizons": ("1m", "3m", "12m")},
    {"edge": 7, "engine": "stablecoin-flow",
     "lambda": "justhodl-stablecoin-flow",
     "s3_key": "data/stablecoin-flow.json",
     "page": "stablecoin-flow.html",
     "state_field": "state",
     "valid_states": ("CONTRACTING", "FLAT", "EXPANDING",
                      "EXPLOSIVE_MINT", "PARABOLIC_MINT"),
     "forward_horizons": ("1m", "3m", "12m")},
    {"edge": 8, "engine": "opex-calendar",
     "lambda": "justhodl-opex-calendar",
     "s3_key": "data/opex-calendar.json",
     "page": "opex-calendar.html",
     "state_field": "state",
     "valid_states": ("QUIET", "BUILDUP", "OPEX_WEEK", "OPEX_DAY",
                      "POST_OPEX", "QUAD_WITCHING", "NORMAL"),
     "forward_horizons": ("1m", "3m", "12m")},
    {"edge": 9, "engine": "activist-13d",
     "lambda": "justhodl-activist-13d",
     "s3_key": "data/activist-13d.json",
     "page": "activist-13d.html",
     "state_field": "state",
     "valid_states": ("QUIET", "ACTIVE", "FRESH_TIER_A",
                      "NEW_FILING", "MULTI_ACTIVIST", "TIER_A_HOT", "WAVE"),
     "forward_horizons": ("1m", "3m", "12m")},
    {"edge": 10, "engine": "rv-iv-scanner",
     "lambda": "justhodl-rv-iv-scanner",
     "s3_key": "data/rv-iv-scanner.json",
     "page": "rv-iv-scanner.html",
     "state_field": "state",
     "valid_states": ("NORMAL", "VRP_RICH", "VRP_CHEAP",
                      "DISPERSION_RICH", "DISPERSION_CHEAP"),
     "forward_horizons": ("21d", "63d", "252d")},
]

CHECKS = []


def add(edge, name, passed, detail=""):
    CHECKS.append({"edge": edge, "name": f"e{edge}.{name}",
                   "passed": bool(passed), "detail": str(detail)[:250]})


def verify(cfg):
    e = cfg["edge"]
    try:
        info = lam.get_function(FunctionName=cfg["lambda"])
        add(e, "lambda_deployed", True,
            f"runtime={info['Configuration'].get('Runtime')}")
    except ClientError as ex:
        add(e, "lambda_deployed", False, str(ex)[:100])
        return

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=cfg["s3_key"])
        d = json.loads(obj["Body"].read())
        add(e, "s3_output_present", True, f"size={obj['ContentLength']}B")
    except ClientError as ex:
        add(e, "s3_output_present", False, str(ex)[:100])
        return

    add(e, "engine_id", d.get("engine") == cfg["engine"], d.get("engine"))

    if cfg.get("state_field"):
        sf = cfg["state_field"]
        val = d.get(sf)
        if cfg.get("valid_states"):
            add(e, "state_valid", val in cfg["valid_states"],
                f"field={sf} got={val}")
        else:
            add(e, "state_present", val is not None, f"field={sf} got={val}")
    else:
        add(e, "state_skipped", True, "no state field for this edge")

    # Canonical schema (edges 5-10 only, since 1-4 use legacy schema)
    if e >= 5:
        canon = ["engine", "version", "as_of", "signal_strength",
                 "trigger_conditions", "forward_expectations",
                 "why_now_explainer", "methodology"]
        missing = [k for k in canon if k not in d]
        add(e, "canonical_schema", len(missing) == 0,
            "all_present" if not missing else f"missing={missing}")

        fe = d.get("forward_expectations", {})
        fh = cfg.get("forward_horizons", ("1m", "3m", "12m"))
        add(e, "forward_horizons",
            isinstance(fe, dict) and all(h in fe for h in fh),
            f"got={list(fe.keys()) if isinstance(fe, dict) else 'n/a'}")

    # Data freshness
    try:
        ts = dt.datetime.fromisoformat(d["as_of"].replace("Z", "+00:00"))
        age_h = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds() / 3600
        add(e, "data_recent_168h", age_h <= 168, f"age_h={round(age_h, 1)}")
    except Exception as ex:
        add(e, "data_recent_168h", False, str(ex)[:60])

    # Page reachable
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{cfg['page']}",
                                     headers={"User-Agent": "ops/957"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        data_file = cfg["s3_key"].split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and data_file in body
        add(e, "page_live_and_wired", ok,
            f"status={resp.status} wired={data_file in body}")
    except Exception as ex:
        add(e, "page_live_and_wired", False, str(ex)[:100])


def main():
    print(f"ops 957 -- FINAL 10-edge verification at {dt.datetime.utcnow().isoformat()}Z")
    reinvoke_edge_5()
    time.sleep(3)
    for cfg in EDGES:
        print(f"\n--- Edge #{cfg['edge']}: {cfg['engine']} ---")
        try:
            verify(cfg)
        except Exception as ex:
            add(cfg["edge"], "unhandled", False, str(ex)[:150])

    per_edge = {}
    for c in CHECKS:
        eid = c["edge"]
        per_edge.setdefault(eid, {"passed": 0, "total": 0})
        per_edge[eid]["total"] += 1
        if c["passed"]:
            per_edge[eid]["passed"] += 1

    op = sum(p["passed"] for p in per_edge.values())
    ot = sum(p["total"] for p in per_edge.values())

    rep = {
        "ops": 957,
        "title": "FINAL 10-edge institutional roadmap verification",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "per_edge_summary": per_edge,
        "checks": CHECKS,
        "summary": {"total": ot, "passed": op, "failed": ot - op,
                    "pct": round(100 * op / max(ot, 1), 1)},
        "overall_ok": op == ot,
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/957_final_10_edge_verify.json", "w") as f:
        json.dump(rep, f, indent=2)

    print("\n=== PER-EDGE SUMMARY ===")
    for eid in sorted(per_edge):
        p = per_edge[eid]
        flag = "PASS" if p["passed"] == p["total"] else "yellow" if p["passed"] >= p["total"]*0.8 else "FAIL"
        print(f"  Edge #{eid:2}  pass={p['passed']:2}/{p['total']:2}  [{flag}]")
    print(f"\n=== OVERALL pass={op}/{ot} ({round(100*op/max(ot,1), 1)}%) ===")
    failed = [c for c in CHECKS if not c["passed"]]
    if failed:
        print(f"\n{len(failed)} FAILED CHECKS:")
        for c in failed:
            print(f"  [FAIL] {c['name']:38} {c['detail'][:90]}")


if __name__ == "__main__":
    main()
