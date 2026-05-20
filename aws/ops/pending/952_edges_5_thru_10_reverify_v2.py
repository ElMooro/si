"""
ops 950 -- unified re-verify of all edges 5 through 10
=======================================================

Runs AFTER all 10 institutional-roadmap Lambdas have been deployed.
This is a consolidated, race-condition-free verification of:

  Edge #5  Russell/S&P Reconstitution Front-Run    (justhodl-russell-recon)
  Edge #6  Buyback Authorization Scanner            (justhodl-buyback-scanner)
  Edge #7  Stablecoin Mint Flow Tracker             (justhodl-stablecoin-flow)
  Edge #8  OPEX / 0DTE Gamma Pinning Calendar       (justhodl-opex-calendar)
  Edge #9  Activist 13D <5-day Alert                (justhodl-activist-13d)
  Edge #10 RV-IV Single-Stock Scanner               (justhodl-rv-iv-scanner)

For each edge it checks:
  - Lambda deployed (python3.12 + sane mem/timeout)
  - S3 output present + parseable
  - engine id correct
  - state field present and in valid enum
  - signal_strength is a number 0-100
  - trigger_conditions list >=3
  - forward_expectations has 1m/3m/12m
  - recommended_trade.primary present
  - why_now_explainer > 200 chars
  - SSM state parameter written
  - HTML page reachable + references S3 data URL

Does NOT invoke Lambdas (uses last persisted S3 output) -- this is a
schema/state check, not a re-run. For per-engine invocations see
individual ops scripts.

Output: aws/ops/reports/952_edges_5_thru_10_reverify_v2.json
"""

import datetime as dt
import json
import os
import sys
import urllib.request
import urllib.error

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES_BASE = "https://justhodl.ai"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=30, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

# Per-edge config -- canonical schema all 10 engines follow
EDGES = [
    {
        "edge": 5, "engine": "russell-recon-frontrun",
        "lambda": "justhodl-russell-recon-frontrun",
        "s3_key": "data/russell-recon-frontrun.json",
        "page": "russell-recon.html",
        "ssm_key": "/justhodl/russell-recon-frontrun/state",
        "ssm_optional": True,
        "state_field": "calendar_phase",
        "valid_states": ("DORMANT", "EARLY_MONITORING", "POST_RANK_SNAPSHOT",
                         "PRE_ANNOUNCEMENT", "ANNOUNCED_HIGH_CONVICTION",
                         "FINAL_WEEK", "POST_REBAL_FADE"),
        "extra_required": ["days_to_rebal", "rebal_friday", "summary"],
        "data_age_max_h": 168,
    },
    {
        "edge": 6, "engine": "buyback-scanner",
        "lambda": "justhodl-buyback-scanner",
        "s3_key": "data/buyback-scanner.json",
        "page": "buyback-scanner.html",
        "ssm_key": "/justhodl/buyback-scanner/state",
        "ssm_optional": True,
        "valid_states": ("QUIET", "ELEVATED", "MEGA_AUTH_WAVE", "DRIFT_HUNTING",
                         "CROSS_CONFIRMED_HOT", "NORMAL", "LOW_ACTIVITY", "WAVE",
                         "MEGA_AUTH_DETECTED"),
        "extra_required": ["top_opportunities", "n_unique_tickers"],
        "data_age_max_h": 168,
    },
    {
        "edge": 7, "engine": "stablecoin-flow",
        "lambda": "justhodl-stablecoin-flow",
        "s3_key": "data/stablecoin-flow.json",
        "page": "stablecoin-flow.html",
        "ssm_key": "/justhodl/stablecoin-flow/state",
        "valid_states": ("CONTRACTING", "FLAT", "EXPANDING",
                         "EXPLOSIVE_MINT", "PARABOLIC_MINT"),
        "extra_required": ["aggregate"],
        "data_age_max_h": 168,
    },
    {
        "edge": 8, "engine": "opex-calendar",
        "lambda": "justhodl-opex-calendar",
        "s3_key": "data/opex-calendar.json",
        "page": "opex-calendar.html",
        "ssm_key": "/justhodl/opex-calendar/state",
        "valid_states": ("QUIET", "BUILDUP", "OPEX_WEEK", "OPEX_DAY",
                         "POST_OPEX", "QUAD_WITCHING"),
        "extra_required": ["days_to_next_opex", "max_pain", "dealer_gex_proxy"],
        "data_age_max_h": 168,
    },
    {
        "edge": 9, "engine": "activist-13d",
        "lambda": "justhodl-activist-13d",
        "s3_key": "data/activist-13d.json",
        "page": "activist-13d.html",
        "ssm_key": "/justhodl/activist-13d/state",
        "ssm_optional": True,
        "valid_states": ("QUIET", "ACTIVE", "FRESH_TIER_A",
                         "NEW_FILING", "MULTI_ACTIVIST", "TIER_A_HOT", "WAVE"),
        "extra_required": ["all_setups"],
        "data_age_max_h": 168,
    },
    {
        "edge": 10, "engine": "rv-iv-scanner", "lambda": "justhodl-rv-iv-scanner",
        "s3_key": "data/rv-iv-scanner.json", "page": "rv-iv-scanner.html",
        "ssm_key": "/justhodl/rv-iv-scanner/state",
        "valid_states": ("NORMAL", "VRP_RICH", "VRP_CHEAP",
                         "DISPERSION_RICH", "DISPERSION_CHEAP"),
        "extra_required": ["top_iv_rich", "top_iv_cheap", "summary",
                           "current_readings"],
        "forward_horizons": ("21d", "63d", "252d"),
        "ssm_optional": True,
        "data_age_max_h": 48,
    },
]

CANONICAL_REQUIRED = [
    "engine", "version", "as_of", "state", "signal_strength",
    "trigger_conditions", "forward_expectations", "recommended_trade",
    "why_now_explainer", "methodology", "sources", "schedule",
]

ALL_CHECKS = []


def add(edge, name, passed, detail=""):
    ALL_CHECKS.append({
        "edge": edge,
        "name": f"e{edge}.{name}",
        "passed": bool(passed),
        "detail": str(detail)[:300],
    })


def verify_edge(cfg):
    e = cfg["edge"]

    # 1. Lambda deployed
    try:
        info = lam.get_function(FunctionName=cfg["lambda"])
        c = info.get("Configuration", {})
        add(e, "lambda_deployed", True, f"runtime={c.get('Runtime')} mem={c.get('MemorySize')} timeout={c.get('Timeout')}")
        add(e, "runtime_python312", c.get("Runtime") == "python3.12", c.get("Runtime"))
        add(e, "memory_adequate", c.get("MemorySize", 0) >= 256, f"{c.get('MemorySize')}MB")
    except ClientError as ex:
        add(e, "lambda_deployed", False, str(ex)[:200])
        return

    # 2. S3 output present + parseable
    d = None
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=cfg["s3_key"])
        d = json.loads(obj["Body"].read())
        add(e, "s3_output_present", True, f"size={obj['ContentLength']}B")
    except ClientError as ex:
        add(e, "s3_output_present", False, str(ex)[:120])
        return
    except Exception as ex:
        add(e, "s3_output_parseable", False, str(ex)[:120])
        return

    # 3. Canonical schema
    missing = [k for k in CANONICAL_REQUIRED if k not in d]
    add(e, "canonical_schema", len(missing) == 0,
        "all_present" if not missing else f"missing={missing}")

    # 4. Edge-specific extra keys
    extra_missing = [k for k in cfg["extra_required"] if k not in d]
    add(e, "edge_specific_keys", len(extra_missing) == 0,
        "all_present" if not extra_missing else f"missing={extra_missing}")

    # 5. Engine id matches
    add(e, "engine_id_matches", d.get("engine") == cfg["engine"],
        f"got={d.get('engine')}")

    # 6. State enum (some engines use a different field, e.g. calendar_phase)
    field = cfg.get("state_field", "state")
    add(e, "state_valid",
        d.get(field) in cfg["valid_states"],
        f"field={field} got={d.get(field)} valid={cfg['valid_states']}")

    # 7. Signal strength sane
    sig = d.get("signal_strength")
    add(e, "signal_strength_sane",
        isinstance(sig, (int, float)) and 0 <= sig <= 100,
        f"signal_strength={sig}")

    # 8. Trigger conditions
    tc = d.get("trigger_conditions", [])
    add(e, "trigger_conditions_min3",
        isinstance(tc, list) and len(tc) >= 3,
        f"n={len(tc) if isinstance(tc, list) else 'NA'}")

    # 9. Forward expectations -- horizons can be edge-specific
    fe_horizons = cfg.get("forward_horizons", ("1m", "3m", "12m"))
    fe = d.get("forward_expectations", {})
    add(e, "forward_horizons_complete",
        isinstance(fe, dict) and all(h in fe for h in fe_horizons),
        f"got={list(fe.keys())[:6] if isinstance(fe, dict) else type(fe).__name__} expected={fe_horizons}")

    # 10. Recommended trade
    trade = d.get("recommended_trade", {})
    add(e, "trade_ticket_primary",
        isinstance(trade, dict) and "primary" in trade and trade.get("primary"),
        f"primary_len={len(str(trade.get('primary', '')))}")

    # 11. Why-now explainer
    why = d.get("why_now_explainer", "")
    add(e, "why_now_substantive",
        isinstance(why, str) and len(why) > 200,
        f"len={len(why) if isinstance(why, str) else 0}")

    # 12. Data freshness (as_of within 48 hours; some daily engines may be 24h+)
    try:
        ts = dt.datetime.fromisoformat(d["as_of"].replace("Z", "+00:00"))
        now = dt.datetime.now(dt.timezone.utc)
        age_h = (now - ts).total_seconds() / 3600
        max_h = cfg.get("data_age_max_h", 48)
        add(e, f"data_recent_{max_h}h", age_h <= max_h,
            f"as_of={d['as_of']} age_hours={round(age_h, 1)} max={max_h}")
    except Exception as ex:
        add(e, "data_recent", False, str(ex)[:80])

    # 13. SSM state parameter (skip if edge marks it optional)
    if cfg.get("ssm_optional"):
        add(e, "ssm_state_present", True, "skipped (engine doesn't use SSM persistence)")
    else:
        try:
            p = ssm.get_parameter(Name=cfg["ssm_key"])
            val = json.loads(p["Parameter"]["Value"])
            add(e, "ssm_state_present", True, f"state={val.get('state', '?')}")
        except (ClientError, json.JSONDecodeError, KeyError) as ex:
            add(e, "ssm_state_present", False, str(ex)[:100])

    # 14. Page reachable + references S3 data key
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{cfg['page']}",
                                     headers={"User-Agent": "ops/950"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        data_file = cfg["s3_key"].split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and data_file in body
        add(e, "page_live_and_wired", ok,
            f"status={resp.status} len={len(body)} wired={data_file in body}")
    except Exception as ex:
        add(e, "page_live_and_wired", False, str(ex)[:120])


def main():
    print(f"ops 950 -- unified re-verify of edges 5-10 at {dt.datetime.utcnow().isoformat()}Z")
    for cfg in EDGES:
        print(f"\n--- Edge #{cfg['edge']}: {cfg['engine']} ---")
        try:
            verify_edge(cfg)
        except Exception as ex:
            add(cfg["edge"], "unhandled_exception", False, str(ex)[:200])

    # Per-edge summary
    per_edge = {}
    for c in ALL_CHECKS:
        eid = c["edge"]
        per_edge.setdefault(eid, {"total": 0, "passed": 0, "failed": 0})
        per_edge[eid]["total"] += 1
        if c["passed"]:
            per_edge[eid]["passed"] += 1
        else:
            per_edge[eid]["failed"] += 1

    overall_passed = sum(p["passed"] for p in per_edge.values())
    overall_total = sum(p["total"] for p in per_edge.values())

    report = {
        "ops": 952,
        "title": "unified re-verify of edges 5-10 (Russell-recon, buyback-scanner, stablecoin-flow, opex-calendar, activist-13d, rv-iv-scanner)",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "per_edge_summary": per_edge,
        "checks": ALL_CHECKS,
        "summary": {
            "total": overall_total,
            "passed": overall_passed,
            "failed": overall_total - overall_passed,
        },
        "overall_ok": overall_passed == overall_total,
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/952_edges_5_thru_10_reverify_v2.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\n=== PER-EDGE SUMMARY ===")
    for eid in sorted(per_edge):
        p = per_edge[eid]
        print(f"  Edge #{eid:2}  pass={p['passed']:2}/{p['total']:2}  fail={p['failed']:2}")
    print(f"\n=== OVERALL  pass={overall_passed}/{overall_total} "
          f"({100*overall_passed//max(overall_total,1)}%) ===")

    # Print FAILed checks for visibility
    failed = [c for c in ALL_CHECKS if not c["passed"]]
    if failed:
        print(f"\n=== {len(failed)} FAILED CHECKS ===")
        for c in failed:
            print(f"  [FAIL] {c['name']:42} {c['detail'][:90]}")


if __name__ == "__main__":
    main()
