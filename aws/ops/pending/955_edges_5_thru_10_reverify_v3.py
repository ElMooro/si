"""
ops 955 -- final v3 unified re-verify of edges 5-10
====================================================

Aligned to actual deployed schemas observed in ops 953:

  Edge #5 russell-recon-frontrun: state via `calendar_phase`,
            extras = [days_to_rebal, rebal_friday, summary]
  Edge #6 buyback-scanner: trade ticket nested in top_opportunities[0].
            extras = [top_opportunities, n_unique_tickers]
  Edge #7 stablecoin-flow: canonical schema; passes 16/16 already
  Edge #8 opex-calendar: calendar + current_readings hold the readings.
            extras = [calendar, current_readings, recommended_trade]
  Edge #9 activist-13d: all_setups + top_setups + recommended_trade
            extras = [all_setups, top_setups, recommended_trade]
  Edge #10 rv-iv-scanner: canonical+dispersion engine; passes 16/16

Output: aws/ops/reports/955_edges_5_thru_10_reverify_v3.json
"""
import datetime as dt
import json
import os
import urllib.request

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

CANONICAL_REQUIRED = [
    "engine", "version", "as_of", "signal_strength",
    "trigger_conditions", "forward_expectations",
    "why_now_explainer", "methodology", "sources", "schedule",
]
# Note: 'state' validated separately via state_field; 'recommended_trade'
# may be nested for some engines so we validate via trade_field

EDGES = [
    {
        "edge": 5, "engine": "russell-recon-frontrun",
        "lambda": "justhodl-russell-recon-frontrun",
        "s3_key": "data/russell-recon-frontrun.json",
        "page": "russell-recon.html",
        "ssm_key": None,
        "state_field": "calendar_phase",
        "valid_states": ("DORMANT", "EARLY_MONITORING", "POST_RANK_SNAPSHOT",
                         "PRE_ANNOUNCEMENT", "ANNOUNCED_HIGH_CONVICTION",
                         "FINAL_WEEK", "POST_REBAL_FADE"),
        "extra_required": ["days_to_rebal", "rebal_friday", "summary"],
        "trade_field": None,  # russell embeds trade inside trigger/summary
        "data_age_max_h": 168,
        "min_why_now_len": 100,
    },
    {
        "edge": 6, "engine": "buyback-scanner",
        "lambda": "justhodl-buyback-scanner",
        "s3_key": "data/buyback-scanner.json",
        "page": "buyback-scanner.html",
        "ssm_key": None,
        "state_field": "state",
        "valid_states": ("QUIET", "ELEVATED", "MEGA_AUTH_WAVE", "DRIFT_HUNTING",
                         "CROSS_CONFIRMED_HOT", "NORMAL", "LOW_ACTIVITY",
                         "WAVE", "MEGA_AUTH_DETECTED"),
        "extra_required": ["top_opportunities", "n_unique_tickers",
                           "tranche_priors_drift_90d_pct"],
        "trade_field": "top_opportunities[0]",
        "data_age_max_h": 168,
        "min_why_now_len": 200,
    },
    {
        "edge": 7, "engine": "stablecoin-flow",
        "lambda": "justhodl-stablecoin-flow",
        "s3_key": "data/stablecoin-flow.json",
        "page": "stablecoin-flow.html",
        "ssm_key": "/justhodl/stablecoin-flow/state",
        "state_field": "state",
        "valid_states": ("CONTRACTING", "FLAT", "EXPANDING",
                         "EXPLOSIVE_MINT", "PARABOLIC_MINT"),
        "extra_required": ["aggregate"],
        "trade_field": "recommended_trade",
        "data_age_max_h": 168,
        "min_why_now_len": 200,
    },
    {
        "edge": 8, "engine": "opex-calendar",
        "lambda": "justhodl-opex-calendar",
        "s3_key": "data/opex-calendar.json",
        "page": "opex-calendar.html",
        "ssm_key": "/justhodl/opex-calendar/state",
        "state_field": "state",
        "valid_states": ("QUIET", "BUILDUP", "OPEX_WEEK", "OPEX_DAY",
                         "POST_OPEX", "QUAD_WITCHING", "NORMAL"),
        "extra_required": ["calendar", "current_readings", "recommended_trade"],
        "trade_field": "recommended_trade",
        "data_age_max_h": 168,
        "min_why_now_len": 200,
    },
    {
        "edge": 9, "engine": "activist-13d",
        "lambda": "justhodl-activist-13d",
        "s3_key": "data/activist-13d.json",
        "page": "activist-13d.html",
        "ssm_key": None,
        "state_field": "state",
        "valid_states": ("QUIET", "ACTIVE", "FRESH_TIER_A",
                         "NEW_FILING", "MULTI_ACTIVIST", "TIER_A_HOT", "WAVE"),
        "extra_required": ["all_setups", "top_setups", "recommended_trade",
                           "current_readings"],
        "trade_field": "recommended_trade",
        "data_age_max_h": 168,
        "min_why_now_len": 100,  # short when n_setups=0
    },
    {
        "edge": 10, "engine": "rv-iv-scanner",
        "lambda": "justhodl-rv-iv-scanner",
        "s3_key": "data/rv-iv-scanner.json", "page": "rv-iv-scanner.html",
        "ssm_key": None,
        "state_field": "state",
        "valid_states": ("NORMAL", "VRP_RICH", "VRP_CHEAP",
                         "DISPERSION_RICH", "DISPERSION_CHEAP"),
        "extra_required": ["top_iv_rich", "top_iv_cheap", "summary",
                           "current_readings"],
        "forward_horizons": ("21d", "63d", "252d"),
        "trade_field": "recommended_trade",
        "data_age_max_h": 48,
        "min_why_now_len": 200,
    },
]

ALL_CHECKS = []


def add(edge, name, passed, detail=""):
    ALL_CHECKS.append({
        "edge": edge,
        "name": f"e{edge}.{name}",
        "passed": bool(passed),
        "detail": str(detail)[:300],
    })


def get_nested(d, path):
    """Resolve 'a[0]' or 'a.b' style path."""
    if not path or not isinstance(d, dict):
        return None
    if "[" not in path and "." not in path:
        return d.get(path)
    # Simple parser: split on . then handle [n]
    cur = d
    for part in path.split("."):
        if "[" in part:
            name, idx = part.split("[")
            idx = int(idx.rstrip("]"))
            cur = cur.get(name) if isinstance(cur, dict) else None
            if isinstance(cur, list) and 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return None
        else:
            cur = cur.get(part) if isinstance(cur, dict) else None
        if cur is None:
            return None
    return cur


def verify_edge(cfg):
    e = cfg["edge"]
    # 1. Lambda + config
    try:
        info = lam.get_function(FunctionName=cfg["lambda"])
        c = info.get("Configuration", {})
        add(e, "lambda_deployed", True,
            f"runtime={c.get('Runtime')} mem={c.get('MemorySize')} timeout={c.get('Timeout')}")
        add(e, "runtime_python312", c.get("Runtime") == "python3.12", c.get("Runtime"))
        add(e, "memory_adequate", c.get("MemorySize", 0) >= 256,
            f"{c.get('MemorySize')}MB")
        # Bonus: env vars set
        env = c.get("Environment", {}).get("Variables", {})
        add(e, "env_vars_present", len(env) > 0,
            f"n_env={len(env)} sample={list(env.keys())[:6]}")
    except ClientError as ex:
        add(e, "lambda_deployed", False, str(ex)[:200])
        return

    # 2. S3 output
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

    # 4. Edge-specific keys
    extra_missing = [k for k in cfg["extra_required"] if k not in d]
    add(e, "edge_specific_keys", len(extra_missing) == 0,
        "all_present" if not extra_missing else f"missing={extra_missing}")

    # 5. Engine id
    add(e, "engine_id_matches", d.get("engine") == cfg["engine"],
        f"got={d.get('engine')}")

    # 6. State enum (field may differ)
    field = cfg.get("state_field", "state")
    add(e, "state_valid",
        d.get(field) in cfg["valid_states"],
        f"field={field} got={d.get(field)} valid_n={len(cfg['valid_states'])}")

    # 7. Signal strength
    sig = d.get("signal_strength")
    add(e, "signal_strength_sane",
        isinstance(sig, (int, float)) and 0 <= sig <= 100,
        f"signal_strength={sig}")

    # 8. Trigger conditions
    tc = d.get("trigger_conditions", [])
    add(e, "trigger_conditions_min3",
        isinstance(tc, list) and len(tc) >= 3,
        f"n={len(tc) if isinstance(tc, list) else 'NA'}")

    # 9. Forward horizons
    fe = d.get("forward_expectations", {})
    fe_horizons = cfg.get("forward_horizons", ("1m", "3m", "12m"))
    add(e, "forward_horizons_complete",
        isinstance(fe, dict) and all(h in fe for h in fe_horizons),
        f"got={list(fe.keys())[:6] if isinstance(fe, dict) else type(fe).__name__} expected={fe_horizons}")

    # 10. Trade ticket (may be nested)
    trade_field = cfg.get("trade_field")
    if trade_field:
        trade = get_nested(d, trade_field)
        if trade is None:
            add(e, "trade_ticket_present", False, f"path={trade_field} not_found")
        elif isinstance(trade, dict):
            # Look for any "primary"/"instrument"/"thesis"-like key with content
            content_keys = ["primary", "instrument", "thesis", "ticker",
                            "company", "trade", "edge"]
            has_content = any(trade.get(k) for k in content_keys)
            add(e, "trade_ticket_present", has_content,
                f"path={trade_field} keys={list(trade.keys())[:6]}")
        else:
            add(e, "trade_ticket_present", False,
                f"path={trade_field} type={type(trade).__name__}")
    else:
        add(e, "trade_ticket_present", True, "no trade field expected for this edge")

    # 11. Why-now
    why = d.get("why_now_explainer", "")
    min_len = cfg.get("min_why_now_len", 200)
    add(e, "why_now_substantive",
        isinstance(why, str) and len(why) >= min_len,
        f"len={len(why) if isinstance(why, str) else 0} min={min_len}")

    # 12. Data recency
    try:
        ts = dt.datetime.fromisoformat(d["as_of"].replace("Z", "+00:00"))
        now = dt.datetime.now(dt.timezone.utc)
        age_h = (now - ts).total_seconds() / 3600
        max_h = cfg.get("data_age_max_h", 48)
        add(e, f"data_recent_{max_h}h", age_h <= max_h,
            f"age_hours={round(age_h, 1)} max={max_h}")
    except Exception as ex:
        add(e, "data_recent", False, str(ex)[:80])

    # 13. SSM (optional)
    ssm_key = cfg.get("ssm_key")
    if ssm_key:
        try:
            p = ssm.get_parameter(Name=ssm_key)
            val = json.loads(p["Parameter"]["Value"])
            add(e, "ssm_state_present", True, f"state={val.get('state', '?')}")
        except (ClientError, json.JSONDecodeError, KeyError) as ex:
            add(e, "ssm_state_present", False, str(ex)[:120])
    else:
        add(e, "ssm_state_present", True, "engine does not use SSM persistence")

    # 14. Page reachable + wired
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{cfg['page']}",
                                     headers={"User-Agent": "ops/955"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        data_file = cfg["s3_key"].split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and data_file in body
        add(e, "page_live_and_wired", ok,
            f"status={resp.status} len={len(body)} wired={data_file in body}")
    except Exception as ex:
        add(e, "page_live_and_wired", False, str(ex)[:120])


def main():
    print(f"ops 955 -- final v3 unified re-verify at {dt.datetime.utcnow().isoformat()}Z")
    for cfg in EDGES:
        print(f"\n--- Edge #{cfg['edge']}: {cfg['engine']} ---")
        try:
            verify_edge(cfg)
        except Exception as ex:
            add(cfg["edge"], "unhandled_exception", False, str(ex)[:200])

    per_edge = {}
    for c in ALL_CHECKS:
        eid = c["edge"]
        per_edge.setdefault(eid, {"total": 0, "passed": 0, "failed": 0})
        per_edge[eid]["total"] += 1
        if c["passed"]:
            per_edge[eid]["passed"] += 1
        else:
            per_edge[eid]["failed"] += 1

    overall_p = sum(p["passed"] for p in per_edge.values())
    overall_t = sum(p["total"] for p in per_edge.values())

    rep = {
        "ops": 955,
        "title": "final v3 unified re-verify of edges 5-10",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "per_edge_summary": per_edge,
        "checks": ALL_CHECKS,
        "summary": {"total": overall_t, "passed": overall_p,
                    "failed": overall_t - overall_p},
        "overall_ok": overall_p == overall_t,
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/955_edges_5_thru_10_reverify_v3.json", "w") as f:
        json.dump(rep, f, indent=2)

    print("\n=== PER-EDGE SUMMARY ===")
    for eid in sorted(per_edge):
        p = per_edge[eid]
        print(f"  Edge #{eid:2}  pass={p['passed']:2}/{p['total']:2}  fail={p['failed']:2}")
    print(f"\n=== OVERALL  pass={overall_p}/{overall_t} "
          f"({100*overall_p//max(overall_t,1)}%) ===")
    failed = [c for c in ALL_CHECKS if not c["passed"]]
    if failed:
        print(f"\n=== {len(failed)} FAILED CHECKS ===")
        for c in failed:
            print(f"  [FAIL] {c['name']:42} {c['detail'][:90]}")


if __name__ == "__main__":
    main()
