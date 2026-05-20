"""
ops 967 -- final definitive 100% verification (10 edges + signal-board)
========================================================================
After Edge #2 state field added (commit 66913aa4) + signal-board
normaliser refreshed, this is the canonical 100% gate.
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
                   config=Config(read_timeout=120, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

EDGES = [
    {"edge": 1, "name": "VIX Backwardation Trigger",
     "lambda": "justhodl-vix-backwardation-trigger",
     "s3_key": "data/vix-backwardation-trigger.json",
     "page": "vix-capitulation.html"},
    {"edge": 2, "name": "Insider Buy Clusters",
     "lambda": "justhodl-insider-buys-enriched",
     "s3_key": "data/insider-buys-enriched.json",
     "page": "insider-buys.html",
     "expect_state_in": ("FRESH_HIGH_CONVICTION", "ELEVATED", "NORMAL", "QUIET")},
    {"edge": 3, "name": "Zweig Breadth Thrust",
     "lambda": "justhodl-breadth-thrust",
     "s3_key": "data/breadth-thrust.json",
     "page": "breadth-thrust.html"},
    {"edge": 4, "name": "Vol-Target Fund Unwind",
     "lambda": "justhodl-vol-target-unwind",
     "s3_key": "data/vol-target-unwind.json",
     "page": "vol-target-unwind.html"},
    {"edge": 5, "name": "Russell Recon Front-Run",
     "lambda": "justhodl-russell-recon-frontrun",
     "s3_key": "data/russell-recon-frontrun.json",
     "page": "russell-recon.html"},
    {"edge": 6, "name": "Buyback Auth Scanner",
     "lambda": "justhodl-buyback-scanner",
     "s3_key": "data/buyback-scanner.json",
     "page": "buyback-scanner.html"},
    {"edge": 7, "name": "Stablecoin Mint Flow",
     "lambda": "justhodl-stablecoin-flow",
     "s3_key": "data/stablecoin-flow.json",
     "page": "stablecoin-flow.html"},
    {"edge": 8, "name": "OPEX Gamma Calendar",
     "lambda": "justhodl-opex-calendar",
     "s3_key": "data/opex-calendar.json",
     "page": "opex-calendar.html"},
    {"edge": 9, "name": "Activist 13D Alert",
     "lambda": "justhodl-activist-13d",
     "s3_key": "data/activist-13d.json",
     "page": "activist-13d.html"},
    {"edge": 10, "name": "RV-IV / Implied Dispersion",
     "lambda": "justhodl-rv-iv-scanner",
     "s3_key": "data/rv-iv-scanner.json",
     "page": "rv-iv-scanner.html"},
]

CHECKS = []


def add(edge, name, ok, det=""):
    CHECKS.append({"edge": edge, "name": f"e{edge}.{name}",
                   "passed": bool(ok), "detail": str(det)[:250]})


def verify_edge(cfg):
    e = cfg["edge"]
    # 1. Lambda
    try:
        lam.get_function(FunctionName=cfg["lambda"])
        add(e, "lambda_deployed", True, "ok")
    except ClientError as ex:
        add(e, "lambda_deployed", False, str(ex)[:120])
        return

    # 2. S3 output (don't re-invoke -- check current state)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=cfg["s3_key"])
        d = json.loads(obj["Body"].read())
        age = (dt.datetime.now(dt.timezone.utc) - obj["LastModified"]).total_seconds()
        add(e, "s3_output_present", True,
            f"size={obj['ContentLength']}B age_h={round(age/3600, 1)}")
    except (ClientError, json.JSONDecodeError) as ex:
        add(e, "s3_output_present", False, str(ex)[:120])
        return

    # 3. Has engine field
    add(e, "engine_field", "engine" in d, f"engine={d.get('engine')}")

    # 4. Has state (or calendar_phase for #5)
    state_field = "calendar_phase" if e == 5 else "state"
    val = d.get(state_field)
    add(e, "has_state", val is not None,
        f"field={state_field} val={val}")

    # 4b. State enum check for edge 2 (newly added)
    if "expect_state_in" in cfg:
        add(e, "state_enum_valid", val in cfg["expect_state_in"],
            f"got={val} expected_in={cfg['expect_state_in']}")

    # 5. Data freshness
    try:
        ts = dt.datetime.fromisoformat(d["as_of"].replace("Z", "+00:00"))
        age_h = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds() / 3600
        add(e, "data_age_under_168h", age_h <= 168, f"age_h={round(age_h, 1)}")
    except Exception as ex:
        add(e, "data_age_under_168h", False, str(ex)[:80])

    # 6. Page wired
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{cfg['page']}",
                                     headers={"User-Agent": "ops/967"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        data_file = cfg["s3_key"].split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and data_file in body
        add(e, "page_wired", ok,
            f"status={resp.status} wired={data_file in body}")
    except Exception as ex:
        add(e, "page_wired", False, str(ex)[:120])


def verify_signal_board():
    print("\n--- Signal Board ---")
    try:
        info = lam.get_function(FunctionName="justhodl-signal-board")
        add(0, "sb.lambda_deployed", True,
            f"runtime={info['Configuration'].get('Runtime')}")
    except ClientError as ex:
        add(0, "sb.lambda_deployed", False, str(ex)[:120])
        return

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        d = json.loads(obj["Body"].read())
        add(0, "sb.s3_output_present", True,
            f"size={obj['ContentLength']}B composite_posture={d.get('composite_posture')}")
        n_eng = d.get("n_engines", 0)
        n_live = d.get("n_live", 0)
        add(0, "sb.n_engines_20plus", n_eng >= 20,
            f"n_engines={n_eng} n_live={n_live}")
        # Check that all 10 edge engines are referenced
        engs = [e.get("engine", "") for e in d.get("engines", [])]
        edge_engines = [e for e in engs if e.startswith("Edge#")]
        add(0, "sb.has_10_edge_engines", len(edge_engines) == 10,
            f"edge_engines_count={len(edge_engines)}")
    except (ClientError, json.JSONDecodeError) as ex:
        add(0, "sb.s3_output_present", False, str(ex)[:120])


def main():
    print(f"ops 967 -- FINAL DEFINITIVE 100% at {dt.datetime.utcnow().isoformat()}Z")
    for cfg in EDGES:
        print(f"\n--- Edge #{cfg['edge']}: {cfg['name']} ---")
        try:
            verify_edge(cfg)
        except Exception as ex:
            add(cfg["edge"], "unhandled", False, str(ex)[:150])
    verify_signal_board()

    per_edge = {}
    for c in CHECKS:
        eid = c["edge"]
        per_edge.setdefault(eid, {"passed": 0, "total": 0})
        per_edge[eid]["total"] += 1
        if c["passed"]:
            per_edge[eid]["passed"] += 1

    op = sum(p["passed"] for p in per_edge.values())
    ot = sum(p["total"] for p in per_edge.values())

    edge_status = {}
    for cfg in EDGES:
        eid = cfg["edge"]
        p = per_edge.get(eid, {"passed": 0, "total": 0})
        edge_status[str(eid)] = {
            "name": cfg["name"], "lambda": cfg["lambda"],
            "passed": p["passed"], "total": p["total"],
            "status": "GREEN" if p["passed"] == p["total"] else
                      ("YELLOW" if p["passed"] >= p["total"] * 0.75 else "RED")}
    sb = per_edge.get(0, {"passed": 0, "total": 0})
    edge_status["SB"] = {"name": "Signal Board (synthesis)",
                         "lambda": "justhodl-signal-board",
                         "passed": sb["passed"], "total": sb["total"],
                         "status": "GREEN" if sb["passed"] == sb["total"] else "RED"}

    rep = {
        "ops": 967,
        "title": "FINAL DEFINITIVE 100% verification (10 edges + signal-board)",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "edge_status": edge_status,
        "checks": CHECKS,
        "summary": {"total": ot, "passed": op, "failed": ot - op,
                    "pct": round(100 * op / max(ot, 1), 1)},
        "overall_ok": op == ot,
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/967_final_definitive_100pct.json", "w") as f:
        json.dump(rep, f, indent=2)

    print("\n=== 10-EDGE INSTITUTIONAL ROADMAP + SIGNAL BOARD ===")
    order = sorted(edge_status.keys(), key=lambda x: (x == "SB", int(x) if x != "SB" else 99))
    for eid in order:
        s = edge_status[eid]
        label = f"Edge #{eid}" if eid != "SB" else "SignalBoard"
        print(f"  {label:>12}  {s['name']:36}  {s['passed']}/{s['total']}  [{s['status']}]")
    print(f"\nOVERALL: {op}/{ot} ({rep['summary']['pct']}%)")
    failed = [c for c in CHECKS if not c["passed"]]
    if failed:
        print(f"\n{len(failed)} FAILED:")
        for c in failed:
            print(f"  [FAIL] {c['name']:38} {c['detail'][:100]}")


if __name__ == "__main__":
    main()
