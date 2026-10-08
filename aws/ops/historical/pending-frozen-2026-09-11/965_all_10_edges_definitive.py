"""
ops 965 -- definitive 10-edge institutional roadmap status
============================================================

After all source fixes have landed (Edge #4 FMP retry resilience,
Edge #5 forward_expectations, Edge #1-3 deploy), this is the
canonical post-mortem of all 10 institutional alpha engines.

For each edge:
  - Lambda deployed under expected name
  - Most recent S3 output at the page-expected key
  - Output age < 168 hours
  - Page reachable + wired to data file

Failures here indicate REAL production gaps to fix.
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

EDGES = [
    {"edge": 1, "name": "VIX Backwardation Trigger",
     "lambda": "justhodl-vix-backwardation-trigger",
     "s3_key": "data/vix-backwardation-trigger.json",
     "page": "vix-capitulation.html"},
    {"edge": 2, "name": "Insider Open-Market Buys",
     "lambda": "justhodl-insider-buys-enriched",
     "s3_key": "data/insider-buys-enriched.json",
     "page": "insider-buys.html"},
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
    {"edge": 6, "name": "Buyback Authorization Scanner",
     "lambda": "justhodl-buyback-scanner",
     "s3_key": "data/buyback-scanner.json",
     "page": "buyback-scanner.html"},
    {"edge": 7, "name": "Stablecoin Mint Flow",
     "lambda": "justhodl-stablecoin-flow",
     "s3_key": "data/stablecoin-flow.json",
     "page": "stablecoin-flow.html"},
    {"edge": 8, "name": "OPEX / 0DTE Gamma Calendar",
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


def verify(cfg):
    e = cfg["edge"]

    # 1. Lambda deployed
    try:
        info = lam.get_function(FunctionName=cfg["lambda"])
        c = info.get("Configuration", {})
        add(e, "lambda_deployed", True,
            f"runtime={c.get('Runtime')} mem={c.get('MemorySize')} timeout={c.get('Timeout')}")
    except ClientError as ex:
        add(e, "lambda_deployed", False, str(ex)[:120])
        return

    # 2. Force invoke (resilience: retry once on inner 500)
    print(f"  Edge #{e}: invoking {cfg['lambda']}...")
    success = False
    last_payload = ""
    for attempt in range(2):
        try:
            t0 = time.time()
            r = lam.invoke(FunctionName=cfg["lambda"],
                           InvocationType="RequestResponse", Payload=b"{}")
            payload = r["Payload"].read().decode()
            last_payload = payload
            try:
                body = json.loads(payload)
                inner = body.get("statusCode", 200)
            except Exception:
                inner = "n/a"
            if r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200:
                add(e, "invoke_success", True,
                    f"attempt={attempt+1} dur={round(time.time()-t0,1)}s inner={inner}")
                success = True
                break
            if attempt < 1:
                time.sleep(4)
        except ClientError as ex:
            last_payload = str(ex)
            if attempt < 1:
                time.sleep(4)
    if not success:
        add(e, "invoke_success", False, f"body={last_payload[:200]}")

    # 3. S3 output at expected key
    time.sleep(1)
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=cfg["s3_key"])
        age = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
        add(e, "s3_output_present", h["ContentLength"] > 200,
            f"size={h['ContentLength']}B age_s={int(age)}")
        add(e, "s3_output_recent_168h", age < 168 * 3600,
            f"age_hours={round(age/3600, 1)}")
    except ClientError as ex:
        add(e, "s3_output_present", False, str(ex)[:120])
        add(e, "s3_output_recent_168h", False, "n/a")

    # 4. Page wired
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{cfg['page']}",
                                     headers={"User-Agent": "ops/965"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        data_file = cfg["s3_key"].split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and data_file in body
        add(e, "page_wired", ok,
            f"status={resp.status} wired={data_file in body}")
    except Exception as ex:
        add(e, "page_wired", False, str(ex)[:120])


def main():
    print(f"ops 965 -- DEFINITIVE 10-edge status at {dt.datetime.utcnow().isoformat()}Z")
    for cfg in EDGES:
        print(f"\n--- Edge #{cfg['edge']}: {cfg['name']} ---")
        try:
            verify(cfg)
        except Exception as ex:
            add(cfg["edge"], "unhandled_exception", False, str(ex)[:200])

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
            "name": cfg["name"],
            "lambda": cfg["lambda"],
            "s3_key": cfg["s3_key"],
            "page": cfg["page"],
            "passed": p["passed"], "total": p["total"],
            "status": "GREEN" if p["passed"] == p["total"] else
                      "YELLOW" if p["passed"] >= p["total"] * 0.75 else "RED",
        }

    rep = {
        "ops": 965,
        "title": "DEFINITIVE 10-edge institutional roadmap status",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "edges": edge_status,
        "checks": CHECKS,
        "summary": {"total": ot, "passed": op, "failed": ot - op,
                    "pct": round(100 * op / max(ot, 1), 1)},
        "overall_ok": op == ot,
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/965_all_10_edges_definitive.json", "w") as f:
        json.dump(rep, f, indent=2)

    print("\n=== 10-EDGE INSTITUTIONAL ROADMAP STATUS ===")
    for eid in sorted(edge_status.keys(), key=int):
        s = edge_status[eid]
        print(f"  Edge #{eid:>2}  {s['name']:36}  {s['passed']}/{s['total']}  [{s['status']}]")
    print(f"\nOVERALL: {op}/{ot} ({rep['summary']['pct']}%)")
    failed = [c for c in CHECKS if not c["passed"]]
    if failed:
        print(f"\n{len(failed)} FAILED:")
        for c in failed:
            print(f"  [FAIL] {c['name']:38} {c['detail'][:100]}")


if __name__ == "__main__":
    main()
