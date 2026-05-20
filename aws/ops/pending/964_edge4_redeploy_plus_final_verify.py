"""
ops 964 -- redeploy Edge #4 (fixed fmp_history) + FINAL 10-edge verify
=======================================================================

After the from/to params removed from fmp_history, manually update the
vol-target-unwind Lambda code via Boto3 (CI not reliable). Then run the
FINAL 10-edge verification across all engines.

Expected: 10/10 edges PASS.
"""
import datetime as dt
import io
import json
import os
import time
import urllib.request
import zipfile

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

CHECKS = []


def add(name, ok, detail=""):
    CHECKS.append({"name": name, "passed": bool(ok), "detail": str(detail)[:280]})


def zip_dir(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                arc = os.path.relpath(full, src_dir)
                zf.write(full, arc)
    buf.seek(0)
    return buf.getvalue()


# ----- Phase 1: redeploy + invoke Edge #4 -----
def redeploy_edge_4():
    fn = "justhodl-vol-target-unwind"
    src = "aws/lambdas/justhodl-vol-target-unwind/source"
    print(f"\n=== Phase 1: redeploy {fn} ===")
    try:
        z = zip_dir(src)
        add("e4.zip_built", True, f"{len(z)}B")
    except Exception as ex:
        add("e4.zip_built", False, str(ex)[:200])
        return

    try:
        lam.update_function_code(FunctionName=fn, ZipFile=z, Publish=False)
        add("e4.code_updated", True, "ok")
    except ClientError as ex:
        add("e4.code_updated", False, str(ex)[:200])
        return

    # Wait
    for _ in range(15):
        v = lam.get_function_configuration(FunctionName=fn)
        if v.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    # Invoke
    print(f"  invoking {fn}...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload = r["Payload"].read().decode()
        try:
            body = json.loads(payload)
            inner = body.get("statusCode", 200)
        except Exception:
            inner = "n/a"
        ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
        add("e4.invoke_post_fix", ok,
            f"dur={dur}s outer={r['StatusCode']} inner={inner} body={payload[:200]}")
    except ClientError as ex:
        add("e4.invoke_post_fix", False, str(ex)[:200])

    time.sleep(2)
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key="data/vol-target-unwind.json")
        age = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
        add("e4.s3_fresh", h["ContentLength"] > 500 and age < 600,
            f"size={h['ContentLength']}B age_s={int(age)}")
    except ClientError as ex:
        add("e4.s3_fresh", False, str(ex)[:200])


# ----- Phase 2: final 10-edge verify -----
ALL_EDGES = [
    {"edge": 1, "lambda": "justhodl-vix-backwardation-trigger",
     "s3_key": "data/vix-backwardation-trigger.json",
     "page": "vix-capitulation.html"},
    {"edge": 2, "lambda": "justhodl-insider-buys-enriched",
     "s3_key": "data/insider-buys-enriched.json",
     "page": "insider-buys.html"},
    {"edge": 3, "lambda": "justhodl-breadth-thrust",
     "s3_key": "data/breadth-thrust.json",
     "page": "breadth-thrust.html"},
    {"edge": 4, "lambda": "justhodl-vol-target-unwind",
     "s3_key": "data/vol-target-unwind.json",
     "page": "vol-target-unwind.html"},
    {"edge": 5, "lambda": "justhodl-russell-recon-frontrun",
     "s3_key": "data/russell-recon-frontrun.json",
     "page": "russell-recon.html"},
    {"edge": 6, "lambda": "justhodl-buyback-scanner",
     "s3_key": "data/buyback-scanner.json",
     "page": "buyback-scanner.html"},
    {"edge": 7, "lambda": "justhodl-stablecoin-flow",
     "s3_key": "data/stablecoin-flow.json",
     "page": "stablecoin-flow.html"},
    {"edge": 8, "lambda": "justhodl-opex-calendar",
     "s3_key": "data/opex-calendar.json",
     "page": "opex-calendar.html"},
    {"edge": 9, "lambda": "justhodl-activist-13d",
     "s3_key": "data/activist-13d.json",
     "page": "activist-13d.html"},
    {"edge": 10, "lambda": "justhodl-rv-iv-scanner",
     "s3_key": "data/rv-iv-scanner.json",
     "page": "rv-iv-scanner.html"},
]


def verify_edge(cfg):
    e = cfg["edge"]
    # 1. Lambda exists
    try:
        info = lam.get_function(FunctionName=cfg["lambda"])
        add(f"verify.e{e}.lambda_deployed", True,
            f"mod={info['Configuration'].get('LastModified', '')[:19]}")
    except ClientError as ex:
        add(f"verify.e{e}.lambda_deployed", False, str(ex)[:120])
        return

    # 2. S3 output present
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=cfg["s3_key"])
        d = json.loads(obj["Body"].read())
        ts_str = d.get("as_of", "")
        try:
            ts = dt.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            age_h = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds() / 3600
        except Exception:
            age_h = -1
        add(f"verify.e{e}.s3_output", True,
            f"size={obj['ContentLength']}B age_h={round(age_h, 1)}")
    except ClientError as ex:
        add(f"verify.e{e}.s3_output", False, str(ex)[:120])
        return

    # 3. Page reachable + wired
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{cfg['page']}",
                                     headers={"User-Agent": "ops/964"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        data_file = cfg["s3_key"].split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and data_file in body
        add(f"verify.e{e}.page_live_and_wired", ok,
            f"status={resp.status} wired={data_file in body}")
    except Exception as ex:
        add(f"verify.e{e}.page_live_and_wired", False, str(ex)[:120])


def main():
    print(f"ops 964 at {dt.datetime.utcnow().isoformat()}Z")
    redeploy_edge_4()

    print("\n=== Phase 2: final 10-edge verification ===")
    for cfg in ALL_EDGES:
        verify_edge(cfg)

    # Phase 3: invoke signal-board so it picks up the new feeds
    print("\n=== Phase 3: kick signal-board to ingest fresh feeds ===")
    try:
        r = lam.invoke(FunctionName="justhodl-signal-board",
                       InvocationType="RequestResponse", Payload=b"{}")
        payload = r["Payload"].read().decode()
        ok = r["StatusCode"] == 200 and not r.get("FunctionError")
        add("signal_board.invoke", ok, f"status={r['StatusCode']} body={payload[:240]}")
    except ClientError as ex:
        add("signal_board.invoke", False, str(ex)[:200])

    # Read signal-board output
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        d = json.loads(obj["Body"].read())
        n_live = d.get("n_live", 0)
        n_engines = d.get("n_engines", 0)
        posture = d.get("composite_posture", "?")
        add("signal_board.aggregating",
            n_engines >= 20 and n_live >= 10,
            f"engines={n_engines} live={n_live} posture={posture}")
    except Exception as ex:
        add("signal_board.aggregating", False, str(ex)[:200])

    rep = {
        "ops": 964,
        "title": "Edge #4 redeploy + FINAL 10-edge verify + signal-board kick",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/964_final_10_edge_complete.json", "w") as f:
        json.dump(rep, f, indent=2)

    p = rep["summary"]["passed"]
    t = rep["summary"]["total"]
    print(f"\n=== FINAL: {p}/{t} ({100*p//max(t,1)}%) ===")
    for c in CHECKS:
        flag = "OK " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:42} {c['detail'][:120]}")


if __name__ == "__main__":
    main()
