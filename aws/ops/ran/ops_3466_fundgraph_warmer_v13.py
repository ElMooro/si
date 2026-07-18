"""ops 3466 — Fundamental Graphs: self-sustaining warmer + page v1.3.

Engine v1.1.1: every URL request drops a demand marker
(data/fundgraph/hits/{SYM}); warm_auto rebuilds STATIC_CORE (36) UNION
symbols hit in the last 7 days (cap 60), time-guarded, annuals refreshed
Mondays. Daily EventBridge Scheduler (fleet pattern, justhodl-scheduler-role)
fires it 09:25 UTC pre-open. Page v1.3: log scale, today divider + dashed
estimate series, CSV/PNG export, one-tap "Load favs".

Gates:
  W1  deploy v1.1.1 (timeout 900) + warm_auto smoke: built >= 30, no core
      errors, elapsed logged
  W2  demand tracking: URL hit on TGT -> hits/TGT object exists + doc ok
  W3  scheduler 'fundamental-graphs-warmer-sched' ENABLED daily with
      warm_auto Input
  W4  page v1.3 live (ops3466 + fgExportCSV + fgExportPNG + logbtn markers)
"""
import gzip
import io
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report                      # noqa: E402
from _lambda_deploy_helpers import deploy_lambda   # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
ACCT = "857687956942"
SCHED_NAME = "fundamental-graphs-warmer-sched"

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=920, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
iam = boto3.client("iam")

with report("3466_fundgraph_warmer_v13") as rep:
    out = {"ops": 3466, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3466 — warmer + page v1.3 (log/today/export/favload)")

    # W1 — deploy + warm_auto smoke
    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs API v1.1.1 + warm_auto (ops 3466)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    t0 = time.time()
    r = lam.invoke(FunctionName=FN,
                   Payload=json.dumps({"warm_auto": True}).encode())
    wp = json.loads(r["Payload"].read() or b"{}")
    gate("W1_warm_auto_smoke",
         wp.get("ok") and wp.get("version") == "1.1.1"
         and wp.get("built", 0) >= 30 and not wp.get("errors"),
         {"built": wp.get("built"), "symbols_n": wp.get("symbols_n"),
          "elapsed_s": wp.get("elapsed_s"),
          "skipped": len(wp.get("skipped_for_time") or []),
          "errors": wp.get("errors"), "wall_s": round(time.time() - t0, 1)})

    # W2 — demand tracking through the public URL
    try:
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        req = urllib.request.Request(f"{url}/?symbol=TGT&period=quarter",
                                     headers={"User-Agent": "ops-3466",
                                              "Accept-Encoding": "gzip"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read()
            if (resp.headers.get("Content-Encoding") or "").lower() == "gzip":
                raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        d = json.loads(raw)
        time.sleep(2)
        hit = s3.head_object(Bucket=BUCKET, Key="data/fundgraph/hits/TGT")
        gate("W2_demand_tracking",
             d.get("ok") and d.get("symbol") == "TGT" and bool(hit),
             {"doc_ok": d.get("ok"), "keys": len(d.get("points", {})),
              "hit_marker": bool(hit)})
    except Exception as e:  # noqa: BLE001
        gate("W2_demand_tracking", False, str(e)[:220])

    # W3 — daily scheduler (fleet pattern)
    try:
        role = iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
        farn = f"arn:aws:lambda:us-east-1:{ACCT}:function:{FN}"
        args = dict(Name=SCHED_NAME,
                    ScheduleExpression="cron(25 9 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": farn, "RoleArn": role,
                            "Input": json.dumps({"warm_auto": True})},
                    State="ENABLED")
        try:
            sch.update_schedule(**args)
            act = "updated"
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(**args)
            act = "created"
        got = sch.get_schedule(Name=SCHED_NAME)
        gate("W3_daily_scheduler",
             got["State"] == "ENABLED"
             and "warm_auto" in got["Target"].get("Input", ""),
             {"act": act, "expr": got["ScheduleExpression"],
              "state": got["State"]})
    except Exception as e:  # noqa: BLE001
        gate("W3_daily_scheduler", False, str(e)[:220])

    # W4 — page v1.3 live
    page_ok, det = False, {}
    for _ in range(21):
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/fundamental-graphs.html?cb={int(time.time())}",
                headers={"User-Agent": "ops-3466"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                b = resp.read()
            page_ok = all(m in b for m in (b"ops3466", b"fgExportCSV",
                                           b"fgExportPNG", b"logbtn", b"favload"))
            det = {"status": 200, "markers": page_ok}
        except Exception as e:  # noqa: BLE001
            det = {"err": str(e)[:120]}
        if page_ok:
            break
        time.sleep(20)
    gate("W4_page_v13_live", page_ok, det)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3466.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
