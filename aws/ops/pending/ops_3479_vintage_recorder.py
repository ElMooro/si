"""ops 3479 — Tier-1c: estimate-vintage recorder (self-built IBES moat).

Every doc BUILD (on-demand refresh + the daily warm_auto over core∪hits)
appends one row/day to data/fundgraph/vintage/{SYM}.json: the street's
current forward revenue/EPS view (next 6 periods). Revision-momentum
series light up automatically as history accrues. Doc carries
vintage_days. Cap 500 rows; idempotent per day.

Gates:
  V1 warm refresh -> vintage files exist for CHTR/AAPL/MSFT with today's
     row, eps+rev forward lists with FUTURE dates, doc.vintage_days >= 1
  V2 idempotency: second warm same day -> row counts unchanged
"""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

with report("3479_vintage_recorder") as rep:
    out = {"ops": 3479, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:360]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3479 — estimate-vintage recorder (clock starts now)")

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.2.1 vintage recorder (ops 3479)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)

    def warm():
        r = lam.invoke(FunctionName=FN, Payload=json.dumps(
            {"warm": ["CHTR", "AAPL", "MSFT"], "periods": ["quarter"],
             "refresh": True}).encode())
        return json.loads(r["Payload"].read() or b"{}")

    wp1 = warm()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    counts = {}
    ok1, det = True, {}
    for sym in ("CHTR", "AAPL", "MSFT"):
        try:
            led = json.loads(s3.get_object(
                Bucket=BUCKET, Key=f"data/fundgraph/vintage/{sym}.json")["Body"].read())
            rows = led.get("rows", [])
            last = rows[-1] if rows else {}
            fut = all(d > today for d, _ in (last.get("eps") or [])[:2] +
                      (last.get("rev") or [])[:2])
            doc = json.loads(s3.get_object(
                Bucket=BUCKET, Key=f"data/fundgraph/cache/{sym}_quarter_v12.json")["Body"].read())
            counts[sym] = len(rows)
            det[sym] = {"rows": len(rows), "today": last.get("d") == today,
                        "eps_fwd": len(last.get("eps") or []),
                        "rev_fwd": len(last.get("rev") or []),
                        "vintage_days": doc.get("vintage_days")}
            if not (rows and last.get("d") == today and fut
                    and (last.get("eps") or last.get("rev"))
                    and doc.get("vintage_days", 0) >= 1):
                ok1 = False
        except Exception as e:
            ok1 = False; det[sym] = str(e)[:120]
    gate("V1_vintage_recorded", ok1 and wp1.get("version") == "1.2.1", det)

    warm()
    ok2, det2 = True, {}
    for sym, n0 in counts.items():
        try:
            led = json.loads(s3.get_object(
                Bucket=BUCKET, Key=f"data/fundgraph/vintage/{sym}.json")["Body"].read())
            det2[sym] = {"before": n0, "after": len(led.get("rows", []))}
            if len(led.get("rows", [])) != n0:
                ok2 = False
        except Exception as e:
            ok2 = False; det2[sym] = str(e)[:120]
    gate("V2_same_day_idempotent", ok2, det2)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3479.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
