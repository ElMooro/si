"""ops 3560 — flagship R/R regate (3559-D2 was a gate KeyError: the
flagship matrix carries no top-level 'n'; use len(tickers))."""
import json, sys, time
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
with report("3560_flagship_rr_regate") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:700]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    lam.invoke(FunctionName="justhodl-fundamental-census",
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    C = MX["cols"]; N = len(MX["tickers"])
    rows = [(MX["tickers"][i], C["rr_ratio"][i], C["upside_pct"][i],
             C["downside_pct"][i]) for i in range(N)
            if isinstance((C.get("rr_ratio") or [None]*N)[i],
                          (int, float))]
    rows.sort(key=lambda x: -x[1])
    upl = sorted([(MX["tickers"][i], C["upside_pct"][i]) for i in
                  range(N) if isinstance((C.get("upside_pct") or
                                          [None]*N)[i], (int, float))],
                 key=lambda x: -x[1])[:6]
    dnl = sorted([(MX["tickers"][i], C["downside_pct"][i]) for i in
                  range(N) if isinstance((C.get("downside_pct") or
                                          [None]*N)[i], (int, float))],
                 key=lambda x: x[1])[:6]
    gate("E1_flagship_rr", len(rows) >= 430
         and all(0 < r[1] <= 30 for r in rows),
         {"n": len(rows), "best_rr": rows[:10], "worst_rr": rows[-5:],
          "top_upside": upl, "lowest_downside": dnl})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3560.json").write_text(
        json.dumps({"ops": 3560, "fails": fails}))
sys.exit(0)
