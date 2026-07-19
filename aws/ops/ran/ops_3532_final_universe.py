"""ops 3532 — census full-universe close. 3531-E2's flat trajectory was
a GATE bug: it polled the matrix, which only the FINAL aggregate
writes; warm phases are silent. Correct odometer = v21 cache count.
This ops reads the truth, tops up any residue via a bounded direct
warm loop (sync, small batches of 8 — no chain dependency), runs the
aggregate, and regates the full universe.

  F1 cache truth: v21 doc count + which universe names missing
  F2 top-up: warm missing names in batches of 8 (sync, <=12 batches
     here; anything beyond is listed for the biweekly run)
  F3 aggregate -> matrix >= 450 tickers, AAPL exactness, census
     scored >= 450, FINAL boards printed
"""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=880, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")

with report("3532_final_universe") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    uni_doc = json.loads(s3c.get_object(
        Bucket=BUCKET, Key="data/forensic-screen.json")["Body"].read())
    uni, seen = [], set()
    for r in uni_doc.get("all_results") or []:
        t = r.get("ticker")
        if t and t not in seen:
            seen.add(t); uni.append(t)
    have = set()
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "data/fundgraph/cache/",
              "MaxKeys": 1000}
        if token: kw["ContinuationToken"] = token
        r = s3c.list_objects_v2(**kw)
        for o in r.get("Contents") or []:
            k = o["Key"]
            if k.endswith("_quarter_v21.json"):
                have.add(k.split("/")[-1].split("_quarter")[0])
        if not r.get("IsTruncated"): break
        token = r["NextContinuationToken"]
    missing = [t for t in uni if t not in have]
    gate("F1_cache_truth", True,
         {"universe": len(uni), "cached": len(have & set(uni)),
          "missing_n": len(missing), "missing_head": missing[:15]})

    warmed = 0
    for i in range(0, min(len(missing), 96), 8):
        batch = missing[i:i+8]
        try:
            rr = lam.invoke(FunctionName="justhodl-fundamental-graphs",
                            Payload=json.dumps(
                                {"warm": batch,
                                 "periods": ["quarter"]}).encode())
            ok = rr.get("StatusCode") == 200 and not rr.get("FunctionError")
            warmed += len(batch) if ok else 0
            if not ok:
                print("[topup] batch err:", rr.get("FunctionError"),
                      rr["Payload"].read()[:120])
        except Exception as e:
            print("[topup] batch exc:", str(e)[:100])
    gate("F2_topup", True, {"attempted": min(len(missing), 96),
                            "warmed_ok": warmed,
                            "left_for_schedule": max(0, len(missing)-96)})

    lam.invoke(FunctionName="justhodl-fundamental-census",
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    MX = json.loads(s3c.get_object(
        Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    D = json.loads(s3c.get_object(
        Bucket=BUCKET,
        Key="data/fundamental-census.json")["Body"].read())
    aapl = json.loads(s3c.get_object(
        Bucket=BUCKET,
        Key="data/fundgraph/cache/AAPL_quarter_v21.json")["Body"].read())
    gm_doc = None
    for d2, v in reversed(aapl["points"]["gross_margin_pct"]):
        if isinstance(v, (int, float)):
            gm_doc = round(float(v), 4); break
    gm_mx = MX["cols"]["gross_margin_pct"][MX["tickers"].index("AAPL")]
    sh = D["metric_boards"]["share_count_yoy_pct"]
    gate("F3_full", MX["n_tickers"] >= 450
         and D["coverage"]["scored"] >= 450
         and abs(gm_mx - gm_doc) < 1e-6,
         {"matrix": (MX["n_tickers"], MX["n_metrics"]),
          "scored": D["coverage"]["scored"],
          "aapl_gm": (gm_mx, gm_doc),
          "top10": [(r["t"], r["score"], r["n_elite"])
                    for r in D["top_quality"][:10]],
          "careful10": [(r["t"], r["flags"][:2], r["flag_w"])
                        for r in D["careful"][:10]],
          "issuers5": sh["worst"][:5], "buybacks5": sh["best"][:5],
          "avg": D["summary"]["avg_score"],
          "flagged": D["summary"]["n_flagged"],
          "dormant": D["coverage"]["dormant_sample"]})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3532.json").write_text(
        json.dumps({"ops": 3532, "fails": fails}))
sys.exit(0)
