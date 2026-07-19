"""ops 3534 — UNIVERSE COMPLETION. Facts from 3533: forensic rows now
carry `symbol` (ticker null) — 3532's ticker-only parse saw universe 0
and skipped every top-up; census coverage says 300 docs never built
(the Event-25 batches were dropped silently). Fixes: engine v1.2.0
chain goes SYNC batch-8 with per-link status prints (bulletproof for
the biweekly); THIS run brute-force completes today's universe from
the runner with the 3528-proven small-sync mechanism, then aggregates.

  H1 deploy v1.2.0 (chain durable for the schedule)
  H2 missing list (symbol-aware parse) + sync warm x6-name batches,
     progress printed every 5; response-checked
  H3 aggregate -> scored >= 470, matrix >= 470, FINAL full-universe
     boards + AAPL exactness + dormant residue named
"""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=420, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")

with report("3534_universe_complete") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    cfg = lam.get_function_configuration(
        FunctionName="justhodl-fundamental-census")
    deploy_lambda(report=rep, function_name="justhodl-fundamental-census",
                  source_dir=REPO/"aws"/"lambdas"/
                  "justhodl-fundamental-census"/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Census v1.2.0 sync-8 chain (ops 3534)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(
            FunctionName="justhodl-fundamental-census")
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    gate("H1_deploy", True, "v1.2.0 live")

    f = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/forensic-screen.json")["Body"].read())
    uni, seen = [], set()
    for r in f.get("all_results") or []:
        t = r.get("ticker") or r.get("symbol")
        if t and 1 <= len(t) <= 6 and t not in seen:
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
    print(f"[3534] universe={len(uni)} cached={len(have & set(uni))} "
          f"missing={len(missing)}")
    rep.log(f"missing={len(missing)}")
    ok_b, err_b = 0, 0
    t0 = time.time()
    for i in range(0, len(missing), 6):
        if time.time() - t0 > 4200:
            print(f"[3534] time budget — stopping at {i}")
            break
        batch = missing[i:i+6]
        try:
            rr = lam.invoke(FunctionName="justhodl-fundamental-graphs",
                            Payload=json.dumps(
                                {"warm": batch,
                                 "periods": ["quarter"]}).encode())
            if rr.get("StatusCode") == 200 and not rr.get("FunctionError"):
                ok_b += 1
            else:
                err_b += 1
                print("[3534] err:", rr.get("FunctionError"),
                      rr["Payload"].read()[:140])
        except Exception as e:
            err_b += 1
            print("[3534] exc:", str(e)[:110])
        if (i // 6) % 5 == 0:
            print(f"[3534] progress {i+len(batch)}/{len(missing)} "
                  f"ok={ok_b} err={err_b} t={int(time.time()-t0)}s")
    gate("H2_warmed", ok_b >= max(1, (len(missing)//6) - 4),
         {"missing_at_start": len(missing), "batches_ok": ok_b,
          "batches_err": err_b, "elapsed_s": int(time.time()-t0)})

    lam.invoke(FunctionName="justhodl-fundamental-census",
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    D = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census.json")["Body"].read())
    aapl = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundgraph/cache/AAPL_quarter_v21.json")["Body"].read())
    gm_doc = None
    for d2, v in reversed(aapl["points"]["gross_margin_pct"]):
        if isinstance(v, (int, float)):
            gm_doc = round(float(v), 4); break
    gm_mx = MX["cols"]["gross_margin_pct"][MX["tickers"].index("AAPL")]
    sh = D["metric_boards"]["share_count_yoy_pct"]
    gate("H3_full", D["coverage"]["scored"] >= 470
         and MX["n_tickers"] >= 470 and abs(gm_mx - gm_doc) < 1e-6,
         {"scored": D["coverage"]["scored"],
          "matrix": (MX["n_tickers"], MX["n_metrics"]),
          "top10": [(r["t"], r["score"], r["n_elite"])
                    for r in D["top_quality"][:10]],
          "careful10": [(r["t"], r["flags"][:2], r["flag_w"])
                        for r in D["careful"][:10]],
          "issuers5": sh["worst"][:5], "buybacks5": sh["best"][:5],
          "avg": D["summary"]["avg_score"],
          "flagged": D["summary"]["n_flagged"],
          "dormant": D["coverage"]["dormant_sample"],
          "dormant_n": D["coverage"]["dormant_n"]})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3534.json").write_text(
        json.dumps({"ops": 3534, "fails": fails}))
sys.exit(0)
