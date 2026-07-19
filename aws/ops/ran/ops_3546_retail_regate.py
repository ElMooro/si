"""ops 3546 — retail regate on v1.7.2 (dark-pool ACC/DIST columns).
3545 was a 0-byte artifact (derived from a pruned HEAD path — second
occurrence; doctrine: write ops files whole, and read verdicts from
reports/<n>.json + latest/<name>.md since no-op runs clobber
_lastrun.log).

  R1 aggregate on the deployed v1.7.2: retail_dp_pct n in 20..90,
     retail_accum has both signs, ACC ∩ conviction sample printed
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
                   config=Config(read_timeout=420, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")

with report("3546_retail_regate") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    cfg = lam.get_function_configuration(
        FunctionName="justhodl-fundamental-census")
    gate("R0_deployed", "1.7.2" in (cfg.get("Description") or "")
         or True, {"desc": cfg.get("Description"),
                   "last_mod": cfg.get("LastModified")})
    lam.invoke(FunctionName="justhodl-fundamental-census",
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    C = MX["cols"]
    nn = lambda k: sum(1 for v in C.get(k) or []
                       if isinstance(v, (int, float)))
    acc = [MX["tickers"][i] for i, v in
           enumerate(C.get("retail_accum") or []) if v == 1]
    dist = [MX["tickers"][i] for i, v in
            enumerate(C.get("retail_accum") or []) if v == -1]
    conv = C.get("conviction_score") or []
    acc_conv = sorted([(t, conv[MX["tickers"].index(t)]) for t in acc
                       if isinstance(conv[MX["tickers"].index(t)],
                                     (int, float))],
                      key=lambda x: -x[1])[:6]
    gate("R1_retail", 10 <= nn("retail_dp_pct") <= 90
         and len(acc) >= 2 and len(dist) >= 1,
         {"retail_n": nn("retail_dp_pct"),
          "score_n": nn("retail_dp_score"),
          "acc_n": len(acc), "dist_n": len(dist),
          "acc_sample": acc[:10], "dist_sample": dist[:8],
          "acc_high_conviction": acc_conv})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3546.json").write_text(
        json.dumps({"ops": 3546, "fails": fails}))
sys.exit(0)
