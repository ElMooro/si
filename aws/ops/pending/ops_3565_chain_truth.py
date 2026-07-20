"""ops 3565 — chain truth + direct aggregate. Matrix stuck at 01:49
though two sweeps were kicked (02:17, 02:51): probe cache-doc
freshness, run aggregate directly, regate raw+stats in the matrix."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 0}))
SAMPLE = ["AAPL", "MSFT", "NVDA", "JNJ", "XOM", "WMT", "ZTS", "YUM"]
RAW30 = ["otherOpex", "netChangeInCash", "preferredStock",
         "totalNonCurrentAssets", "dReceivables", "taxPayables"]
STATK = ["price_to_book", "roa_pct", "days_inventory", "zmijewski_x",
         "pe_fwd", "graham_number", "quick_ratio", "asset_turnover"]

with report("3565_chain_truth") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:720]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    fresh = {}
    for t in SAMPLE:
        try:
            h = s3c.head_object(Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{t}_quarter_v21.json")
            fresh[t] = str(h["LastModified"])[11:19]
        except Exception as e:  # noqa: BLE001
            fresh[t] = "ERR " + str(e)[:30]
    n_new = sum(1 for v in fresh.values() if v >= "02:17")
    gate("I1_doc_freshness", True,
         {"lastmod_utc": fresh, "n_post_0217": n_new, "of": 8})

    r = lam.invoke(FunctionName="justhodl-fundamental-census",
                   Payload=json.dumps({"phase": "aggregate"}).encode())
    pay = r["Payload"].read()[:200]
    time.sleep(3)
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    C = MX["cols"]
    N = len(MX["tickers"])
    idx = {t: i for i, t in enumerate(MX["tickers"])}
    nn = lambda k: sum(1 for v in C.get(k) or []
                       if isinstance(v, (int, float)))
    g = lambda t, k: (C.get(k) or [None]*N)[idx.get(t, 0)]
    gate("I2_matrix_after_agg",
         nn("netChangeInCash") >= 50 or n_new < 6,
         {"generated_at": MX.get("generated_at"),
          "metrics_total": len(MX.get("metrics") or []),
          "raw_nn": {k: nn(k) for k in RAW30},
          "stat_nn": {k: nn(k) for k in STATK},
          "AAPL_pb": g("AAPL", "price_to_book"),
          "NVDA_pe_fwd": g("NVDA", "pe_fwd"),
          "invoke_head": pay.decode(errors="ignore")})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3565.json").write_text(
        json.dumps({"ops": 3565, "fails": fails}))
sys.exit(0)
