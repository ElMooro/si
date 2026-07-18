"""ops 3485 — whale fusion FINAL: cohort semantics + schema-truth gate.

3484 revealed the feed's true scope: data/13f-flows-by-ticker.json is the
CONTRIBUTOR/WHALE-COHORT ledger (writer: 13f-positions ops3299 block) —
n/b/s are dollar flows within the tracked clone-alpha cohort, nf its fund
count (GOOGL: net +$6.36B, 14 funds, $30.1B held). The +$11.5B memory
figure belongs to the ALL-institutions dollar-flow join (different feed).
Kept per real-data doctrine, relabeled honestly on both surfaces.
v1.3.2 passes bought/sold through so the gate can assert n == b - s.

  Q1 GOOGL: net == bought - sold (schema truth), n_funds >= 5,
     held > $1B, |net| > $100M
  Q2 both surfaces live with cohort labels
"""
import json, sys, time, urllib.request
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
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3485"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3485_whale_semantics") as rep:
    out = {"ops": 3485, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3485 — whale cohort semantics final")

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.3.2 whale semantics (ops 3485)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    wp = json.loads(lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["GOOGL"], "periods": ["quarter"],
         "refresh": True}).encode())["Payload"].read() or b"{}")
    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/GOOGL_quarter_v13.json")["Body"].read())
        wq = doc.get("whales_q") or {}
        b, s2, n = wq.get("bought_usd"), wq.get("sold_usd"), wq.get("net_usd")
        gate("Q1_schema_truth",
             wp.get("version") == "1.3.2" and None not in (b, s2, n)
             and abs((b - s2) - n) <= 1 and (wq.get("n_funds") or 0) >= 5
             and (wq.get("held_usd") or 0) > 1e9 and abs(n) > 1e8,
             {"version": wp.get("version"), "bought": b, "sold": s2,
              "net": n, "n_minus_bs": (b - s2) - n if None not in (b, s2, n) else None,
              "n_funds": wq.get("n_funds"), "held": wq.get("held_usd")})
    except Exception as e:
        gate("Q1_schema_truth", False, str(e)[:260])

    fl = wy = b""
    for _ in range(18):
        try:
            cb = int(time.time())
            fl = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            wy = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"tracked whale cohort" in fl and b"tracked whale cohort" in wy:
                break
        except Exception:
            pass
        time.sleep(20)
    gate("Q2_cohort_labels",
         b"tracked whale cohort" in fl and b"tracked whale cohort" in wy
         and b"ops3475" in wy, {"flag": b"tracked whale cohort" in fl,
                                "why": b"tracked whale cohort" in wy})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3485.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
