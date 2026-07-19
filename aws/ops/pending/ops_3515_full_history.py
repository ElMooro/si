"""ops 3515 — FULL history on Fundamental Graphs (Khalid ask): FMP
statements to inception (FETCH_Q 50->200 quarters, FETCH_A 14->60
years), price from 1962, deep NBER backdrop (8 recessions incl 1970/
73-75/80/81-82/90-91/2001), 20Y + MAX range buttons on BOTH surfaces
(rng=0 sentinel). Cache v21.

  N1 AAPL v21: quarterly revenue >=140 pts, oldest < 1995; weekly px
     >=1500 pts, oldest < 1985; doc size + elapsed printed
  N2 PG v21 (the 60-year test): revenue oldest < 1992, n printed
  N3 surfaces: MAX button flagship + 0:Max module + core 8 NBER
     entries served + node x3
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3515"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3515_full_history") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:500]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3515 — full history (statements + price + NBER)")
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                            "S3_BUCKET": BUCKET, "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.11.0 full history (ops 3515)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL", "PG"], "periods": ["quarter"],
         "refresh": True}).encode())
    for sym, gname, rev_min, rev_before, px_before in (
            ("AAPL", "N1_aapl", 140, "1995", "1985"),
            ("PG", "N2_pg", 130, "1992", "1985")):
        try:
            o = s3c.get_object(Bucket=BUCKET,
                               Key=f"data/fundgraph/cache/{sym}_quarter_v21.json")
            body = o["Body"].read()
            doc = json.loads(body)
            rev = (doc.get("points") or {}).get("revenue") or []
            px = doc.get("price") or []
            gate(gname,
                 len(rev) >= rev_min and rev[0][0] < rev_before
                 and len(px) >= 1500 and px[0][0] < px_before,
                 {"n_revenue": len(rev),
                  "oldest_revenue": rev[0][0] if rev else None,
                  "n_px_weeks": len(px),
                  "oldest_px": px[0][0] if px else None,
                  "doc_kb": len(body)//1024,
                  "vintage_days": doc.get("vintage_days")})
        except Exception as e:
            gate(gname, False, str(e)[:300])

    got = {}
    for _ in range(15):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3515" in got["core"] and b"MAX</button>" in got["flag"]:
                break
        except Exception:
            pass
        time.sleep(20)
    core = got.get("core", b"")
    nber_n = len(re.findall(rb"\['\d{4}-\d{2}-\d{2}','\d{4}-\d{2}-\d{2}'\]",
                            core[core.index(b"var NBER"):core.index(b"NBER.forEach")]
                            if b"var NBER" in core else b""))
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', got.get("why", b""))
    gate("N3_surfaces",
         nber_n == 8 and b"MAX</button>" in got.get("flag", b"")
         and b"0:Max" in got.get("why", b"")
         and node_ok(core) and node_ok(m1.group(1) if m1 else b"x=")
         and node_ok(m2.group(1) if m2 else b"x="),
         {"nber": nber_n, "flag_max": b"MAX</button>" in got.get("flag", b""),
          "why_max": b"0:Max" in got.get("why", b"")})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3515.json").write_text(json.dumps({"ops":3515,"fails":fails}))
sys.exit(0)
