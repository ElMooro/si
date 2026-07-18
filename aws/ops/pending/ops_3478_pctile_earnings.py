"""ops 3478 — Tier-1a: own-history percentile/z + p10-p90 band + earnings layer.

Engine v1.2.0 (cache v12): doc gains `earnings` [[date, epsActual,
epsEstimated]..] from /stable `earnings` (renamed endpoint, fleet-memory
field names). Core v2 (FG_CHART_OPS3478): opts.band shaded p10-p90 zone +
p50 dashline, opts.earnings tick/dot beat-miss layer with native tooltips.
Flagship v1.7 + why-module v3.1: pctile/z chips on every legend entry
("p92 · z+1.6"), band auto-shown for a solo-unit series in Values, Earnings
toggle, Table gains per-cell pXX. jsdom harness v3: 8/8 incl. the
mixed-units Values-override bug it surfaced (module now has MIXOK).

Gates:
  N1 warm v1.2.0; AAPL _v12 cache: earnings >=25 rows, actuals populated,
     both beats and misses present (real-data sanity)
  N2 served-JS integrity (core/catalog/flagship-inline/module-inline node
     --check) + core carries band+earnings markers
  N3 flagship v1.7 live (ops3478, ernbtn, seriesStats)
  N4 why module v3.1 live (fgwhy-3478, jhfgErn, MIXOK) + ops3475 intact
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
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3478"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b, label):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    r = subprocess.run(["node", "--check", p], capture_output=True, text=True)
    return r.returncode == 0, (r.stderr or "")[:140], label


with report("3478_pctile_earnings") as rep:
    out = {"ops": 3478, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:360]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3478 — percentile bands + earnings layer")

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.2.0 earnings layer (ops 3478)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    warm = lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["CHTR", "AAPL", "MSFT"], "periods": ["quarter"],
         "refresh": True}).encode())
    wp = json.loads(warm["Payload"].read() or b"{}")
    try:
        doc = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/fundgraph/cache/AAPL_quarter_v12.json")["Body"].read())
        ern = doc.get("earnings", [])
        acts = [e for e in ern if e[1] is not None and e[2] is not None]
        beats = sum(1 for e in acts if e[1] > e[2])
        misses = sum(1 for e in acts if e[1] < e[2])
        gate("N1_engine_earnings",
             wp.get("version") == "1.2.0" and len(ern) >= 25
             and len(acts) >= 20 and beats >= 3 and misses >= 1,
             {"version": wp.get("version"), "rows": len(ern),
              "with_both": len(acts), "beats": beats, "misses": misses,
              "last": ern[-1] if ern else None})
    except Exception as e:
        gate("N1_engine_earnings", False, str(e)[:240])

    got = {}
    for _ in range(21):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3478" in got["core"] and b"ops3478" in got["flag"] and b"fgwhy-3478" in got["why"]:
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x="), "core"),
              node_ok(got.get("cat", b"x="), "catalog")]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x=", "flagship-inline"))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x=", "module-inline"))
    bad = [(l, e) for ok, e, l in checks if not ok]
    gate("N2_served_js", not bad and b"OPS3478" in got.get("core", b"")
         and b"opts.earnings" in got.get("core", b""),
         {"bad": bad, "core_bytes": len(got.get("core", b""))})
    f = got.get("flag", b"")
    gate("N3_flagship_v17", b"ops3478" in f and b"ernbtn" in f and b"seriesStats" in f, {})
    y = got.get("why", b"")
    gate("N4_why_v31", b"fgwhy-3478" in y and b"jhfgErn" in y and b"MIXOK" in y
         and b"ops3475" in y and b"fgwhy-3477" not in y, {})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3478.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
