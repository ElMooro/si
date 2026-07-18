"""ops 3482 — Tier-2a: reverse-DCF implied growth + NBER backdrop + 13F fusion.

Engine v1.3.0 (cache v13): implied_fcf_growth_pct — the Mauboussin
expectations series (2-stage 10y rev-DCF, r=9%, gT=2.5%, bisection;
solver unit: inversion err 0.0000%) + implied_vs_actual_gap_pct
(implied − 3y FCF CAGR); doc.whales_q joined from
data/13f-flows-by-ticker.json (/tmp-memoized, 1h). Catalog 200→202,
implied growth HF-flagged. Core v4: always-on NBER recession backdrop
(micro-asserted). Meta strips on both surfaces show 🐋 latest-quarter
institutional net $.

Gates:
  Z1 CI solver unit (inversion + None-guards)
  Z2 warm v1.3.0: AAPL _v13 implied series ≥30 pts, latest in sane band
     (3–30%), gap present
  Z3 REAL-DATA cross-check: GOOGL whales_q.net_usd ≈ +$11.5B (±30%) —
     the exact figure verified in the 13F arc
  Z4 served-JS integrity 4 surfaces + core OPS3482/NBER + catalog OPS3482
  Z5 flagship v2.0 + module whale chip live; ops3475/flags/marks intact
"""
import importlib.util, json, os, re, subprocess, sys, tempfile, time, urllib.request
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3482"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p], capture_output=True).returncode == 0


with report("3482_impliedg_nber_whales") as rep:
    out = {"ops": 3482, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3482 — implied growth + NBER + whale fusion")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        gg = m.implied_fcf_growth(3.6e12, 110e9)

        def pv(g):
            v, f = 0.0, 110e9
            for y in range(1, 11):
                f *= (1 + g / 100); v += f / 1.09 ** y
            return v + (f * 1.025 / 0.065) / 1.09 ** 10
        err = abs(pv(gg) - 3.6e12) / 3.6e12
        gate("Z1_solver_unit", err < 0.002 and 5 < gg < 25
             and m.implied_fcf_growth(1e12, -5e9) is None,
             {"implied_pct": round(gg, 2), "inversion_err": round(err, 6)})
    except Exception as e:
        gate("Z1_solver_unit", False, str(e)[:240])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.3.0 impliedG+whales (ops 3482)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    wp = json.loads(lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["CHTR", "AAPL", "MSFT", "GOOGL"], "periods": ["quarter"],
         "refresh": True}).encode())["Payload"].read() or b"{}")
    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET, Key="data/fundgraph/cache/AAPL_quarter_v13.json")["Body"].read())
        ig = doc["points"].get("implied_fcf_growth_pct", [])
        gap = doc["points"].get("implied_vs_actual_gap_pct", [])
        lastg = ig[-1][1] if ig else None
        gate("Z2_implied_series",
             wp.get("version") == "1.3.0" and len(ig) >= 30
             and lastg is not None and 3 <= lastg <= 30 and len(gap) >= 10,
             {"pts": len(ig), "latest_implied_pct": lastg,
              "gap_pts": len(gap),
              "latest_gap": gap[-1][1] if gap else None})
    except Exception as e:
        gate("Z2_implied_series", False, str(e)[:240])

    try:
        docg = json.loads(s3c.get_object(
            Bucket=BUCKET, Key="data/fundgraph/cache/GOOGL_quarter_v13.json")["Body"].read())
        wq = docg.get("whales_q") or {}
        net = wq.get("net_usd")
        gate("Z3_whale_crosscheck",
             net is not None and 8e9 <= net <= 15e9,
             {"GOOGL_net_usd": net, "expected": "~+11.5e9",
              "n_funds": wq.get("n_funds")})
    except Exception as e:
        gate("Z3_whale_crosscheck", False, str(e)[:240])

    got = {}
    for _ in range(21):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3482" in got["core"] and b"ops3482" in got["flag"] \
               and b"OPS3482" in got["cat"] and b"ops3482" in got["why"]:
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    gate("Z4_served_js", all(checks) and b"NBER" in got.get("core", b"")
         and b"implied_fcf_growth_pct" in got.get("cat", b""),
         {"node_ok": checks})
    gate("Z5_surfaces", b"ops3482" in got.get("flag", b"")
         and b"whales_q" in got.get("flag", b"")
         and b"ops3482" in got.get("why", b"")
         and b"ops3475" in got.get("why", b"")
         and b"jh_fgwhy_marks" in got.get("why", b""), {})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3482.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
