"""ops 3499 — chart controls + verdict color semantics.

Flagship v2.9: every metric legend chip gains 👁 hide (stays selected,
dimmed, off-chart; ?h= persists; saves carry it) and ✕ remove. Verdict
semantics reworked per Khalid: RED = red flags (sev3 FLASHING),
YELLOW ⚠ = warnings (sev1-2), GREEN = greens AND elites (⭐, EXTREME
elites FLASHING green). Engine v1.6.1 adds extreme=true via dual-basis
(norm-elite AND sector-decile) or hard table (ROIC>=60, coverage>=100,
Z>=10, perfect Piotroski, Beneish<=-3.5, net cash>=2x, GM>=85,
FCFm>=40). Harness v8 = 17 behaviors (extreme-green-flash,
sev3-red-flash, yellow-warn all asserted).

  C1 CI extreme suites (hard-table, dual-basis, elite-not-extreme)
  C2 AAPL live: n extreme >= 3 (ROIC 99.7 / Z 10.9 / coverage 120 /
     Piotroski 9 expected), every extreme is elite, printed
  C3 surfaces: data-eye/data-del/jhfl on flagship, jhfgfl/EXTREME on
     module, priors, node x4
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3499"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


def flat(v, n=14):
    return [["2024-%02d-01" % (i % 12 + 1), v] for i in range(n)]


with report("3499_controls_semantics") as rep:
    out = {"ops": 3499, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:460]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:420]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3499 — controls + red/yellow/green + flashing extremes")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        r = m.derive_verdicts({"roic_pct": flat(99.7),
                               "altman_z": flat(10.9)}, 4, "Technology", {}, {})
        e = {x["k"]: x for x in r["greens"]}
        t1 = e["roic_pct"].get("extreme") and e["altman_z"].get("extreme")
        r2 = m.derive_verdicts({"fcf_yield_pct": flat(11)}, 4, "Technology",
                               {"fcf_yield_pct": 4},
                               {"fcf_yield_pct": {"p10": 1, "p90": 9, "n": 60}})
        t2 = [x for x in r2["greens"]][0].get("extreme")
        r3 = m.derive_verdicts({"roic_pct": flat(35)}, 4, "Technology", {}, {})
        t3 = (r3["greens"][0].get("elite")
              and not r3["greens"][0].get("extreme"))
        gate("C1_extreme_suites", all([t1, t2, t3]),
             {"hard_table": bool(t1), "dual_basis": bool(t2),
              "elite_not_extreme": bool(t3)})
    except Exception as e:
        gate("C1_extreme_suites", False, str(e)[:280])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.6.1 extreme flag (ops 3499)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL"], "periods": ["quarter"], "refresh": True}).encode())
    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v16.json")["Body"].read())
        els = [x for x in (doc["verdicts"]["greens"] or []) if x.get("extreme")]
        gate("C2_aapl_extremes",
             len(els) >= 3 and all(x.get("elite") for x in els),
             {"n_extreme": len(els),
              "extremes": [x["why"][:70] for x in els][:5]})
    except Exception as e:
        gate("C2_aapl_extremes", False, str(e)[:280])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            if b"ops3499" in got["flag"] and b"jhfgfl" in got["why"]:
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    f = got.get("flag", b""); y = got.get("why", b"")
    d3 = {"node_ok": all(checks),
          "flag": all(k in f for k in [b"ops3499", b"data-eye", b"data-del",
                                       b"jhfl", b"EXTREME"]),
          "why": b"jhfgfl" in y and b"EXTREME" in y,
          "priors": b"fgverd" in f and b"jhfgVerd" in y and b"ops3475" in y}
    gate("C3_surfaces", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3499.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
