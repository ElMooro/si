"""ops 3510 — factor-DNA v2 regate: the radar's percentile universe is
now OUR 503-name forensic cross-section (3509 probe showed master-
ranker top_tickers is a 25-name conviction list, not a factor panel);
the ranker joins as a conviction overlay chip (#rank · score · systems)
when the ticker is in the top set. Lower-better axes (Beneish, Sloan,
P/E, PEG) are goodness-flipped so the polygon always reads
bigger=better. Harness v13 -> 37 behaviors incl the conviction chip.

  J1 CI battery: seeded 500-row forensic fixture -> goodness-flip
     EXACT (beneish 97.9 vs hand-computed), conviction rank#2 joined,
     absent-ticker dormancy carries conviction=None
  J2 NVDA + AAPL live v20 (refresh): state ok, n_universe>=400,
     4-7 axes all in [0,100], chosen axis keys printed (self-
     documenting schema), conviction join status printed
  J3 surfaces: conviction marker both pages + node
"""
import importlib.util, json, os, random, re, subprocess, sys, tempfile, time, urllib.request
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3510"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3510_radar_v2") as rep:
    out = {"ops": 3510, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:520]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:480]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3510 — radar on the forensic universe + conviction")
    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        random.seed(3)
        rows = [{"ticker": "T%03d" % i, "piotroski": random.randint(0, 9),
                 "altman_z": random.random() * 8,
                 "beneish_m": -3 + random.random() * 3,
                 "sloan_accruals_pct": random.random() * 20,
                 "fcf_yield_pct": random.random() * 12,
                 "sector": "x"} for i in range(500)]
        rows[7] = {"ticker": "NVDA", "piotroski": 9, "altman_z": 7.9,
                   "beneish_m": -2.95, "sloan_accruals_pct": 0.5,
                   "fcf_yield_pct": 11.5, "sector": "x"}
        m._FOREN["rows"] = rows; m._FOREN["ts"] = 1e12
        m._RANKER["doc"] = [
            {"ticker": "OXY", "score": 143.6, "n_systems": 4,
             "systems": ["a"], "rationale": "r"},
            {"ticker": "NVDA", "score": 120.2, "n_systems": 3,
             "systems": ["compound", "pead", "smart_money"],
             "rationale": "3 systems agree"}]
        m._RANKER["ts"] = 1e12
        f = m.factor_dna("NVDA")
        ax = {a["k"]: a for a in f["axes"]}
        col = sorted(r["beneish_m"] for r in rows)
        below = sum(1 for v in col if v < -2.95)
        exp = round(100 - 100 * (below + 0.5) / 500, 1)
        f2 = m.factor_dna("ZZZZ")
        gate("J1_battery",
             f["state"] == "ok" and abs(ax["beneish_m"]["pct"] - exp) < 0.11
             and ax["sloan_accruals_pct"]["pct"] > 95
             and f["conviction"]["rank"] == 2
             and f2["state"] == "insufficient"
             and f2["conviction"] is None,
             {"beneish_pct": ax["beneish_m"]["pct"], "expected": exp,
              "conviction": f["conviction"]})
    except Exception as e:
        gate("J1_battery", False, str(e)[:320])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                            "S3_BUCKET": BUCKET, "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.10.1 radar v2 (ops 3510)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["NVDA", "AAPL"], "periods": ["quarter"],
         "refresh": True}).encode())
    for sym, gn in (("NVDA", "J2_nvda"), ("AAPL", "J2b_aapl")):
        try:
            doc = json.loads(s3c.get_object(
                Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{sym}_quarter_v20.json")["Body"].read())
            fd = doc.get("factor_dna") or {}
            axes = fd.get("axes") or []
            gate(gn, fd.get("state") == "ok"
                 and (fd.get("n_universe") or 0) >= 400
                 and 4 <= len(axes) <= 7
                 and all(0 <= (a.get("pct") or -1) <= 100 for a in axes),
                 {"n_universe": fd.get("n_universe"),
                  "axes": [(a["k"], a["pct"]) for a in axes],
                  "conviction": fd.get("conviction"),
                  "why": fd.get("why")})
        except Exception as e:
            gate(gn, False, str(e)[:300])

    got = {}
    for _ in range(15):
        try:
            cb = int(time.time())
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"master-ranker #" in got["flag"] and b"master-ranker #" in got["why"]:
                break
        except Exception as e:
            got["err"] = str(e)[:100]
        time.sleep(20)
    f9 = got.get("flag", b""); y9 = got.get("why", b"")
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", f9)
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', y9)
    gate("J3_surfaces",
         b"master-ranker #" in f9 and b"master-ranker #" in y9
         and node_ok(m1.group(1) if m1 else b"x=")
         and node_ok(m2.group(1) if m2 else b"x="),
         {"flag": b"master-ranker #" in f9, "why": b"master-ranker #" in y9})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3510.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
