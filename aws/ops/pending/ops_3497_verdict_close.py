"""ops 3497 — verdict layer close: call-site NameError fixed (prof ->
profile; the ONE variable outside the unit-tested pure function, caught
by the live gates exactly as designed). v1.5.1 deploy + rerun U2/U3/U4.

  W2 AAPL live v15: verdicts present, zero null-val, sector refs exact
     vs served medians, n_green > 0; top verdicts printed
  W3 JPM live: >=12 rules suppressed, zero suppressed-key leakage
  W4 surfaces intact (quick re-affirm)
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
s3c = boto3.client("s3", region_name="us-east-1")
SUPPRESSED_KEYS = {"altman_z", "beneish_m", "sloan_accruals_pct", "roic_pct",
                   "income_quality", "interest_coverage_ttm",
                   "netdebt_to_ebitda_ttm", "current_ratio", "fcf_margin_pct",
                   "gross_margin_pct", "operating_margin_pct", "dso_days"}


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3497"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3497_verdict_close") as rep:
    out = {"ops": 3497, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:460]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:420]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3497 — verdict close (v1.5.1)")

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.5.1 verdict fix (ops 3497)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL", "JPM"], "periods": ["quarter"],
         "refresh": True}).encode())

    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v15.json")["Body"].read())
        V = doc.get("verdicts") or {}
        allv = (V.get("greens") or []) + (V.get("reds") or [])
        sm = json.loads(fetch(
            "https://justhodl.ai/data/fundgraph/sector-medians.json"))
        med = (sm.get("sectors") or {}).get(
            (doc.get("profile") or {}).get("sector") or "", {})
        ref_ok = all(abs((x.get("ref") or 0) - med.get(x["k"], -9e9)) < 1e-6
                     for x in allv if x.get("basis") == "sector")
        gate("W2_aapl_live",
             len(allv) > 0 and all(x.get("val") is not None for x in allv)
             and V.get("summary", {}).get("n_green", 0) > 0 and ref_ok
             and "error" not in V.get("summary", {}),
             {"version": doc.get("version"),
              "n_green": V["summary"].get("n_green"),
              "n_red": V["summary"].get("n_red"),
              "sector_refs_exact": ref_ok,
              "greens": [x["why"] for x in (V.get("greens") or [])[:4]],
              "reds": [x["why"] for x in (V.get("reds") or [])[:3]]})
    except Exception as e:
        gate("W2_aapl_live", False, str(e)[:300])

    try:
        docj = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/JPM_quarter_v15.json")["Body"].read())
        Vj = docj.get("verdicts") or {}
        allkj = {x["k"] for x in
                 (Vj.get("greens") or []) + (Vj.get("reds") or [])}
        gate("W3_jpm_suppression",
             len(Vj.get("summary", {}).get("fin_suppressed") or []) >= 12
             and not (allkj & SUPPRESSED_KEYS),
             {"sector": (docj.get("profile") or {}).get("sector"),
              "suppressed_n": len(Vj.get("summary", {}).get(
                  "fin_suppressed") or []),
              "leaked": sorted(allkj & SUPPRESSED_KEYS),
              "verdicts": [x["why"] for x in
                           (Vj.get("greens") or []) + (Vj.get("reds") or [])][:4]})
    except Exception as e:
        gate("W3_jpm_suppression", False, str(e)[:300])

    try:
        f = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={int(time.time())}")
        y = fetch(f"https://justhodl.ai/why.html?cb={int(time.time())}")
        gate("W4_surfaces", b"fgverd" in f and b"jhfgVerd" in y, {})
    except Exception as e:
        gate("W4_surfaces", False, str(e)[:200])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3497.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
