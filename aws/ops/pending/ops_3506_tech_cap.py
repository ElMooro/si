"""ops 3506 — v1.9.1: tech verdicts cap-exempt (3505-F2 caught the sev1
tech greens being evicted by the fundamentals cap on NVDA's 19-green
doc). Regate NVDA/AAPL.
"""
import json, sys, time
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

with report("3506_tech_cap") as rep:
    out = {"ops": 3506, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:460]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3506 — tech verdicts cap-exempt")
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                            "S3_BUCKET": BUCKET, "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.9.1 tech cap-exempt (ops 3506)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["NVDA", "AAPL"], "periods": ["quarter"],
         "refresh": True}).encode())
    for sym, gname, mn in (("NVDA", "G2_nvda", 3), ("AAPL", "G3_aapl", 4)):
        try:
            doc = json.loads(s3c.get_object(
                Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{sym}_quarter_v19.json")["Body"].read())
            V = doc["verdicts"]
            techs = [x for x in V["greens"] + V["reds"]
                     if x.get("basis") == "tech"]
            gate(gname, len(techs) >= mn
                 and any(x["k"] == "px_vs_200" for x in techs)
                 and any(x["k"] == "ma_regime" for x in techs),
                 {"summary": V["summary"],
                  "tech": [t["why"][:62] for t in techs]})
        except Exception as e:
            gate(gname, False, str(e)[:300])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3506.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
