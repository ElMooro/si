"""ops 3636 — final asia wire: morning-intelligence +china_liq feed (CN TSF
¥20.84tn H1-2026, yoyΔ −2.02tn now reaches the daily brief ctx). Zip-marker
verified, no invoke (LLM-burn doctrine)."""
import io, json, sys, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))

with report("3636_mi_cn") as rep:
    rep.heading("ops 3636 — MI +china_liq (zip-marker)")
    out = {"gates": {}}
    fn = "justhodl-morning-intelligence"
    try:
        cfg = LAM.get_function_configuration(FunctionName=fn)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=ROOT / "lambdas" / fn / "source",
                      env_vars=env, timeout=cfg.get("Timeout", 300),
                      memory=cfg.get("MemorySize", 1024),
                      description=(cfg.get("Description") or fn)[:200],
                      create_function_url=False)
        loc = LAM.get_function(FunctionName=fn)["Code"]["Location"]
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        ok = b"china_liq" in blob and b"china-liquidity.json" in blob
        out["gates"]["G1"] = {"ok": ok, "detail": f"zip china_liq={b'china_liq' in blob}"}
        print(("PASS  " if ok else "FAIL  ") + "G1 — marker " + str(ok)); rep.log("G1 " + str(ok))
        out["verdict"] = "PASS_ALL" if ok else "GAPS: G1"
    except Exception as e:
        out["gates"]["G1"] = {"ok": False, "detail": str(e)[:300]}
        out["verdict"] = "GAPS: G1"
        print("FAIL  G1 —", str(e)[:280])
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3636.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
