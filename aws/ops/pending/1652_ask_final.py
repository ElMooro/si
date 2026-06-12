# ops 1651 — redeploy ask-desk v1.0.1, rerun ranking test + one multi-hop test
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
out = {"ops": 1652}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-ask-desk/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-ask-desk/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-ask-desk", ZipFile=buf.getvalue()); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-ask-desk")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
tests = ["Which underlooked stock ranks #1 and what stance did its research debate reach?",
          "Did the backtest harness indict any live signal type, and does the meta-labeler agree?"]
out["tests"] = []
for q in tests:
    r = lam.invoke(FunctionName="justhodl-ask-desk", InvocationType="RequestResponse",
                    Payload=json.dumps({"test_question": q}).encode())
    body = json.loads(json.loads(r["Payload"].read())["body"])
    out["tests"].append({"q": q, "sources": body.get("sources_used"),
                          "duration_s": body.get("duration_s"),
                          "answer_head": (body.get("answer") or "")[:560],
                          "cited": "[data/" in (body.get("answer") or ""),
                          "names_opra": "OPRA" in (body.get("answer") or "")})
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1652_ask_final.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"opra": out["tests"][0]["names_opra"], "t2_cited": out["tests"][1]["cited"]}))
