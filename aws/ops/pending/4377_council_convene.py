"""ops 4377 — hot-update council with hardened router; re-convene.

update_function_code directly (mega-deploy will later converge to the same
committed source). Then the consultation, with the recovery paths live:
Claude 400 -> SSM-truth retry, GLM 429 -> single backoff retry, HTTP error
bodies surfaced. Perplexity still degrades until Khalid's key lands.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-ai-council"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 4377, "started": datetime.now(timezone.utc).isoformat()}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.write("aws/lambdas/justhodl-ai-council/source/lambda_function.py",
            "lambda_function.py")
    for sh in ("llm_router.py", "llm_cost.py", "_sentry_lite.py"):
        p = "aws/shared/" + sh
        if os.path.exists(p):
            z.write(p, sh)
try:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    for _ in range(24):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(5)
    R["code_updated"] = True
except Exception as e:
    R["code_update_err"] = f"{type(e).__name__}: {str(e)[:200]}"

QUESTION = """You are reviewing the frontend of a live institutional finance
page: justhodl.ai/insiders.html (SEC Form 4 insider intelligence, dark
theme, part of a 400-page quant platform).

CURRENT v3.1 STRUCTURE: (1) hero KPI cards + top-20 transaction bars;
(2) prose explainers; (3) cluster-buys and $1M+ single-buys cards;
(4) sector heat bars; (5) fleet cards joining 5 sibling engines;
(6) FULL DATA SURFACE: tabbed explorer (Tickers/Sectors/Industries/Daily
Flow/Top Insiders/Roles/Size Bands/Selling/More) over the entire engine
payload — click-to-sort sticky-header tables, monospace right-aligned
numerics, net +/- coloring, daily buy-vs-sell bar chart, coverage HUD
(contract leaves / ratchet / hydration). Unknown future payload fields
auto-render under More.

CONSTRAINTS: vanilla single-file HTML/CSS/JS, no libs, dark institutional
aesthetic, the zero-edit auto-render property must survive.

CRITIQUE + PRESCRIBE the 5 highest-impact NEXT frontend improvements
(what is still missing vs a Bloomberg/Koyfin desk page). Be specific
enough to implement directly."""

try:
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({
                         "question": QUESTION,
                         "providers": ["perplexity", "glm", "claude"],
                         "synthesize": True,
                         "tag": "insiders-frontend-critique-v31",
                         "max_tokens": 1500}).encode())
    R["invoke"] = {"code": inv.get("StatusCode"),
                   "fn_err": inv.get("FunctionError"),
                   "summary": inv["Payload"].read().decode()[:400]}
except Exception as e:
    R["invoke"] = {"err": str(e)[:200]}
try:
    R["consultation"] = json.loads(
        s3.get_object(Bucket=BUCKET,
                      Key="data/ai-council.json")["Body"].read())
except Exception as e:
    R["consultation_err"] = str(e)[:120]

ok = (R.get("consultation", {}).get("ok_count") or 0) >= 1
R["verdict"] = (("PASS — council convened, %d providers answered"
                 % R.get("consultation", {}).get("ok_count", 0))
                if ok else "PARTIAL")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4377_council_convene.json", "w"),
          indent=1, default=str)
md = [f"# ops 4377 — council convened (hardened) — {R['verdict']}",
      f"- code_updated={R.get('code_updated')} err={R.get('code_update_err')}",
      f"- invoke: {json.dumps(R.get('invoke'))[:300]}"]
cons = R.get("consultation") or {}
for p, a in (cons.get("answers") or {}).items():
    md.append(f"\n## {p.upper()} ({a.get('model')}, {a.get('latency_s')}s, "
              f"ok={a.get('ok')})")
    md.append((a.get("answer") or a.get("error") or "")[:2800])
if cons.get("synthesis"):
    md.append("\n## SYNTHESIS (Claude chair)")
    md.append(cons["synthesis"][:2800])
open("aws/ops/reports/4377_council_convene.md", "w").write(
    "\n".join(md) + "\n")
print(json.dumps({k: v for k, v in R.items() if k != "consultation"},
                 indent=1, default=str))
