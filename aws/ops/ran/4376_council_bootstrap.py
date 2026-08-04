"""ops 4376 — council bootstrap + consultation.

The llm_router change queued a fleet-wide shared-lib redeploy (60min+);
justhodl-ai-council sits behind it. The runner holds the same AWS creds,
so: if the function is missing, build the zip here (source + shared
llm_router/llm_cost/_sentry_lite at root), inherit env from
justhodl-bottleneck-research per config, create-function, wait Active —
idempotent; the deploy workflow harmlessly updates it later. Then convene
the council on the v3.1 frontend critique.
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
R = {"ops": 4376, "started": datetime.now(timezone.utc).isoformat()}


def exists():
    try:
        lam.get_function_configuration(FunctionName=FN)
        return True
    except Exception:
        return False


R["pre_existing"] = exists()
if not R["pre_existing"]:
    try:
        cfg = json.load(open("aws/lambdas/justhodl-ai-council/config.json"))
        env = dict(cfg.get("env") or {})
        inh = cfg.get("inherit_env") or {}
        try:
            src_env = (lam.get_function_configuration(
                FunctionName=inh.get("from_function"))
                .get("Environment", {}) or {}).get("Variables", {}) or {}
            for k in inh.get("keys") or []:
                if src_env.get(k):
                    env[k] = src_env[k]
            R["env_inherited"] = sorted(
                k for k in (inh.get("keys") or []) if k in env)
        except Exception as e:
            R["env_inherit_err"] = str(e)[:120]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-ai-council/source/"
                    "lambda_function.py", "lambda_function.py")
            for sh in ("llm_router.py", "llm_cost.py", "_sentry_lite.py"):
                p = "aws/shared/" + sh
                if os.path.exists(p):
                    z.write(p, sh)
        lam.create_function(
            FunctionName=FN, Runtime=cfg["runtime"],
            Role=cfg["role"], Handler=cfg["handler"],
            Code={"ZipFile": buf.getvalue()},
            Timeout=int(cfg.get("timeout", 240)),
            MemorySize=int(cfg.get("memory", 512)),
            Architectures=cfg.get("architectures") or ["x86_64"],
            Description=cfg.get("description", "")[:250],
            Environment={"Variables": env})
        for _ in range(24):
            st = lam.get_function_configuration(
                FunctionName=FN).get("State")
            if st == "Active":
                break
            time.sleep(5)
        R["created"] = True
        R["state"] = st
    except Exception as e:
        R["create_err"] = f"{type(e).__name__}: {str(e)[:200]}"

R["function_ready"] = exists()

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

if R["function_ready"]:
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

ok = R.get("function_ready") and (
    R.get("consultation", {}).get("ok_count") or 0) >= 1
R["verdict"] = (("PASS — council convened, %d providers answered"
                 % R.get("consultation", {}).get("ok_count", 0))
                if ok else "PARTIAL")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4376_council_bootstrap.json", "w"),
          indent=1, default=str)
md = [f"# ops 4376 — council bootstrap + consultation — {R['verdict']}",
      f"- pre_existing={R.get('pre_existing')} created={R.get('created')} "
      f"state={R.get('state')} err={R.get('create_err')}",
      f"- env_inherited: {R.get('env_inherited')}",
      f"- invoke: {json.dumps(R.get('invoke'))[:300]}"]
cons = R.get("consultation") or {}
for p, a in (cons.get("answers") or {}).items():
    md.append(f"\n## {p.upper()} ({a.get('model')}, {a.get('latency_s')}s, "
              f"ok={a.get('ok')})")
    md.append((a.get("answer") or a.get("error") or "")[:2600])
if cons.get("synthesis"):
    md.append("\n## SYNTHESIS (Claude chair)")
    md.append(cons["synthesis"][:2600])
open("aws/ops/reports/4376_council_bootstrap.md", "w").write(
    "\n".join(md) + "\n")
print(json.dumps({k: v for k, v in R.items() if k != "consultation"},
                 indent=1, default=str))
