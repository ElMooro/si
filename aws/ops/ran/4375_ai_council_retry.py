"""ops 4375 — AI Council consultation (retry after deploy race).
Same maiden question, v3.1-aware. Perplexity degrades cleanly until the
key lands in SSM; glm+claude answer now."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-ai-council"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4375,"started":datetime.now(timezone.utc).isoformat()}

deadline=time.time()+150; exists=False
while time.time()<deadline:
    try:
        lam.get_function_configuration(FunctionName=FN); exists=True; break
    except Exception: time.sleep(10)
R["function_deployed"]=exists

QUESTION='You are reviewing the frontend of a live institutional finance\npage: justhodl.ai/insiders.html (SEC Form 4 insider-trading intelligence,\ndark theme, part of a 400-page quant platform).\n\nCURRENT STRUCTURE, top to bottom:\n1. HERO: title + 4 KPI stat cards + top-20 transaction bars.\n2. Prose explainers (Cohen-Malloy-Pomorski).\n3. Cluster-buys card and $1M+ single-buys card (often empty-state).\n4. Sector heat value-sorted bars.\n5. FLEET INSIDER INTELLIGENCE — 5 cards joining sibling engines.\n6. FULL DATA SURFACE — auto-renderer over the entire engine payload,\n   NOW REBUILT AS: tabbed explorer (Tickers/Sectors/Industries/Daily Flow/\n   Top Insiders/Roles/Size Bands/Selling/More), click-to-sort union-column\n   tables with sticky headers and monospace right-aligned numerics, daily\n   buy-vs-sell bar chart, coverage HUD (contract leaves / ratchet /\n   hydration), net-value +/- coloring. Unknown future payload fields\n   auto-render under More.\n\nCONSTRAINTS: vanilla single-file HTML/CSS/JS, no libs, dark institutional\naesthetic, the auto-render property must survive.\n\nGiven the v3.1 rebuild described, CRITIQUE + PRESCRIBE the 5 highest-impact\nNEXT frontend improvements (what is still missing vs a Bloomberg/Koyfin\ndesk page). Be specific enough to implement directly.'

if exists:
    try:
        inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",
                       Payload=json.dumps({"question":QUESTION,
                           "providers":["perplexity","glm","claude"],
                           "synthesize":True,
                           "tag":"insiders-frontend-critique-v31",
                           "max_tokens":1500}).encode())
        R["invoke"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError"),
                     "summary":inv["Payload"].read().decode()[:400]}
    except Exception as e:
        R["invoke"]={"err":str(e)[:200]}
    try:
        R["consultation"]=json.loads(s3.get_object(Bucket=BUCKET,
                          Key="data/ai-council.json")["Body"].read())
    except Exception as e:
        R["consultation_err"]=str(e)[:120]

ok=R.get("function_deployed") and (R.get("consultation",{}).get("ok_count") or 0)>=1
R["verdict"]=("PASS — council convened, %d providers answered"
              % R.get("consultation",{}).get("ok_count",0)) if ok else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4375_ai_council_retry.json","w"),indent=1,default=str)
md=[f"# ops 4375 — AI Council consultation retry — {R['verdict']}",
    f"- deployed: {R.get('function_deployed')} | invoke: {json.dumps(R.get('invoke'))[:280]}"]
cons=R.get("consultation") or {}
for p,a in (cons.get("answers") or {}).items():
    md.append(f"\n## {p.upper()} ({a.get('model')}, {a.get('latency_s')}s, ok={a.get('ok')})")
    md.append((a.get("answer") or a.get("error") or "")[:2600])
if cons.get("synthesis"):
    md.append("\n## SYNTHESIS (Claude chair)"); md.append(cons["synthesis"][:2600])
open("aws/ops/reports/4375_ai_council_retry.md","w").write("\n".join(md)+"\n")
print(json.dumps({k:v for k,v in R.items() if k!="consultation"},indent=1,default=str))
