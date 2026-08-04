"""ops 4374 — AI Council live proof + maiden consultation.

1. Probe SSM for a Perplexity key (any /justhodl/*perplex* name).
2. Poll for justhodl-ai-council existence (Deploy-Lambdas race), then invoke
   it with the first REAL consultation: an institutional frontend critique of
   insiders.html (the page Khalid flagged), providers perplexity+glm+claude,
   synthesized. Perplexity degrades cleanly if the key isn't in SSM yet —
   proving the slot either way.
3. Commit every answer verbatim into the report: this IS the channel —
   Claude asks via ops, the other AIs answer via the auto-committed report.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-ai-council"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 4374, "started": datetime.now(timezone.utc).isoformat()}

# 1 — Perplexity key probe
try:
    names = []
    tok = None
    while True:
        kw = dict(Path="/justhodl/", Recursive=True, MaxResults=10)
        if tok:
            kw["NextToken"] = tok
        r = ssm.get_parameters_by_path(**kw)
        names += [p["Name"] for p in r.get("Parameters", [])]
        tok = r.get("NextToken")
        if not tok:
            break
    hits = [n for n in names if "perplex" in n.lower() or "pplx" in n.lower()]
    R["ssm"] = {"total_params": len(names), "perplexity_keys": hits,
                "expected_name": "/justhodl/perplexity/api-key",
                "key_present": "/justhodl/perplexity/api-key" in names}
except Exception as e:
    R["ssm"] = {"err": str(e)[:120]}

# 2 — wait for the function (deploy race), then convene
deadline = time.time() + 150
exists = False
while time.time() < deadline:
    try:
        lam.get_function_configuration(FunctionName=FN)
        exists = True
        break
    except Exception:
        time.sleep(10)
R["function_deployed"] = exists

QUESTION = """You are reviewing the frontend of a live institutional finance
page: justhodl.ai/insiders.html (SEC Form 4 insider-trading intelligence,
dark theme, part of a 400-page quant platform).

CURRENT STRUCTURE, top to bottom:
1. HERO: title + 4 KPI stat cards (30-day buys, total $, companies, cluster
   count) + a horizontal bar list of top-20 transactions by $ value.
2. Prose explainer paragraphs (why insider buying matters, Cohen-Malloy-
   Pomorski citation).
3. 'Cluster buys' card and 'Big-money single buys' card (often empty-state).
4. 'Sector heat' — value-sorted sector bars.
5. FLEET INSIDER INTELLIGENCE — 5 small cards joining sibling engines
   (radar clusters, industry clusters, sell clusters, market-wide ratio,
   insider-x-buyback confluence), inline-styled.
6. FULL DATA SURFACE — an auto-renderer that walks the entire engine payload
   and prints EVERY section as generic union-column tables: by_ticker (183
   rows: buys/sells/net value/insiders/roles/sector/dates), by_sector,
   by_industry, by_role, by_day daily series, size_distribution,
   top_insiders, sell_by_ticker — plus a coverage HUD (total_leaves,
   ratchet, store totals). All inline styles, no tabs, one long scroll,
   no sorting, no charts.

CONSTRAINTS: vanilla HTML/CSS/JS single file, no build step, no external
libs, dark institutional aesthetic, must keep the auto-renderer property
(new engine fields must display with zero page edits).

CRITIQUE + PRESCRIBE: the 5-8 highest-impact concrete frontend changes
(layout, hierarchy, tables, navigation, data-viz, typography) that would
make this read like a Bloomberg/Koyfin-grade desk page. Be specific enough
to implement directly."""

if exists:
    try:
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({
                             "question": QUESTION,
                             "providers": ["perplexity", "glm", "claude"],
                             "synthesize": True,
                             "tag": "insiders-frontend-critique",
                             "max_tokens": 1600}).encode())
        R["invoke"] = {"code": inv.get("StatusCode"),
                       "fn_err": inv.get("FunctionError"),
                       "summary": inv["Payload"].read().decode()[:500]}
    except Exception as e:
        R["invoke"] = {"err": str(e)[:200]}
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/ai-council.json")
                         ["Body"].read())
        R["consultation"] = doc
    except Exception as e:
        R["consultation_err"] = str(e)[:120]

ok = (R.get("function_deployed")
      and (R.get("consultation", {}).get("ok_count") or 0) >= 1)
R["verdict"] = ("PASS — council convened, "
                f"{R.get('consultation', {}).get('ok_count', 0)} providers "
                "answered" if ok else "PARTIAL — see fields")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4374_ai_council.json", "w"),
          indent=1, default=str)

md = [f"# ops 4374 — AI Council maiden consultation — {R['verdict']}",
      f"- SSM: {json.dumps(R.get('ssm'))}",
      f"- deployed: {R.get('function_deployed')} | invoke: "
      f"{json.dumps(R.get('invoke'))[:300]}"]
cons = R.get("consultation") or {}
for p, a in (cons.get("answers") or {}).items():
    md.append(f"\n## {p.upper()} ({a.get('model')}, {a.get('latency_s')}s, "
              f"ok={a.get('ok')})")
    md.append((a.get("answer") or a.get("error") or "")[:2600])
if cons.get("synthesis"):
    md.append("\n## SYNTHESIS (Claude chair)")
    md.append(cons["synthesis"][:2600])
open("aws/ops/reports/4374_ai_council.md", "w").write("\n".join(md) + "\n")
print(json.dumps({k: v for k, v in R.items() if k != "consultation"},
                 indent=1, default=str))
