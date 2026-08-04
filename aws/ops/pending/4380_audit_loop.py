"""ops 4380 — the ongoing audit loop goes live.

Deploys bus v1.1 (evidence-in-view, tolerant parser, continuation
protocol) + justhodl-audit-loop, binds rule justhodl-audit-loop-2h,
seeds the full inventory (engines from manifest, pages from this repo
checkout), registers claude-audit on the bus, runs TWO live shards
immediately, fans out to Perplexity, and renders the first real findings
+ handoff brief + Perplexity's verify/NEXT_ACTIONS turns.
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
LOOP = "justhodl-audit-loop"
BUS = "justhodl-a2a-bus"
RULE = "justhodl-audit-loop-2h"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
R = {"ops": 4380, "started": datetime.now(timezone.utc).isoformat()}


def zip_fn(src_dir, shared):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{src_dir}/source/lambda_function.py",
                "lambda_function.py")
        for sh in shared:
            p = "aws/shared/" + sh
            if os.path.exists(p):
                z.write(p, sh)
    return buf.getvalue()


def ensure(fn, src_dir, shared):
    code = zip_fn(src_dir, shared)
    try:
        lam.get_function_configuration(FunctionName=fn)
        lam.update_function_code(FunctionName=fn, ZipFile=code)
        mode = "updated"
    except lam.exceptions.ResourceNotFoundException:
        cfg = json.load(open(f"aws/lambdas/{src_dir}/config.json"))
        env = dict(cfg.get("env") or {})
        inh = cfg.get("inherit_env") or {}
        if inh:
            try:
                se = (lam.get_function_configuration(
                    FunctionName=inh["from_function"])
                    .get("Environment", {}) or {}).get("Variables", {}) or {}
                for k in inh.get("keys") or []:
                    if se.get(k):
                        env[k] = se[k]
            except Exception:
                pass
        lam.create_function(FunctionName=fn, Runtime=cfg["runtime"],
                            Role=cfg["role"], Handler=cfg["handler"],
                            Code={"ZipFile": code},
                            Timeout=cfg.get("timeout", 240),
                            MemorySize=cfg.get("memory", 512),
                            Description=cfg.get("description", "")[:250],
                            Environment={"Variables": env})
        mode = "created"
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and \
                c.get("LastUpdateStatus") in (None, "Successful"):
            break
        time.sleep(5)
    return mode


R["bus"] = ensure(BUS, "justhodl-a2a-bus",
                  ("llm_router.py", "llm_cost.py", "_sentry_lite.py"))
R["loop"] = ensure(LOOP, "justhodl-audit-loop", ("_sentry_lite.py",))

# schedule
try:
    arn = ev.put_rule(Name=RULE, ScheduleExpression="rate(2 hours)",
                      State="ENABLED",
                      Description="ops4380 perpetual audit loop")["RuleArn"]
    fa = lam.get_function_configuration(FunctionName=LOOP)["FunctionArn"]
    ev.put_targets(Rule=RULE, Targets=[{"Id": LOOP[:60], "Arn": fa}])
    try:
        lam.add_permission(FunctionName=LOOP, StatementId="ops4380-" + RULE,
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=arn)
    except lam.exceptions.ResourceConflictException:
        pass
    R["schedule"] = "bound rate(2 hours)"
except Exception as e:
    R["schedule_err"] = str(e)[:150]

# inventory: engines from manifest (S3) + pages from this checkout
pages = []
DENY = {"aws", ".github", "scripts", "ci", "config", "cloudflare", "ops",
        "docs", "supabase", "tools-src", "chrome-extension",
        "node_modules", "_partials", "_site", ".git"}
for root, dirs, files in os.walk("."):
    dirs[:] = [d for d in dirs if d not in DENY and not d.startswith(".")]
    for f in files:
        if f.endswith(".html"):
            pages.append(os.path.join(root, f)[2:])
pages = sorted(pages)
man = {}
try:
    man = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/engine-manifest.json")["Body"].read())
except Exception as e:
    R["manifest_err"] = str(e)[:100]
rows = man.get("engines") or man.get("functions") or man
if isinstance(rows, dict):
    rows = list(rows.values())
engines = sorted({(r.get("function_name") or r.get("name"))
                  for r in rows if isinstance(r, dict)
                  and (r.get("function_name") or r.get("name"))})
man2 = {}
try:
    man2 = json.loads(s3.get_object(
        Bucket=BUCKET, Key="config/schedule-manifest.json")["Body"].read())
except Exception:
    pass
ent = man2.get("schedules") or man2.get("entries") or man2
if isinstance(ent, dict):
    mfns = sorted(ent.keys())
elif isinstance(ent, list):
    mfns = sorted({e.get("function") or e.get("fn") or e.get("name")
                   for e in ent if isinstance(e, dict)} - {None})
else:
    mfns = []
inventory = {"updated": datetime.now(timezone.utc).isoformat(),
             "engines": engines, "pages": pages,
             "manifest_fns": mfns, "_persisted": True}
s3.put_object(Bucket=BUCKET, Key="data/audit/inventory.json",
              Body=json.dumps(inventory).encode(),
              ContentType="application/json")
R["inventory"] = {"engines": len(engines), "pages": len(pages),
                  "manifest_fns": len(mfns)}

# register claude-audit on the bus
try:
    reg = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/a2a/registry.json")["Body"].read())
    reg["providers"]["claude-audit"] = {
        "kind": "agent", "transport": "lambda",
        "capabilities": ["mechanical_audit", "evidence"],
        "status": "healthy",
        "note": "justhodl-audit-loop files findings; Claude fixes; "
                "Perplexity verifies"}
    reg["updated"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key="data/a2a/registry.json",
                  Body=json.dumps(reg).encode(),
                  ContentType="application/json")
    R["registry"] = "claude-audit registered"
except Exception as e:
    R["registry_err"] = str(e)[:100]


def call(fn, payload):
    inv = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


R["shard_runs"] = []
for i in range(2):
    try:
        r = call(LOOP, {})
        R["shard_runs"].append({k: r.get(k) for k in
                                ("shard", "new_findings", "open_total",
                                 "critical", "filed_to_bus", "telegram")})
    except Exception as e:
        R["shard_runs"].append({"err": str(e)[:200]})
    time.sleep(8)

call(BUS, {"action": "fanout_pending"})
time.sleep(5)

def sget(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception as e:
        return {"err": str(e)[:80]}

R["handoff"] = sget("data/audit/handoff.json")
fdoc = sget("data/audit/findings.json")
fmap = fdoc.get("findings") or {}
R["findings_total"] = len(fmap)
R["thread"] = call(BUS, {"action": "get_thread",
                         "thread_id": "audit-loop-main"}).get("thread")

runs_ok = sum(1 for r in R["shard_runs"] if r.get("shard"))
ok = (R["loop"] in ("created", "updated") and "bound" in
      str(R.get("schedule", "")) and runs_ok >= 1
      and isinstance(R["handoff"].get("coverage"), dict))
R["verdict"] = ("PASS — loop live on rate(2 hours), "
                f"{R['findings_total']} findings banked, handoff active"
                if ok else "PARTIAL — see fields")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4380_audit_loop.json", "w"),
          indent=1, default=str)

md = [f"# ops 4380 — perpetual audit loop — {R['verdict']}",
      f"- bus={R['bus']} loop={R['loop']} schedule={R.get('schedule')}",
      f"- inventory: {json.dumps(R['inventory'])}",
      f"- shard runs: {json.dumps(R['shard_runs'])[:700]}",
      f"- handoff coverage: {json.dumps(R['handoff'].get('coverage'))} | "
      f"open={R['handoff'].get('open_findings')} "
      f"crit={R['handoff'].get('critical')}",
      "\n## TOP OPEN FINDINGS"]
for f in (R["handoff"].get("top_open") or [])[:8]:
    md.append(f"- [{f['severity']}] {f['layer']}:{f['target']} — "
              f"{f['check']}: {f['detail'][:140]}")
md.append("\n## AUDIT THREAD (audit-loop-main)")
for x in (R.get("thread") or {}).get("turns", [])[-6:]:
    md.append(f"\n### {x['from']} -> {x['to']} [{x['kind']}"
              f"{'/' + x['verdict'] if x.get('verdict') else ''}] {x['ts']}")
    md.append((x.get("content") or "")[:1500])
    if x.get("evidence"):
        md.append("evidence: " + json.dumps(
            [{k: e.get(k) for k in ('kind', 'ref', 'resolved')}
             for e in x["evidence"]]))
open("aws/ops/reports/4380_audit_loop.md", "w").write("\n".join(md) + "\n")
print(json.dumps({k: v for k, v in R.items()
                  if k not in ("thread", "handoff")},
                 indent=1, default=str)[:3000])
