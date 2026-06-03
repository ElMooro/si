"""1240 — Deploy political-intel engine + invoke + verify conviction output."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1240_political_intel_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-political-intel"
SOURCE_DIR = "aws/lambdas/justhodl-political-intel/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE = "justhodl-political-intel-daily"
SCHEDULE = "cron(0 11 * * ? *)"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                zf.write(fpath, arcname=os.path.relpath(fpath, SOURCE_DIR))
    return buf.getvalue()


# Create / update
print(f"[1240] 1. Deploy {LAMBDA}")
try:
    zb = build_zip()
    try:
        lam.get_function_configuration(FunctionName=LAMBDA)
        lam.update_function_code(FunctionName=LAMBDA, ZipFile=zb)
        action = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
                            Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                            Description="Congressional + executive trade tracker (Quiver-free)",
                            Timeout=180, MemorySize=512, Architectures=["x86_64"], Publish=False)
        action = "created"
    for _ in range(30):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
            break
    out["deploy"] = {"action": action, "sha": c.get("CodeSha256")[:16], "state": c.get("State")}
    print(f"  ✓ {action} sha={c.get('CodeSha256')[:16]}")
except Exception as e:
    out["deploy_err"] = str(e)[:300]
    print(f"  ❌ {e}")

# Schedule
print(f"\n[1240] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Daily political intel refresh")
    fn = lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EB-{RULE}",
                            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException:
        pass
    out["schedule"] = SCHEDULE
    print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"] = str(e)[:300]

# Invoke
print(f"\n[1240] 3. Invoke (live disclosure fetch + scoring)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                      "function_error": resp.get("FunctionError"), "body": payload[:1200]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  scored_trades={inner.get('scored_trades')} top_conviction={inner.get('top_conviction')} "
                  f"committee_relevant={inner.get('committee_relevant')} clusters={inner.get('clusters')}")
        except: pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Verify output
print(f"\n[1240] 4. Verify political-intel.json")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/political-intel.json")["Body"].read())
    top = doc.get("top_conviction_buys", [])
    out["output"] = {
        "schema": doc.get("schema_version"),
        "sources": doc.get("sources"),
        "stats": doc.get("stats"),
        "n_top_conviction": len(top),
        "top10": [{"ticker": r["ticker"], "conviction": r["conviction_score"],
                    "n_buyers": r["n_buyers"], "committee": r["committee_relevant"],
                    "cluster": r["cluster"]} for r in top[:10]],
        "committee_sample": [{"ticker": r["ticker"], "matches": r.get("committee_matches", [])[:2]}
                              for r in doc.get("committee_relevant_buys", [])[:5]],
    }
    print(f"  ✓ schema {doc.get('schema_version')}, sources {doc.get('sources')}")
    print(f"  ✓ stats: {doc.get('stats')}")
    print(f"\n  TOP CONVICTION BUYS:")
    for r in top[:10]:
        flags = []
        if r["committee_relevant"]: flags.append("COMMITTEE")
        if r["cluster"]: flags.append(f"CLUSTER({r['n_buyers']})")
        print(f"    {r['ticker']:<6s} conviction={r['conviction_score']:>7.1f}  {r['n_buyers']} buyers  {' '.join(flags)}")
    print(f"\n  COMMITTEE-RELEVANT (the edge):")
    for r in doc.get("committee_relevant_buys", [])[:6]:
        cm = r.get("committee_matches", [])
        if cm:
            m = cm[0]
            print(f"    {r['ticker']:<6s} — {m.get('member','?')} on {m.get('committee','?')[:40]} (match: {m.get('match','')})")
except Exception as e:
    out["output"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1240] DONE")
