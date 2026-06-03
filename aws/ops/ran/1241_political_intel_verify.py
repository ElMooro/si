"""1241 — Re-invoke political-intel (FMP source) + verify real conviction data."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1241_political_intel_verify.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-political-intel"
REGION = "us-east-1"
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
out = {"started": datetime.now(timezone.utc).isoformat()}

# wait for deploy-lambdas to land the new code
time.sleep(60)

print("[1241] invoke political-intel")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": round(time.time()-t0,1),
                      "function_error": resp.get("FunctionError"), "body": payload[:1000]}
    print(f"  status={resp.get('StatusCode')} body={payload[:300]}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

print("[1241] verify output")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/political-intel.json")["Body"].read())
    top = doc.get("top_conviction_buys", [])
    out["output"] = {
        "schema": doc.get("schema_version"), "sources": doc.get("sources"),
        "stats": doc.get("stats"),
        "top15": [{"ticker": r["ticker"], "conviction": r["conviction_score"], "n_buyers": r["n_buyers"],
                    "committee": r["committee_relevant"], "cluster": r["cluster"],
                    "asset": r.get("asset","")[:30], "latest": r.get("latest_tx_date")} for r in top[:15]],
        "committee_buys": [{"ticker": r["ticker"], "matches": r.get("committee_matches",[])[:2]}
                            for r in doc.get("committee_relevant_buys",[])[:8]],
    }
    print(f"  sources={doc.get('sources')} stats={doc.get('stats')}")
    for r in top[:15]:
        fl = []
        if r["committee_relevant"]: fl.append("COMMITTEE")
        if r["cluster"]: fl.append(f"CLUSTER({r['n_buyers']})")
        print(f"    {r['ticker']:<6s} conv={r['conviction_score']:>7.1f} {r['n_buyers']}buyers {' '.join(fl)}")
except Exception as e:
    out["output"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
open(REPORT, "w").write(json.dumps(out, indent=2, default=str))
print("[1241] DONE")
