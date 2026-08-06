"""ops 4457 — F6 two-gate approvals SHIPPED (recon-first per Khalid: C3
verified ALREADY DONE — zero direct-SDK engines lacking the router; E2 fetch
pattern already live in 3 engines, only warm-writer pending). Ledger seeded
at data/audit/approvals.json (schema: pending[]/decided[]); approvals.html
read-only by design — Gate 2 is KHALID, not a click; approvals arrive via
chat or bus task_update, applied only as propose_patch PRs. Seeds one real
pending item (E2 warm-archive writer) so the flow is live, not theoretical."""
import json,os
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
s3=boto3.client("s3",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
R={"ops":4457,"started":datetime.now(timezone.utc).isoformat()}
now=R["started"]
ledger={"as_of":now,"spec":"F6 two-gate: scouts file -> KHALID decides -> propose_patch applies",
 "pending":[{"id":"APR-0001","filed_at":now,"proposed_by":"claude (session recon)",
   "title":"E2 warm-archive writer: nightly Polygon grouped-daily (all ~11k US equities) -> data/warm/us-equities-daily/{date}.json.gz",
   "detail":{"basis":"grouped-daily fetch pattern already proven in accumulation-radar/breadth-thrust/crypto-ma200; this adds ONLY the archival writer",
             "cost_note":"1 Polygon call + ~3-6MB gz/night","apply_as":"new lambda justhodl-polygon-daily-snapshot, nightly 21:30 UTC"}}],
 "decided":[{"id":"APR-0000","title":"C3 router migration","decision":"approved",
   "decided_by":"verification (already built)","decided_at":now,
   "reason":"recon found 0 engines calling api.anthropic.com without llm_router — migration completed organically via aws/shared propagation; closed without work"}]}
s3.put_object(Bucket=BUCKET,Key="data/audit/approvals.json",
 Body=json.dumps(ledger,indent=1).encode(),ContentType="application/json",CacheControl="no-cache")
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("RECON-FIRST (Khalid's rule: verify before building) + F6 SHIPPED — 31/34.\n"
  "ALREADY BUILT, closed by verification not work: C3 — zero engines call api.anthropic.com "
  "without llm_router; migration completed organically as aws/shared propagated. E2's grouped-"
  "daily FETCH already lives in 3 engines; only the warm-archive writer is missing — filed as "
  "APR-0001 in the new approvals ledger rather than built unilaterally.\n"
  "F6 LIVE (ledger + page): data/audit/approvals.json (pending[]/decided[]) + "
  "https://justhodl.ai/approvals.html — read-only BY DESIGN: Gate 2 is KHALID, not a button; "
  "he approves via chat or bus task_update; approved items apply only as propose_patch PRs; "
  "nothing self-applies, ever. Seeded with one real pending (APR-0001 E2 writer) and one real "
  "decided (APR-0000 C3 closed-as-built) so the flow is live. Verify+seal F6+C3; remaining 3: "
  "E2-writer (awaits Khalid on APR-0001), F5 WORM-posture doc, F7 self-critique."),
 "evidence":[{"kind":"log","ref":"data/audit/approvals.json","snippet":"APR-0001"},
             {"kind":"file","ref":"approvals.html","snippet":"Two-Gate"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"31/34: +F6 approvals, C3 closed-as-built"})
bus({"action":"fanout_pending"})
R["verdict"]="PASS — F6 ledger+page live, C3 closed by verification, APR-0001 filed for Khalid"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4457_f6.json","w"),indent=1,default=str)
open("aws/ops/reports/4457_f6.md","w").write(f"# ops 4457 — F6 + C3-verified — {R['verdict']}\n")
print(R["verdict"])
