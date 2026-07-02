"""ops 2705 — PROVIDER-UTILIZATION AUDIT close-out: fuse the idle FinViz Elite fields.

Audit finding (repo-wide extraction): 45 of 118 paid-for Elite fields were fetched
daily for 11,397 names and consumed by NOTHING. This op deploys finviz-signals
v2.1 which fuses the highest-alpha eight — inst_trans_pct (3-mo institutional
flow), insider_trans_pct (6-mo insider flow), eps/sales_growth_qoq (fundamental
momentum, zero FMP quota), perf_y (12-1 momentum), off_50d_high_pct (base
tightness), target_price+analyst_recom (consensus gap) — into coil scoring,
sweep reasons, breakout quality flags, three new quality boards, and harvester
top_picks. Hard-asserts FIELD COVERAGE in the live universe (if a column is
structurally empty, this fails loudly instead of shipping dead features).
Report: aws/ops/reports/2705_finviz_field_fusion.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2705, "ts": datetime.now(timezone.utc).isoformat()}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

sect("1/4 FIELD COVERAGE IN LIVE UNIVERSE (are the idle columns actually populated?)")
uni = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-universe.json")["Body"].read()).get("by_ticker", {})
FIELDS = ("insider_trans_pct", "inst_trans_pct", "eps_growth_qoq", "sales_growth_qoq",
          "perf_y", "off_50d_high_pct", "target_price", "analyst_recom")
cov = {f: sum(1 for u in uni.values() if u.get(f) is not None) for f in FIELDS}
R["field_coverage"] = {"universe_n": len(uni), **cov}
print("  n=%d" % len(uni))
for f, n in cov.items():
    print("  %-20s %5d non-null" % (f, n))
for f in FIELDS:
    assert cov[f] >= 2500, "field %s structurally empty (%d) — fusion would be dead code" % (f, cov[f])

sect("2/4 DEPLOY v2.1")
print("  settling 30s for parallel deploy-lambdas…"); time.sleep(30)
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"):
                z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return
        time.sleep(5)
for _try in range(6):
    try:
        wait_ok("justhodl-finviz-signals")
        lam.update_function_code(FunctionName="justhodl-finviz-signals", ZipFile=zip_fn("justhodl-finviz-signals"))
        break
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
            time.sleep(18)
        else:
            raise
wait_ok("justhodl-finviz-signals")
print("  engine synced")

sect("3/4 RUN + PROVE (async + poll — 46 screens ~3-4 min)")
lm0 = s3.head_object(Bucket=BUCKET, Key="data/finviz-signals.json")["LastModified"]
lam.invoke(FunctionName="justhodl-finviz-signals", InvocationType="Event")
doc = None
t0 = time.time()
while time.time() - t0 < 420:
    time.sleep(20)
    lm = s3.head_object(Bucket=BUCKET, Key="data/finviz-signals.json")["LastModified"]
    if lm > lm0:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-signals.json")["Body"].read())
        print("  landed after %.0fs" % (time.time() - t0))
        break
assert doc, "engine did not land within budget"
q = (doc.get("boards") or {}).get("quality") or {}
cons = (doc.get("boards") or {}).get("consolidation") or {}
coil_tags = {}
for c in cons.get("coiled", []):
    for t in c.get("tags", []):
        coil_tags[t] = coil_tags.get(t, 0) + 1
new_reason_picks = [p for p in doc.get("top_picks", [])
                    if any(k in (p.get("reason") or "") for k in ("accumulating", "12-1", "consensus target"))]
R["after"] = {"quality_momo_n": len(q.get("momo_12_1") or []),
              "quality_gap_n": len(q.get("consensus_gap") or []),
              "smart_accum_n": len(q.get("smart_accum_coils") or []),
              "coil_tag_counts": coil_tags,
              "coiled_row_has_new_fields": all(k in (cons.get("coiled") or [{}])[0]
                                               for k in ("inst_trans_pct", "target_upside_pct")) if cons.get("coiled") else False,
              "new_reason_picks": len(new_reason_picks),
              "picks_total": len(doc.get("top_picks") or []),
              "momo_top3": [(r["ticker"], r["momo_12_1"]) for r in (q.get("momo_12_1") or [])[:3]],
              "gap_top3": [(r["ticker"], r["target_upside_pct"]) for r in (q.get("consensus_gap") or [])[:3]]}
print(json.dumps(R["after"], indent=1)[:900])
assert R["after"]["quality_momo_n"] >= 20, "momo board thin"
assert R["after"]["quality_gap_n"] >= 5, "consensus-gap board thin"
assert R["after"]["coiled_row_has_new_fields"], "coil rows missing fused fields"
assert R["after"]["new_reason_picks"] >= 6, "new pick tiers absent (reserved slots should guarantee representation)"
assert any(k in coil_tags for k in ("INST_ACCUMULATING", "INSIDER_BUYING_6M", "TIGHT_50D", "UPSIDE_25")), "no fusion tags fired"

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2705_finviz_field_fusion.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2705_finviz_field_fusion.json")
print("\nOPS 2705 COMPLETE — paid-for fields now working")
