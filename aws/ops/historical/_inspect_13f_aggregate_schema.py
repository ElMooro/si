"""Quick peek at the 13F aggregate structure to leverage what's already there."""
import json, os, time
import boto3
S3 = boto3.client("s3", region_name="us-east-1")
REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/13f-positions.json")
data = json.loads(obj["Body"].read())

section("aggregate_by_ticker — sample top entry")
agg = data.get("aggregate_by_ticker", {})
log(f"  total tickers: {len(agg)}")
if agg:
    sample_key = list(agg.keys())[0]
    log(f"  sample key: {sample_key}")
    log(f"  sample structure: {json.dumps(agg[sample_key], indent=2)[:1500]}")

section("most_bought")
mb = data.get("most_bought", [])
log(f"  count: {len(mb)}")
if mb:
    log(f"  sample: {json.dumps(mb[0], indent=2)[:600]}")
    log("")
    log(f"  ── top 12 most_bought ──")
    for i, m in enumerate(mb[:12]):
        log(f"    {i+1}. {json.dumps(m)[:250]}")

section("rare_picks (single-fund high-conviction)")
rp = data.get("rare_picks", [])
log(f"  count: {len(rp)}")
if rp:
    log(f"  sample: {json.dumps(rp[0], indent=2)[:500]}")
    log("")
    log(f"  ── top 12 rare_picks ──")
    for i, m in enumerate(rp[:12]):
        log(f"    {i+1}. {json.dumps(m)[:250]}")

section("consensus_holds (broadly held)")
ch = data.get("consensus_holds", [])
log(f"  count: {len(ch)}")
if ch:
    log(f"  sample: {json.dumps(ch[0], indent=2)[:500]}")
    log("")
    log(f"  ── top 12 ──")
    for i, m in enumerate(ch[:12]):
        log(f"    {i+1}. {json.dumps(m)[:250]}")

out = "aws/ops/reports/latest"
os.makedirs(out, exist_ok=True)
with open(os.path.join(out, "inspect_13f_aggregate.md"), "w") as f:
    f.write("\n".join(REPORT))
print("[report written]")
