"""
Audit the existing justhodl-insider-trades pipeline.
Verify:
  1. Lambda configuration + schedule
  2. Current S3 output: data/insider-trades.json shape, freshness, cluster count
  3. Is anything consuming the cluster data? (search HTML pages)
  4. What's missing to turn this into a tradeable signal?
"""
import json, os, time, re
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EB = boto3.client("events", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("1) Lambda config + schedule")
    cfg = L.get_function(FunctionName="justhodl-insider-trades")["Configuration"]
    log(f"  state: {cfg['State']}  mem={cfg['MemorySize']}MB  timeout={cfg['Timeout']}s")
    log(f"  modified: {cfg['LastModified']}")
    log(f"  env: {list(cfg.get('Environment', {}).get('Variables', {}).keys())}")

    rules = EB.list_rule_names_by_target(
        TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:justhodl-insider-trades")
    for rn in rules.get("RuleNames", []):
        r = EB.describe_rule(Name=rn)
        log(f"  schedule: {rn}  expr={r.get('ScheduleExpression')}  state={r.get('State')}")

    section("2) Current S3 output shape")
    try:
        head = S3.head_object(Bucket=BUCKET, Key="data/insider-trades.json")
        log(f"  size: {head['ContentLength']:,}b  modified: {head['LastModified']}")
    except Exception as e:
        log(f"  ❌ {e}")
        return

    obj = S3.get_object(Bucket=BUCKET, Key="data/insider-trades.json")
    data = json.loads(obj["Body"].read())
    log(f"  top-level keys: {sorted(data.keys())}")
    
    stats = data.get("stats", {})
    log(f"  stats: {stats}")
    
    clusters = data.get("clusters", [])
    log(f"  clusters: {len(clusters)}")
    if clusters:
        log("  ── Top 10 clusters by total_value ──")
        sorted_c = sorted(clusters, key=lambda c: c.get("total_value", 0), reverse=True)
        for c in sorted_c[:10]:
            log(f"    {c.get('ticker','?'):<8} {c.get('insider_count','?')}-insiders ${c.get('total_value',0):>14,.0f} {c.get('transactions','?')}-txns  {c.get('company','?')[:40]}")
        log("")
        log(f"  ── Sample cluster fields ──")
        sample = sorted_c[0]
        for k, v in sample.items():
            if isinstance(v, str) and len(v) > 80:
                v = v[:80] + "..."
            elif isinstance(v, list):
                v = f"[{len(v)} items]"
            log(f"    {k}: {v}")
    
    big_buys = data.get("big_buys", [])
    log(f"  big_buys (>$1M single tx): {len(big_buys)}")
    if big_buys:
        log("  ── Top 8 big single buys ──")
        for b in big_buys[:8]:
            log(f"    {b.get('ticker','?'):<8} {b.get('insider','?'):<30}  ${b.get('value',0):>12,.0f}  {b.get('role','?')}")
    
    sector_heat = data.get("sector_heat", [])
    log(f"  sector_heat: {len(sector_heat)} sectors")

    section("3) Is anything consuming this data?")
    consumers = []
    import os as _os
    for fn in _os.listdir("."):
        if fn.endswith(".html"):
            with open(fn, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            if "insider-trades" in content or "insider_trades" in content:
                consumers.append(fn)
    log(f"  HTML pages referencing insider-trades.json: {consumers}")

    # Lambda consumers — search source code
    lambda_consumers = []
    for d in _os.listdir("aws/lambdas"):
        src_path = f"aws/lambdas/{d}/source/lambda_function.py"
        if _os.path.exists(src_path):
            with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            if "insider-trades.json" in content or "insider_trades" in content.lower():
                lambda_consumers.append(d)
    log(f"  Lambdas referencing insider-trades.json: {lambda_consumers}")

    section("4) What's the universe of cluster opportunities?")
    if clusters:
        # Distribution
        by_count = {}
        by_value = {"<100k": 0, "100k-500k": 0, "500k-1M": 0, "1M-5M": 0, "5M+": 0}
        for c in clusters:
            n = c.get("insider_count", 0)
            by_count[n] = by_count.get(n, 0) + 1
            v = c.get("total_value", 0)
            if v < 100_000: by_value["<100k"] += 1
            elif v < 500_000: by_value["100k-500k"] += 1
            elif v < 1_000_000: by_value["500k-1M"] += 1
            elif v < 5_000_000: by_value["1M-5M"] += 1
            else: by_value["5M+"] += 1
        log(f"  cluster size distribution: {by_count}")
        log(f"  cluster value distribution: {by_value}")
        log("")
        # Filter: 3+ insiders AND >$500k total
        strong = [c for c in clusters
                  if c.get("insider_count", 0) >= 3 and c.get("total_value", 0) >= 500_000]
        log(f"  STRONG cluster signals (3+ insiders AND >$500k): {len(strong)}")
        for c in sorted(strong, key=lambda x: x.get("total_value", 0), reverse=True)[:10]:
            insiders = c.get("insiders", [])
            roles = sorted(set(i.get("role", "?")[:20] for i in insiders))
            log(f"    {c.get('ticker','?'):<8} ${c.get('total_value',0):>12,.0f} {c.get('insider_count','?')}-insiders roles={roles}")

if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "audit_insider_pipeline.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
