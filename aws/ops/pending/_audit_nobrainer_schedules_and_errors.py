"""
Audit:
  1. EventBridge schedules for L1-L6 — are they all configured for daily auto-update?
  2. L6 nobrainer-tracker — diagnose the 1 error from last invocation, deep-dump CloudWatch logs.
  3. DDB justhodl-signals — verify L6 actually logged 24 nobrainer signals.
  4. Top 8 nobrainers from S3 — read full data for the production view.
"""
import json, os, time, base64
import boto3
from collections import Counter

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
DDB = boto3.client("dynamodb", region_name=REGION)

LAYERS = [
    "justhodl-theme-detector",
    "justhodl-supply-inflection-scanner",
    "justhodl-theme-tier-classifier",
    "justhodl-asymmetric-hunter",
    "justhodl-nobrainer-rationale",
    "justhodl-nobrainer-tracker",
]

REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("1) EventBridge schedules per layer")
    for fn in LAYERS:
        log("")
        log(f"── {fn}")
        # Find rules that have this lambda as target
        rules = EB.list_rule_names_by_target(TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn}")
        names = rules.get("RuleNames", [])
        if not names:
            log(f"  ⚠ no schedule")
            continue
        for rn in names:
            r = EB.describe_rule(Name=rn)
            log(f"  rule: {rn}  expr={r.get('ScheduleExpression','?')}  state={r.get('State')}")

    section("2) L6 tracker — full CloudWatch logs from last invocation")
    log_grp = "/aws/lambda/justhodl-nobrainer-tracker"
    streams = LOGS.describe_log_streams(logGroupName=log_grp, orderBy="LastEventTime",
                                          descending=True, limit=1)
    if streams.get("logStreams"):
        stream = streams["logStreams"][0]["logStreamName"]
        log(f"  stream: {stream}")
        events = LOGS.get_log_events(logGroupName=log_grp, logStreamName=stream,
                                      limit=300, startFromHead=True)
        msgs = [e["message"] for e in events.get("events", [])]
        for m in msgs[-50:]:
            log(f"    {m.rstrip()}")

    section("3) DDB justhodl-signals — count nobrainer entries")
    try:
        # Scan with FilterExpression on signal_type prefix 'nobrainer'
        # Use EXPRESSION ATTRIBUTE NAMES to handle reserved words
        resp = DDB.scan(
            TableName="justhodl-signals",
            FilterExpression="begins_with(signal_type, :nb)",
            ExpressionAttributeValues={":nb": {"S": "nobrainer"}},
            Select="ALL_ATTRIBUTES",
            Limit=300,
        )
        items = resp.get("Items", [])
        log(f"  found {len(items)} nobrainer signals in DDB")
        # Tally by sub-type
        types = Counter()
        symbols = Counter()
        for it in items:
            t = it.get("signal_type", {}).get("S", "?")
            sym = it.get("symbol", {}).get("S", "?")
            types[t] += 1
            symbols[sym] += 1
        log(f"  signal_types: {dict(types.most_common(10))}")
        log(f"  symbols (top 10): {dict(symbols.most_common(10))}")
        if items:
            sample = items[0]
            log(f"  sample item keys: {list(sample.keys())}")
            for k, v in sample.items():
                vv = list(v.values())[0]
                if isinstance(vv, str) and len(vv) > 100:
                    vv = vv[:100] + "..."
                log(f"    {k}: {vv}")
    except Exception as e:
        log(f"  ❌ DDB scan: {e}")

    section("4) Top 12 TIER_A nobrainers from S3")
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
        data = json.loads(obj["Body"].read())
        log(f"  generated_at: {data.get('generated_at')}")
        log(f"  summary: {json.dumps(data.get('summary', {}))[:500]}")
        ranked = data.get("ranked") or data.get("nobrainers") or []
        log(f"  total ranked: {len(ranked)}")
        log("")
        log("  ── Top 12 by score ──")
        for r in ranked[:12]:
            sym = r.get("symbol", "?")
            theme = r.get("theme", "?")
            score = r.get("nobrainer_score") or r.get("score", "?")
            flag = r.get("flag", "")
            mcr = r.get("mcap_to_rev", "?")
            log(f"    {sym:<8} {theme:<8} score={score:>6} flag={flag:<22} mcap/rev={mcr}")
        log("")
        log("  ── MU-grade subset (mcap_to_rev<=3) ──")
        mu = [r for r in ranked if isinstance(r.get("mcap_to_rev"), (int,float)) and r["mcap_to_rev"] <= 3]
        for r in mu[:8]:
            sym = r.get("symbol", "?")
            theme = r.get("theme", "?")
            score = r.get("nobrainer_score") or r.get("score", "?")
            mcr = r.get("mcap_to_rev", "?")
            log(f"    {sym:<8} {theme:<8} score={score:>6} mcap/rev={mcr}")
    except Exception as e:
        log(f"  ❌ S3 read: {e}")

    section("5) L5 rationale — sample thesis text")
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        theses = data.get("theses") or data.get("rationales") or []
        log(f"  n_theses: {len(theses)}")
        if theses:
            t = theses[0]
            log(f"  ── sample thesis: {t.get('symbol','?')} ──")
            txt = t.get("rationale") or t.get("thesis") or t.get("body") or ""
            for ln in txt.splitlines()[:20]:
                log(f"    {ln}")
    except Exception as e:
        log(f"  ❌ rationale read: {e}")

if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "audit_nobrainer_schedules.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
