"""1167 — Pull usage telemetry from CloudWatch with correct filter syntax."""
import json, time, re
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1167_cache_telemetry.json"
LOG_GROUP = "/aws/lambda/justhodl-equity-research"

logs = boto3.client("logs", region_name="us-east-1")

# Filter for the usage log line — use a token CloudWatch accepts (no colons)
# CloudWatch Logs filter pattern syntax: terms with special chars need quotes.
# Quoted filter: "[claude] usage" — but bracket chars are also special.
# Simplest: use just "usage" as the term, then post-filter in Python.

start_ms = int((time.time() - 600) * 1000)  # last 10 min

print(f"[1167] Querying logs since {datetime.fromtimestamp(start_ms/1000, timezone.utc).isoformat()}")

# Get all log streams for this Lambda, look in the most recent ones
streams = logs.describe_log_streams(
    logGroupName=LOG_GROUP,
    orderBy="LastEventTime",
    descending=True,
    limit=5,
)["logStreams"]

print(f"[1167] Found {len(streams)} log streams")

usage_events = []
for s in streams:
    if s.get("lastEventTimestamp", 0) < start_ms:
        continue
    print(f"  Reading stream {s['logStreamName']}...")
    try:
        page = logs.get_log_events(
            logGroupName=LOG_GROUP,
            logStreamName=s["logStreamName"],
            startTime=start_ms,
            limit=500,
        )
        for e in page.get("events", []):
            msg = e.get("message", "")
            if "[claude] usage:" in msg:
                usage_events.append({"ts": e.get("timestamp"), "msg": msg.strip()})
    except Exception as ex:
        print(f"    error: {ex}")

print(f"[1167] Found {len(usage_events)} '[claude] usage' lines")

# Parse them
def parse_usage(line: str):
    m = re.search(r"input=(\d+)\s+cache_create=(\d+)\s+cache_read=(\d+)\s+output=(\d+)", line)
    if m:
        return {"input": int(m.group(1)),
                "cache_create": int(m.group(2)),
                "cache_read": int(m.group(3)),
                "output": int(m.group(4))}
    return None

parsed = []
for e in usage_events:
    u = parse_usage(e["msg"])
    if u:
        u["ts"] = e["ts"]
        u["ts_iso"] = datetime.fromtimestamp(e["ts"] / 1000, timezone.utc).isoformat()
        parsed.append(u)

# Sort by timestamp
parsed.sort(key=lambda x: x["ts"])

# Cost math (Haiku 4.5)
def cost(u):
    return round(
        (u["input"] * 1.00 + u["cache_create"] * 2.00 + u["cache_read"] * 0.10 + u["output"] * 5.00) / 1_000_000,
        6,
    )

for p in parsed:
    p["cost_usd"] = cost(p)

out = {
    "started": datetime.now(timezone.utc).isoformat(),
    "n_usage_events": len(parsed),
    "usage_events": parsed[-10:],  # last 10
}

# Analyze the pattern
if len(parsed) >= 2:
    first_create = next((p for p in parsed if p["cache_create"] > 1000), None)
    first_read = next((p for p in parsed if p["cache_read"] > 1000), None)
    out["analysis"] = {
        "found_cache_write": bool(first_create),
        "found_cache_read":  bool(first_read),
        "first_cache_write": first_create,
        "first_cache_read": first_read,
    }
    if first_create and first_read:
        savings = (cost(first_create) - cost(first_read)) / cost(first_create) * 100
        out["analysis"]["per_call_savings_pct"] = round(savings, 1)
        # Monthly projection at 52/night
        n_nightly = 52
        without_cache_cost = cost(first_create) * n_nightly * 30
        with_cache_cost = (cost(first_create) + cost(first_read) * (n_nightly - 1)) * 30
        out["analysis"]["monthly_no_cache_usd"] = round(without_cache_cost, 2)
        out["analysis"]["monthly_with_cache_usd"] = round(with_cache_cost, 2)
        out["analysis"]["monthly_savings_usd"] = round(without_cache_cost - with_cache_cost, 2)

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"[1167] DONE — {len(parsed)} usage events captured")
