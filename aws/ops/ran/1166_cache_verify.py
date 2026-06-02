"""1166 — Verify prompt caching: force two refresh calls, check usage telemetry.

Plan:
  Call 1 (refresh=1): expect cache_creation_input_tokens > 4000, cache_read = 0
                       Pays the 2.0x write surcharge once.
  Call 2 (refresh=1) within ~1 hour: expect cache_creation = 0,
                                     cache_read_input_tokens > 4000.
                                     Pays only 0.10x read cost — 90% savings.

We can't read the usage telemetry from the Lambda's HTTP response (it's
internal), so we tail the CloudWatch logs after each invocation. The
"[claude] usage:" log line is what we look for.
"""
import json, time, urllib.request, urllib.error, ssl
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/1166_cache_verify.json"
RESEARCH_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
LOG_GROUP = "/aws/lambda/justhodl-equity-research"
ctx = ssl.create_default_context()

logs = boto3.client("logs", region_name="us-east-1")


def fetch_recent_usage_lines(since_ms: int, limit: int = 30) -> list:
    """Query CloudWatch for the most recent '[claude] usage:' lines."""
    try:
        # Use filter_log_events to grep for our usage marker
        resp = logs.filter_log_events(
            logGroupName=LOG_GROUP,
            startTime=since_ms,
            filterPattern="[claude] usage:",
            limit=limit,
        )
        events = resp.get("events", [])
        return [
            {"ts": e.get("timestamp"), "msg": e.get("message", "").strip()}
            for e in events
        ]
    except Exception as e:
        return [{"error": str(e)[:300]}]


def force_refresh_call(ticker: str, call_label: str) -> dict:
    """Force a refresh on the equity-research Lambda. Returns timing + status."""
    url = f"{RESEARCH_URL}?ticker={ticker}&refresh=1"
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-CacheVerify/1.0"})
        with urllib.request.urlopen(req, timeout=180, context=ctx) as r:
            body = r.read()
            elapsed = round(time.time() - t0, 2)
            return {
                "call": call_label, "ticker": ticker,
                "http": r.status, "elapsed_s": elapsed,
                "size_kb": round(len(body) / 1024, 1),
            }
    except Exception as e:
        return {"call": call_label, "ticker": ticker, "error": str(e)[:300],
                "elapsed_s": round(time.time() - t0, 2)}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    start_ms = int(time.time() * 1000) - 60_000  # look back 60s

    # ── Call 1: cache write
    print("[1166] Call 1 (refresh=1, expect cache write)...")
    out["call_1"] = force_refresh_call("AAPL", "call_1_cache_write")
    print(f"  done in {out['call_1'].get('elapsed_s')}s")

    # Brief pause for CloudWatch lag, then fetch logs from call 1
    time.sleep(8)
    out["call_1_logs"] = fetch_recent_usage_lines(start_ms, limit=10)
    print(f"  Found {len(out['call_1_logs'])} usage line(s) after call 1")
    for line in out["call_1_logs"]:
        print(f"    {line.get('msg', line.get('error',''))[:180]}")

    # ── Wait 10s, then call 2: cache read (within 1h TTL)
    print("\n[1166] Call 2 (refresh=1 again, expect cache HIT)...")
    call2_start_ms = int(time.time() * 1000) - 5_000
    out["call_2"] = force_refresh_call("AAPL", "call_2_cache_read")
    print(f"  done in {out['call_2'].get('elapsed_s')}s")

    # Wait for CloudWatch
    time.sleep(8)
    out["call_2_logs"] = fetch_recent_usage_lines(call2_start_ms, limit=10)
    print(f"  Found {len(out['call_2_logs'])} usage line(s) after call 2")
    for line in out["call_2_logs"]:
        print(f"    {line.get('msg', line.get('error',''))[:180]}")

    # ── Parse usage numbers
    import re
    def parse_usage(line: str):
        m = re.search(r"input=(\d+)\s+cache_create=(\d+)\s+cache_read=(\d+)\s+output=(\d+)", line)
        if m:
            return {"input": int(m.group(1)),
                    "cache_create": int(m.group(2)),
                    "cache_read": int(m.group(3)),
                    "output": int(m.group(4))}
        return None

    # Find usage line from each call (most recent in each window)
    c1_usage = None
    for line in out["call_1_logs"]:
        if u := parse_usage(line.get("msg", "")):
            c1_usage = u  # last one wins (most recent)
    c2_usage = None
    for line in out["call_2_logs"]:
        if u := parse_usage(line.get("msg", "")):
            c2_usage = u

    out["call_1_usage_parsed"] = c1_usage
    out["call_2_usage_parsed"] = c2_usage

    # ── Cost math
    # Haiku 4.5 pricing (per million tokens):
    #   input:                    $1.00
    #   cache write 1h (2.0x):    $2.00
    #   cache read (0.10x):       $0.10
    #   output:                   $5.00
    PRICE_INPUT_1M = 1.00
    PRICE_CACHE_WRITE_1H = 2.00
    PRICE_CACHE_READ = 0.10
    PRICE_OUTPUT_1M = 5.00

    def cost_of(u):
        if not u: return None
        return round(
            (u["input"] * PRICE_INPUT_1M
             + u["cache_create"] * PRICE_CACHE_WRITE_1H
             + u["cache_read"] * PRICE_CACHE_READ
             + u["output"] * PRICE_OUTPUT_1M) / 1_000_000, 6)

    out["cost_analysis"] = {
        "call_1_cost_usd": cost_of(c1_usage),
        "call_2_cost_usd": cost_of(c2_usage),
        "savings_pct_call_2_vs_1": (
            round((1 - cost_of(c2_usage) / cost_of(c1_usage)) * 100, 1)
            if c1_usage and c2_usage and cost_of(c1_usage) > 0 else None
        ),
        "monthly_proj_no_cache": round(cost_of(c1_usage) * 52 * 30, 2) if c1_usage else None,
        "monthly_proj_with_cache": round(
            (cost_of(c1_usage) + 51 * cost_of(c2_usage)) * 30, 2
        ) if c1_usage and c2_usage else None,
    }

    out["verdict"] = {
        "cache_works": bool(c1_usage and c2_usage
                              and c1_usage["cache_create"] > 3000
                              and c2_usage["cache_read"] > 3000),
        "expected_pattern_match": (
            c1_usage and c2_usage
            and c1_usage["cache_create"] > 3000 and c1_usage["cache_read"] == 0
            and c2_usage["cache_create"] == 0 and c2_usage["cache_read"] > 3000
        ),
    }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print("\n[1166] DONE")


if __name__ == "__main__":
    main()
