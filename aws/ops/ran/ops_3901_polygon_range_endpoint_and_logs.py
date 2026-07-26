"""
ops_3901 — PROBE: the key works (confirmed, real Polygon /prev call succeeded).
But evaluate_call() doesn't use /prev — it uses the historical range endpoint
/v2/aggs/ticker/{symbol}/range/1/day/{from}/{to} to look up the close price
AT a specific past checkpoint date. Testing THAT exact endpoint, with a real
historical date (day after a real May-2026 call date), using the same
verified-working key. Also pulling real CloudWatch invocation history to see
what trade-evaluator's own print() logs actually say on recent scheduled
runs — direct evidence beats more hypothesis-testing. Writes no code.
"""
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")


def get_live_env(fn_name):
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    return (cfg.get("Environment") or {}).get("Variables") or {}


def main():
    with report("3901_polygon_range_endpoint_and_logs") as rep:
        rep.heading("ops 3901 — the ACTUAL endpoint evaluate_call() uses + real invocation history")

        rep.section("1. get the verified-working key")
        env = get_live_env("justhodl-trade-evaluator")
        key = env.get("POLY_KEY", "")
        if not key:
            rep.fail("  no key on trade-evaluator itself")
            sys.exit(1)
        rep.ok(f"  key present, len={len(key)}")

        rep.section("2. THE EXACT endpoint evaluate_call() uses — historical range for a real past call date")
        # mirror fetch_historical_close() exactly: call_date + 1 day checkpoint,
        # using a real call_date from the trade journal sample (2026-05-13, ABT)
        target = "2026-05-14"  # call_date 2026-05-13 + 1 day checkpoint
        end = (datetime.fromisoformat(target).date() + timedelta(days=10)).isoformat()
        url = (f"https://api.polygon.io/v2/aggs/ticker/ABT/range/1/day/"
               f"{target}/{end}?adjusted=true&sort=asc&limit=20&apiKey={key}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Eval/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            rep.ok(f"  SUCCESS: {json.dumps(data, default=str)[:500]}")
            bars = data.get("results") or []
            rep.kv(n_bars_returned=len(bars),
                   first_close=bars[0].get("c") if bars else None)
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "ignore")
            except Exception:
                body = "(no body)"
            rep.fail(f"  HTTP {e.code} {e.reason} — REAL error body: {body[:500]}")
        except Exception as e:
            rep.fail(f"  non-HTTP failure: {str(e)[:200]}")

        rep.section("3. real CloudWatch invocation history — has this actually been running, "
                    "and what do its own logs say")
        try:
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-trade-evaluator",
                orderBy="LastEventTime", descending=True, limit=5)["logStreams"]
            rep.kv(n_recent_streams=len(streams))
            for st in streams:
                last_ts = st.get("lastEventTimestamp")
                last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc) if last_ts else None
                age_h = round((datetime.now(timezone.utc) - last_dt).total_seconds() / 3600, 1) if last_dt else None
                rep.log(f"  stream {st['logStreamName']}: last event {age_h}h ago")
            if streams:
                events = logs.get_log_events(
                    logGroupName="/aws/lambda/justhodl-trade-evaluator",
                    logStreamName=streams[0]["logStreamName"], limit=50)["events"]
                tail = "\n".join(e["message"].rstrip() for e in events)
                rep.log(f"  MOST RECENT RUN full log tail:\n{tail}")
        except Exception as e:
            rep.fail(f"  CloudWatch read failed: {str(e)[:200]}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
