"""
ops_3905 — PROBE: test signal-backtest's exact FMP batch-quote-short endpoint
with real tickers, to find why n_observations has been 0 despite 64 real,
aged, well-formed snapshots (confirmed in ops 3899). The key itself is very
likely fine (confluence-meta confirmed real FMP_KEY in ops 3887, plus a
shared fallback literal used successfully across dozens of other engines
this session) - testing the actual endpoint/response shape directly rather
than guessing further. Writes no code.
"""
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1")


def get_live_env(fn_name):
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    return (cfg.get("Environment") or {}).get("Variables") or {}


def main():
    with report("3905_fmp_batch_quote_test") as rep:
        rep.heading("ops 3905 — test the exact FMP batch-quote-short endpoint signal-backtest uses")

        rep.section("1. live FMP_KEY on signal-backtest itself")
        env = get_live_env("justhodl-signal-backtest")
        fmp_key = env.get("FMP_KEY", "")
        rep.kv(present=bool(fmp_key), length=len(fmp_key),
               all_env_keys=str(sorted(env.keys())))
        if not fmp_key:
            rep.fail("  no FMP_KEY on signal-backtest at all — real, concrete finding")
            sys.exit(1)

        rep.section("2. the EXACT endpoint + real tickers from the FICO sample seen in ops 3899")
        tickers = ["FICO", "AAPL", "MSFT"]
        url = f"https://financialmodelingprep.com/stable/batch-quote-short?symbols={','.join(tickers)}&apikey={fmp_key}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                data = json.loads(r.read().decode())
            rep.ok(f"  SUCCESS: {json.dumps(data, default=str)[:600]}")
            rep.kv(response_type=type(data).__name__,
                   n_results=len(data) if isinstance(data, list) else None)
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "ignore")
            except Exception:
                body = "(no body)"
            rep.fail(f"  HTTP {e.code} {e.reason} — REAL error body: {body[:600]}")
        except Exception as e:
            rep.fail(f"  non-HTTP failure: {str(e)[:250]}")

        rep.section("3. sanity check — does /stable/quote (singular, older-style) work as a comparison")
        url2 = f"https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey={fmp_key}"
        try:
            req2 = urllib.request.Request(url2, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req2, timeout=15) as r:
                data2 = json.loads(r.read().decode())
            rep.ok(f"  /stable/quote works: {json.dumps(data2, default=str)[:400]}")
        except urllib.error.HTTPError as e:
            try:
                body2 = e.read().decode("utf-8", "ignore")
            except Exception:
                body2 = "(no body)"
            rep.fail(f"  /stable/quote ALSO fails: HTTP {e.code} — {body2[:400]}")
        except Exception as e:
            rep.log(f"  /stable/quote check skipped: {str(e)[:150]}")

        rep.section("4. real CloudWatch invocation history for signal-backtest")
        try:
            logs = boto3.client("logs", region_name="us-east-1")
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-signal-backtest",
                orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
            if streams:
                events = logs.get_log_events(
                    logGroupName="/aws/lambda/justhodl-signal-backtest",
                    logStreamName=streams[0]["logStreamName"], limit=40)["events"]
                tail = "\n".join(e["message"].rstrip() for e in events)
                rep.log(f"  most recent run log tail:\n{tail}")
        except Exception as e:
            rep.fail(f"  CloudWatch read failed: {str(e)[:200]}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
