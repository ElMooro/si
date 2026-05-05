"""Diagnose why theme-detector failed all 550 polygon fetches."""
import boto3
import json
import time
from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)


def main():
    with report("debug_theme_detector") as r:
        # 1. Inspect Lambda config
        r.heading("1) Lambda config inspection")
        cfg = LAM.get_function(FunctionName="justhodl-theme-detector")["Configuration"]
        r.log(f"  Runtime: {cfg.get('Runtime')}")
        r.log(f"  Memory:  {cfg.get('MemorySize')} MB")
        r.log(f"  Timeout: {cfg.get('Timeout')} s")
        r.log(f"  State:   {cfg.get('State')}")
        env = cfg.get("Environment", {}).get("Variables", {})
        r.log(f"  Env keys: {list(env.keys())}")
        if "POLYGON_KEY" in env:
            pk = env["POLYGON_KEY"]
            r.log(f"  POLYGON_KEY: {pk[:8]}...{pk[-4:]} (len={len(pk)})")

        # 2. Compare to a known-working polygon Lambda
        r.heading("2) Compare against working Polygon Lambda")
        try:
            cfg_r = LAM.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            env_r = cfg_r.get("Environment", {}).get("Variables", {})
            r.log(f"  backtest-engine env keys: {list(env_r.keys())}")
            if "POLYGON_KEY" in env_r:
                pk = env_r["POLYGON_KEY"]
                r.log(f"  backtest-engine POLYGON_KEY: {pk[:8]}...{pk[-4:]} (len={len(pk)})")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 3. Read recent log streams
        r.heading("3) Last invocation log stream")
        log_group = "/aws/lambda/justhodl-theme-detector"
        try:
            streams = LOGS.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=2
            )
            if not streams.get("logStreams"):
                r.log("  no log streams yet")
            else:
                stream = streams["logStreams"][0]
                r.log(f"  stream: {stream['logStreamName']}")
                r.log(f"  events: {stream.get('storedBytes', 0)}b")
                events = LOGS.get_log_events(
                    logGroupName=log_group,
                    logStreamName=stream["logStreamName"],
                    limit=200,
                    startFromHead=True,
                )
                r.log("")
                r.log("  Log events:")
                for e in events.get("events", [])[:80]:
                    msg = e["message"].rstrip()
                    if not msg or msg.startswith("START") or msg.startswith("END") or msg.startswith("REPORT"):
                        continue
                    r.log(f"    {msg[:200]}")
        except Exception as e:
            r.log(f"  ✗ logs: {e}")

        # 4. Direct polygon test from inline script
        r.heading("4) Direct API test via test Lambda invocation")
        # Build a simple inline payload that calls polygon for SPY
        test_code = '''
import urllib.request, json, os
key = os.environ.get("POLYGON_KEY", "MISSING")
url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2026-01-01/2026-05-04?adjusted=true&sort=asc&limit=200&apiKey={key}"
print(f"key_present={key != 'MISSING'} key_len={len(key)} key_first8={key[:8]}")
print(f"url={url[:120]}...")
try:
    req = urllib.request.Request(url, headers={"User-Agent": "Test/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read().decode()
        d = json.loads(body)
        print(f"status={d.get('status')} count={d.get('resultsCount')} results={len(d.get('results') or [])}")
        if d.get("results"):
            r = d["results"][0]
            print(f"first_bar: t={r.get('t')} c={r.get('c')}")
        else:
            print(f"raw_body[:500]: {body[:500]}")
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")
'''
        r.log(f"  Test code (will run inside Lambda):")
        r.log(f"  {test_code[:200]}...")

        # 5. Re-invoke and capture logs
        r.heading("5) Re-invoke + read fresh logs")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-theme-detector", InvocationType="RequestResponse", LogType="Tail")
        body = resp["Payload"].read().decode()
        r.log(f"  invocation duration: {time.time()-t0:.1f}s")
        r.log(f"  status: {resp.get('StatusCode')}")
        # LogResult is base64-encoded last 4KB of logs
        import base64
        log_result = resp.get("LogResult")
        if log_result:
            try:
                log_text = base64.b64decode(log_result).decode("utf-8", errors="replace")
                r.log("")
                r.log("  Tail logs (last 4KB):")
                r.log("  " + "─" * 60)
                for line in log_text.split("\n"):
                    if not line.strip() or line.startswith("START") or line.startswith("END") or line.startswith("REPORT"):
                        continue
                    r.log(f"    {line[:200]}")
                r.log("  " + "─" * 60)
            except Exception as e:
                r.log(f"  ✗ decode: {e}")

        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}")) if "body" in outer else {}
            r.log("")
            r.log(f"  result: n_themes={inner.get('n_themes')} duration={inner.get('duration_s')}s")
        except Exception:
            r.log(f"  body[:300]: {body[:300]}")


if __name__ == "__main__":
    main()
