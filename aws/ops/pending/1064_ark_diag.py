#!/usr/bin/env python3
"""1064 — read ARK Lambda logs + probe arkfunds.io directly."""
import json, os, pathlib, time, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/1064_ark_diag.json"

logs = boto3.client("logs", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Pull recent ARK Lambda logs
    print("[1064] phase 1: ARK Lambda logs (last 20 min)…")
    log_group = "/aws/lambda/justhodl-ark-holdings"
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = end_time - (20 * 60 * 1000)
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=3,
        )
        out["log_events"] = []
        for s in streams.get("logStreams", [])[:2]:
            evt = logs.get_log_events(
                logGroupName=log_group, logStreamName=s["logStreamName"],
                startTime=start_time, limit=100,
            )
            for e in evt.get("events", []):
                msg = e["message"].strip()
                if msg and not msg.startswith(("START ", "END ", "REPORT ", "INIT_START")):
                    out["log_events"].append(msg[:300])
    except Exception as e:
        out["log_err"] = str(e)[:200]
    
    # Probe arkfunds.io directly from ops runner
    print("[1064] phase 2: probe arkfunds.io directly…")
    for url in [
        "https://arkfunds.io/api/v2/etf/holdings?symbol=ARKK",
        "https://arkfunds.io/api/v2/etf/holdings?symbol=ARKK,ARKQ,ARKW,ARKF,ARKG,ARKX",
        "https://arkfunds.io/api/v2/etf/profile?symbol=ARKK",
    ]:
        label = url.split("/")[-1]
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "JustHodl-Test/1.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as r:
                body = r.read()
                preview = body[:600].decode("utf-8", errors="replace")
                out[f"probe_{label}"] = {
                    "status": r.status,
                    "size": len(body),
                    "preview": preview,
                }
                # Try to parse
                try:
                    parsed = json.loads(body)
                    if isinstance(parsed, dict):
                        out[f"probe_{label}"]["top_keys"] = list(parsed.keys())[:10]
                        if "holdings" in parsed:
                            h = parsed["holdings"]
                            out[f"probe_{label}"]["n_holdings"] = len(h) if isinstance(h, list) else None
                            if isinstance(h, list) and h:
                                out[f"probe_{label}"]["sample"] = h[0]
                except Exception:
                    pass
        except urllib.error.HTTPError as e:
            out[f"probe_{label}"] = {"err": f"HTTP {e.code}"}
        except Exception as e:
            out[f"probe_{label}"] = {"err": f"{type(e).__name__}: {str(e)[:120]}"}
        time.sleep(0.5)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1064] DONE → {REPORT}")


if __name__ == "__main__":
    main()
