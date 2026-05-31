#!/usr/bin/env python3
"""1044 — find which 5 tickers failed Wikipedia lookup, write list to report."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1044_ticker_trends_404s.json"
REGION = "us-east-1"

logs = boto3.client("logs", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    lg = "/aws/lambda/justhodl-ticker-trends"
    try:
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        
        all_404s = []
        all_errs = []
        all_universe = []
        
        for stream in streams[:2]:
            ev = logs.get_log_events(
                logGroupName=lg, logStreamName=stream["logStreamName"],
                limit=500, startFromHead=True,
            )
            for e in ev.get("events") or []:
                msg = e.get("message", "")
                # Match HTTP 404 lines: [ticker-trends-v2] wiki[<article>] HTTP 404 from <url>
                if "wiki[" in msg and "HTTP 404" in msg:
                    # Extract article name
                    import re
                    m = re.search(r"wiki\[([^\]]+)\]", msg)
                    if m:
                        all_404s.append({"article": m.group(1),
                                         "ts": datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat()[:19],
                                         "stream": stream["logStreamName"][-30:]})
                elif "no_source_data" in msg or "err on" in msg:
                    all_errs.append({"msg": msg[:250],
                                     "ts": datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat()[:19]})
                elif "universe:" in msg.lower():
                    all_universe.append(msg.strip())
        
        # Dedupe 404s by article name
        seen = set()
        out["unique_404_articles"] = []
        for r in all_404s:
            if r["article"] not in seen:
                seen.add(r["article"])
                out["unique_404_articles"].append(r)
        
        out["all_404_events_count"] = len(all_404s)
        out["err_lines_sample"] = all_errs[-10:]
        out["universe_log_lines"] = all_universe[-3:]
    except Exception as e:
        out["err"] = str(e)[:300]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
