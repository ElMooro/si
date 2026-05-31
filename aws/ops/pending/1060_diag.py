#!/usr/bin/env python3
"""1060 — diagnose why ARK + patent returned empty data.

Tests:
  1. ARK CSV URLs — fetch each, show first 500 bytes, count rows
  2. PatentsView API — try the query and see what it returns
  3. CloudWatch logs for ARK invoke (to see why ok=False)
"""
import json, os, pathlib, time, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/1060_diag.json"

USER_AGENT = "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)"
ARK_URLS = {
    "ARKK": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
    "ARKQ": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_AUTONOMOUS_TECH_%26_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
}

logs = boto3.client("logs", region_name="us-east-1")


def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {"status": r.status, "size": len(body),
                     "headers": dict(r.headers),
                     "body_preview": body[:600].decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}", "body_preview": str(e)[:300]}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def patentsview_test():
    """Try PatentsView API with different schemas."""
    url = "https://search.patentsview.org/api/v1/patent/"
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc).date() - timedelta(days=365)).isoformat()
    
    # Version 1: As implemented in lambda
    payload_v1 = {
        "q": {
            "_and": [
                {"_gte": {"patent_date": start}},
                {"_lte": {"patent_date": end}},
                {"_or": [{"assignee_organization": "APPLE INC"}]},
            ]
        },
        "f": ["patent_number", "patent_date", "patent_title",
               "assignee_organization", "cpc_at_issue"],
        "s": [{"patent_date": "desc"}],
        "o": {"page": 1, "per_page": 5},
    }
    
    # Version 2: simpler — no cpc_at_issue
    payload_v2 = {
        "q": {"_and": [{"_gte": {"patent_date": start}}, {"_lte": {"patent_date": end}},
                       {"assignee_organization": "APPLE INC"}]},
        "f": ["patent_number", "patent_date", "patent_title"],
        "s": [{"patent_date": "desc"}],
        "o": {"page": 1, "per_page": 5},
    }
    
    # Version 3: alternative field name (PatentsView newer schema uses different names)
    payload_v3 = {
        "q": {"_text_phrase": {"assignee_organization": "Apple Inc"}},
        "f": ["patent_number", "patent_date", "patent_title"],
        "o": {"page": 1, "per_page": 5},
    }
    
    results = {}
    for label, payload in [("v1_full", payload_v1), ("v2_simple", payload_v2), ("v3_text_phrase", payload_v3)]:
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, method="POST",
                                            headers={"User-Agent": USER_AGENT,
                                                       "Content-Type": "application/json",
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=25) as r:
                body = r.read().decode("utf-8", errors="replace")
                results[label] = {
                    "status": r.status,
                    "size": len(body),
                    "preview": body[:600],
                }
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")
            except Exception:
                err_body = ""
            results[label] = {"err": f"HTTP {e.code}", "body": err_body[:400]}
        except Exception as e:
            results[label] = {"err": f"{type(e).__name__}: {str(e)[:200]}"}
    return results


def cloudwatch_recent_log(log_group, minutes=10):
    """Get recent log events for a Lambda."""
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = end_time - (minutes * 60 * 1000)
    
    try:
        # Find latest log streams
        streams = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=3,
        )
        if not streams.get("logStreams"):
            return {"err": "no streams"}
        
        # Get events from the most recent stream
        events_out = []
        for s in streams["logStreams"][:2]:
            try:
                evt = logs.get_log_events(
                    logGroupName=log_group,
                    logStreamName=s["logStreamName"],
                    startTime=start_time,
                    limit=80,
                )
                for e in evt.get("events", []):
                    msg = e["message"].strip()
                    if msg and not msg.startswith(("START ", "END ", "REPORT ")):
                        events_out.append(msg[:300])
            except Exception:
                pass
        return events_out[-30:]  # last 30 lines
    except logs.exceptions.ResourceNotFoundException:
        return {"err": "log group not found"}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: ARK URL probes
    print("[1060] phase 1: probe ARK CSV URLs…")
    out["ark_url_probes"] = {}
    for fund, url in ARK_URLS.items():
        out["ark_url_probes"][fund] = http_get(url, timeout=20)
        time.sleep(0.5)
    
    # Phase 2: PatentsView API tests
    print("[1060] phase 2: probe PatentsView API…")
    out["patentsview_tests"] = patentsview_test()
    
    # Phase 3: Recent CloudWatch logs
    print("[1060] phase 3: pull recent Lambda logs…")
    out["ark_logs"] = cloudwatch_recent_log("/aws/lambda/justhodl-ark-holdings", minutes=15)
    out["patent_logs"] = cloudwatch_recent_log("/aws/lambda/justhodl-patent-velocity", minutes=15)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1060] DONE → {REPORT}")


if __name__ == "__main__":
    main()
