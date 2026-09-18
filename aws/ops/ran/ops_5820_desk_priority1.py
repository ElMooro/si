#!/usr/bin/env python3
"""ops 5820 — desk priority 1: clocks, CISS headline, brief verb, no WALCL change."""
import json
import sys
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"


def main():
    try:
        lam = boto3.client("lambda", region_name="us-east-1")
        s3 = boto3.client("s3", region_name="us-east-1")

        for fn in ("justhodl-eurodollar-plumbing", "justhodl-capital-flow"):
            try:
                r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
                print("invoke", fn, r.get("StatusCode"), r["Payload"].read()[:240])
            except Exception as e:
                print("invoke-skip", fn, type(e).__name__, e)

        raw = s3.get_object(Bucket=BUCKET, Key="data/ciss-stress.json")["Body"].read()
        ciss = json.loads(raw)
        headline = {
            "engine": "ciss-headline",
            "source": "data/ciss-stress.json",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "observation_date": ciss.get("ea_composite_date"),
            "ea_composite": ciss.get("ea_composite"),
            "ea_regime": ciss.get("ea_regime"),
            "n_series": ciss.get("n_series"),
            "quality": ciss.get("quality") or {"status": "fresh", "freshness_basis": "observation"},
            "full_series": "/data/ciss-stress.json",
            "note": "2KB desk headline. Not the 1.4MB series. Calls must read this key, not ciss-stress.",
        }
        body = json.dumps(headline).encode()
        for key in ("data/ciss-headline.json",):
            s3.put_object(Bucket=BUCKET, Key=key, Body=body,
                          ContentType="application/json", CacheControl="public, max-age=120")
        print("ciss-headline", headline.get("ea_composite"), headline.get("ea_regime"), headline.get("observation_date"))

        brief = {}
        for key in ("data/ai-brief.md", "data/ai-brief.json"):
            try:
                b = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                print("brief", key, "bytes", len(b))
                if key.endswith(".md"):
                    tail = b.decode("utf-8", "replace")[-800:]
                    print("brief-tail", tail.replace("\n", " | ")[:500])
                else:
                    brief = json.loads(b)
                    print("brief-json-keys", list(brief)[:20] if isinstance(brief, dict) else type(brief))
            except Exception as e:
                print("brief-miss", key, type(e).__name__, e)

        hist = json.loads(s3.get_object(Bucket=BUCKET, Key="data/decisive-call-history.json")["Body"].read())
        last = (hist.get("snapshots") or [])[-1]
        print("calls-last", last.get("timestamp"), last.get("call_verb"), last.get("brief_chars"))

        ed = json.loads(s3.get_object(Bucket=BUCKET, Key="data/eurodollar-plumbing.json")["Body"].read())
        print("eurodollar-gen", ed.get("generated_at"), "score", ed.get("composite_score") or ed.get("score"))
        return 0
    except Exception as e:
        print("FAIL", type(e).__name__, e)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main() or 0)
