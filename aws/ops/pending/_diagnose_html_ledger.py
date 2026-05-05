"""Diagnose HTML deployment + locate decisive-call ledger."""
import boto3
import json
from ops_report import report

REGION = "us-east-1"
S3 = boto3.client("s3", region_name=REGION)


def main():
    with report("diagnose_html_ledger") as r:
        # Where do HTML files actually live?
        r.heading("HTML files in S3 root")
        paginator = S3.get_paginator("list_objects_v2")
        html_files = []
        for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".html"):
                    html_files.append((obj["Key"], obj["Size"], obj["LastModified"]))
        r.log(f"  Total HTML files: {len(html_files)}")
        # Show recent
        html_files.sort(key=lambda x: x[2], reverse=True)
        r.log("  10 most recently modified:")
        for k, s, m in html_files[:10]:
            r.log(f"    {m}  {s:>8}b  {k}")
        # Check specific ones
        r.log("")
        r.log("  Specific page presence checks:")
        for key in ["backtest.html", "calls.html", "horizons.html", "weights.html", "performance.html",
                    "sizing.html", "brief.html", "desk.html", "index.html"]:
            try:
                o = S3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                r.log(f"    ✓ {key:25s}  {o['ContentLength']:>8,}b  mod={o['LastModified']}")
            except Exception:
                r.log(f"    ✗ {key:25s}  NOT IN S3")

        # Locate decisive-call ledger
        r.heading("Decisive-call ledger search")
        candidates = [
            "data/decisive-call-history.json",
            "data/decisive-calls.json",
            "data/decisive-call-ledger.json",
            "data/calls-history.json",
            "decisive-calls.json",
            "ai-brief/calls.json",
        ]
        for k in candidates:
            try:
                o = S3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                r.log(f"    ✓ {k}  {o['ContentLength']:,}b mod={o['LastModified']}")
            except Exception:
                r.log(f"    ✗ {k}")

        # Search for any "decisive" or "call" file
        r.log("")
        r.log("  All keys with 'call' or 'decisive' in them:")
        for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
            for obj in page.get("Contents", []):
                k = obj["Key"].lower()
                if "decisive" in k or "/calls" in k or k.endswith("calls.json") or "call-" in k:
                    r.log(f"    {obj['Key']}  {obj['Size']:,}b  mod={obj['LastModified']}")

        # If decisive-call-history.json exists, dump it
        r.heading("Inspecting decisive-call-history.json directly")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/decisive-call-history.json")
            content = obj["Body"].read()
            r.log(f"  size: {len(content):,}b")
            try:
                d = json.loads(content)
                r.log(f"  type: {type(d).__name__}")
                if isinstance(d, list):
                    r.log(f"  list length: {len(d)}")
                    if d:
                        r.log(f"  first entry keys: {list(d[0].keys())}")
                        r.log(f"  first 200 chars: {json.dumps(d[0])[:200]}")
                elif isinstance(d, dict):
                    r.log(f"  dict keys: {list(d.keys())}")
                    for k, v in d.items():
                        if isinstance(v, list):
                            r.log(f"    {k}: list of {len(v)}")
                        else:
                            r.log(f"    {k}: {type(v).__name__}")
            except Exception as e:
                r.log(f"  parse error: {e}")
                r.log(f"  raw first 500 chars: {content[:500]}")
        except S3.exceptions.NoSuchKey:
            r.log("  ✗ Key does not exist")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Check ai-brief.json — does its snapshot include the call?
        r.heading("ai-brief.json output (checking call_verb field)")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  keys: {list(d.keys())}")
            r.log(f"  generated_at: {d.get('generated_at')}")
            r.log(f"  call_verb: {d.get('call_verb')}")
            r.log(f"  call: {d.get('call')}")
            # any history field?
            for k in ["history", "ledger", "calls", "decisive_call"]:
                if k in d:
                    r.log(f"  {k}: {type(d[k]).__name__}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
