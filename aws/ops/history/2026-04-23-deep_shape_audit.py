#!/usr/bin/env python3
"""
Deep dive — examine actual JSON shapes to separate:
  (a) real data-loss bugs (missing values where they should be)
  (b) probe-key mismatches (values under different keys than I guessed)

For each file, dump the structure + first real numeric value for
known fields.
"""
import json
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def tree(obj, depth=0, max_depth=3, max_items=6):
    """Pretty-print the structure of a nested JSON object."""
    if depth >= max_depth:
        return ["(max depth reached)"]
    if isinstance(obj, dict):
        out = []
        items = list(obj.items())
        for i, (k, v) in enumerate(items[:max_items]):
            if isinstance(v, (dict, list)):
                if isinstance(v, list):
                    out.append(f"{'  '*depth}{k}: list[{len(v)}]")
                else:
                    out.append(f"{'  '*depth}{k}: dict[{len(v)}]")
                    out.extend(tree(v, depth+1, max_depth, max_items))
            else:
                preview = str(v)[:80].replace("\n", " ")
                out.append(f"{'  '*depth}{k}: {preview}")
        if len(items) > max_items:
            out.append(f"{'  '*depth}... ({len(items) - max_items} more)")
        return out
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            out = [f"{'  '*depth}[0]: dict[{len(obj[0])}]"]
            out.extend(tree(obj[0], depth+1, max_depth, max_items))
            if len(obj) > 1:
                out.append(f"{'  '*depth}(... {len(obj)-1} more items)")
            return out
        return [f"{'  '*depth}{str(obj)[:100]}"]
    return [f"{'  '*depth}{obj}"]


with report("deep_shape_audit") as r:
    r.heading("Deep shape audit — where do the real numeric values live?")

    # ═════════ 1. FRED — dig into one series' shape ═════════
    r.section("1. FRED — shape of one complete series")
    obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
    rpt = json.loads(obj["Body"].read())

    fred = rpt.get("fred", {})
    r.log(f"  fred top-level categories: {list(fred.keys())}")
    # Pick a series that MUST have fresh data — DGS10 (daily)
    for cat_name, cat_data in fred.items():
        if isinstance(cat_data, dict) and "DGS10" in cat_data:
            r.log(f"\n  DGS10 is in category: {cat_name}")
            dgs10 = cat_data["DGS10"]
            r.log(f"  Full shape:")
            for line in tree(dgs10, max_depth=3, max_items=10):
                r.log(f"    {line}")
            break

    # Now check one more
    for cat_name, cat_data in fred.items():
        if isinstance(cat_data, dict) and "VIXCLS" in cat_data:
            r.log(f"\n  VIXCLS is in category: {cat_name}")
            vix = cat_data["VIXCLS"]
            for line in tree(vix, max_depth=3, max_items=10):
                r.log(f"    {line}")
            break

    # ═════════ 2. flow-data.json — shape ═════════
    r.section("2. flow-data.json — full top-level shape")
    obj = s3.get_object(Bucket=BUCKET, Key="flow-data.json")
    flow = json.loads(obj["Body"].read())
    for line in tree(flow, max_depth=2, max_items=20):
        r.log(f"  {line}")

    # ═════════ 3. crypto-intel.json — shape ═════════
    r.section("3. crypto-intel.json — full top-level shape")
    obj = s3.get_object(Bucket=BUCKET, Key="crypto-intel.json")
    ci = json.loads(obj["Body"].read())
    for line in tree(ci, max_depth=2, max_items=20):
        r.log(f"  {line}")

    # ═════════ 4. stocks — is change_1d really null? ═════════
    r.section("4. Stocks — SPY full shape")
    stocks = rpt.get("stocks") or {}
    spy = stocks.get("SPY")
    if spy:
        for line in tree(spy, max_depth=3, max_items=15):
            r.log(f"  {line}")

    r.log("Done")
