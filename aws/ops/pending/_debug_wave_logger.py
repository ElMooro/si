"""Verify wave-logger wrote signals + inspect schema bugs."""
import json
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta
from collections import Counter
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")


def main():
    with report("debug_wave_logger") as r:
        r.heading("Step 1 — DDB scan: any wave-signal-logger-v1 signals in last 30 min?")
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        tbl = ddb.Table("justhodl-signals")
        last_key = None
        items = []
        pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 5:
                break
        wave_items = [it for it in items if it.get("source") == "wave-signal-logger-v1"]
        r.log(f"  total recent items in 30 min: {len(items)}")
        r.log(f"  with source=wave-signal-logger-v1: {len(wave_items)}")
        types = Counter(it.get("signal_type", "?") for it in wave_items)
        for t, n in types.most_common():
            r.log(f"    {t:30s} n={n}")
        # First few
        for it in wave_items[:3]:
            r.log(f"    sample: type={it.get('signal_type')} pred={it.get('predicted_direction')} bp={it.get('baseline_price')} against={it.get('measure_against')}")

        r.heading("Step 2 — etf-flows schema (find the str-not-dict bug)")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/etf-flows.json")["Body"].read())
        r.log(f"  top-level keys: {list(d.keys())}")
        if "by_category" in d:
            for cat, etfs in d["by_category"].items():
                r.log(f"  category '{cat}': type={type(etfs).__name__} len={len(etfs) if hasattr(etfs,'__len__') else '?'}")
                if etfs:
                    first = etfs[0] if isinstance(etfs, list) else etfs
                    r.log(f"    first item type: {type(first).__name__}  preview={str(first)[:200]}")
                    if isinstance(first, dict):
                        r.log(f"    first item keys: {list(first.keys())[:10]}")
                break  # one category is enough

        r.heading("Step 3 — yield-curve schema (spreads_bps shape)")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/yield-curve.json")["Body"].read())
        r.log(f"  spreads_bps: {d.get('spreads_bps')}")
        r.log(f"  butterfly_5y_bps: {d.get('butterfly_5y_bps')}")
        r.log(f"  inversion_flags: {d.get('inversion_flags')}")
        r.log(f"  regime: {d.get('regime')}")

        r.heading("Step 4 — momentum-scanner schema")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")["Body"].read())
        r.log(f"  top-level keys: {list(d.keys())}")
        for k in ("top_composite", "top_50", "ranked", "by_composite", "tickers"):
            if k in d:
                v = d[k]
                if isinstance(v, list) and v:
                    r.log(f"  {k}: list of {len(v)}, first item keys: {list(v[0].keys())[:8] if isinstance(v[0], dict) else 'not-dict'}")
                    if isinstance(v[0], dict):
                        r.log(f"    sample: {json.dumps(v[0], default=str)[:300]}")
                    break

        r.heading("Step 5 — earnings-tracker pead_signals shape")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/earnings-tracker.json")["Body"].read())
        peads = d.get("pead_signals", [])
        r.log(f"  pead_signals count: {len(peads)}")
        labels = Counter(p.get("signal") for p in peads if isinstance(p, dict))
        for label, n in labels.most_common():
            r.log(f"    {label}: {n}")

        r.heading("Step 6 — auction-crisis composite_score")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")["Body"].read())
        r.log(f"  composite_score: {d.get('composite_score')}  regime: {d.get('regime')}")

        r.heading("Step 7 — historical-analogs directional_call")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/historical-analogs.json")["Body"].read())
        r.log(f"  directional_call: {d.get('directional_call')}")
        r.log(f"  forward_distribution keys: {list((d.get('forward_distribution') or {}).keys())[:10]}")
        r.log(f"  forward_distribution: {json.dumps(d.get('forward_distribution', {}), default=str)[:300]}")

        r.heading("Step 8 — event-study expected return")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/event-study.json")["Body"].read())
        r.log(f"  expected_21d_return_from_active_pct: {d.get('expected_21d_return_from_active_pct')}")
        r.log(f"  active_themes: {d.get('active_themes')}")


if __name__ == "__main__":
    main()
