"""Inspect earnings-tracker pead_signals + momentum-scanner top_50_composite shapes."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_remaining_schemas") as r:
        r.heading("earnings-tracker pead_signals[0] keys")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/earnings-tracker.json")["Body"].read())
        peads = d.get("pead_signals", [])
        r.log(f"  count: {len(peads)}")
        if peads:
            sample = peads[0]
            r.log(f"  keys: {list(sample.keys())}")
            r.log(f"  full sample: {json.dumps(sample, indent=2, default=str)[:1200]}")
        r.log(f"  -- distinct values in 'signal' field --")
        from collections import Counter
        sigs = Counter(p.get("signal") for p in peads)
        for k, n in sigs.most_common():
            r.log(f"    {k}: {n}")

        r.heading("momentum-scanner top_50_composite[0] keys")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")["Body"].read())
        top = d.get("top_50_composite", [])
        r.log(f"  count: {len(top)}")
        if top:
            sample = top[0]
            r.log(f"  keys: {list(sample.keys())}")
            r.log(f"  full sample[0]: {json.dumps(sample, indent=2, default=str)[:800]}")
            r.log(f"  full sample[2]: {json.dumps(top[2] if len(top)>2 else {}, indent=2, default=str)[:600]}")

        r.heading("event-study active themes")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/event-study.json")["Body"].read())
        r.log(f"  top-level keys: {list(d.keys())[:15]}")
        r.log(f"  active_themes: {d.get('active_themes')}")
        r.log(f"  expected_21d_return_from_active_pct: {d.get('expected_21d_return_from_active_pct')}")
        # find any 21d-window expected returns elsewhere
        for k, v in list(d.items())[:15]:
            if "expected" in k.lower() or "return" in k.lower() or "theme" in k.lower():
                r.log(f"    relevant: {k}: {str(v)[:200]}")


if __name__ == "__main__":
    main()
