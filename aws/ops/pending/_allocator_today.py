"""Pull today's allocator output for review."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("allocator_today") as r:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/allocator.json")
        d = json.loads(obj["Body"].read())

        r.heading("Allocator headline")
        r.log(f"  generated_at: {d.get('generated_at')}")
        r.log(f"  regime_headline: {d.get('regime_headline')}")
        r.log(f"  rules: {d.get('n_rules_applied')} of {d.get('n_rules_total')}")
        r.log(f"  cash_buffer_pct: {d.get('cash_buffer_pct')}")

        r.heading("Recommended weights (sorted)")
        weights = d.get("recommended_weights_pct", {}) or {}
        for asset, w in sorted(weights.items(), key=lambda x: -x[1]):
            r.log(f"  {asset:15s} {w:>6.2f}%")

        r.heading("Overweights")
        for ow in d.get("overweights", [])[:10]:
            r.log(f"  {str(ow)[:200]}")

        r.heading("Underweights")
        for uw in d.get("underweights", [])[:10]:
            r.log(f"  {str(uw)[:200]}")

        r.heading("Asset scores (composite)")
        scores = d.get("asset_scores", {}) or {}
        if isinstance(scores, dict):
            for asset, sc in sorted(scores.items(), key=lambda x: -(x[1].get('composite', 0) if isinstance(x[1], dict) else x[1])):
                if isinstance(sc, dict):
                    r.log(f"  {asset:15s} composite={sc.get('composite',0):>+6.2f}  rules={sc.get('n_active_rules',0)}")
                else:
                    r.log(f"  {asset:15s} {sc}")

        r.heading("Rule results")
        rules = d.get("rule_results", []) or []
        r.log(f"  total rules: {len(rules)}")
        for ru in rules[:15]:
            if isinstance(ru, dict):
                r.log(f"  {ru.get('rule','?'):30s} active={ru.get('active','?'):5}  signal={ru.get('signal','?'):20s}  effects={str(ru.get('effects','?'))[:80]}")


if __name__ == "__main__":
    main()
