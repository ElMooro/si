"""Diagnose: what fields does an open position actually have?"""
import json
import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("debug_portfolio_fields") as r:
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/signal-portfolio-state.json")
            d = json.loads(obj["Body"].read())
            r.heading("Top-level keys")
            for k in sorted(d.keys()):
                v = d[k]
                if isinstance(v, list):
                    r.log(f"  {k}: list of {len(v)} items")
                elif isinstance(v, dict):
                    r.log(f"  {k}: dict with {len(v)} keys")
                else:
                    r.log(f"  {k}: {repr(v)[:80]}")

            r.heading("Sample open position (first one)")
            ops = d.get("open_positions") or []
            if ops:
                first = ops[0]
                r.log(f"  ALL fields:")
                for k, v in sorted(first.items()):
                    r.log(f"    {k}: {repr(v)[:120]}")

            r.heading("All ticker -> dollar_size mappings")
            for p in ops:
                r.log(f"  {p.get('ticker')}: dollars={p.get('position_size_dollars') or p.get('dollar_size') or p.get('size_dollars') or p.get('size')}, "
                      f"shares={p.get('shares')}, "
                      f"entry_price={p.get('entry_price')}, "
                      f"current_price={p.get('current_price')}, "
                      f"current_pnl_pct={p.get('current_pnl_pct')}")

            # Check totals
            r.heading("Aggregated NAV calc")
            total_dollars = 0
            for p in ops:
                shares = float(p.get("shares") or 0)
                entry = float(p.get("entry_price") or 0)
                if shares and entry:
                    total_dollars += shares * entry
            r.log(f"  Total opening exposure (shares × entry): ${total_dollars:,.2f}")
            r.log(f"  d.get('initial_nav') = {d.get('initial_nav')}")
            r.log(f"  d.get('current_nav') = {d.get('current_nav')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
