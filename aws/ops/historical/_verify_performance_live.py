"""Final verification of performance.html + ensure pnl-tracker is producing fresh data."""
import json
import time
import urllib.request
import boto3
from ops_report import report

UA = {"User-Agent": "justhodl-audit/1.0"}
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="replace"), None
    except Exception as e:
        return None, "", str(e)


def main():
    with report("verify_performance_live") as r:
        # 1. performance.html live
        r.heading("1) performance.html live on production")
        code, body, err = fetch("https://justhodl.ai/performance.html")
        if err:
            r.log(f"  ✗ {code}: {err}")
        else:
            r.log(f"  ✓ status: {code}, size: {len(body):,}b")
            checks = [
                ("title", '<title>Performance · JustHodl</title>' in body),
                ("nav active", 'class="tab active" href="/performance.html"' in body),
                ("KPI row", 'id="kpi-row"' in body),
                ("NAV chart SVG", 'id="nav-chart"' in body),
                ("positions table", 'id="positions-table"' in body),
                ("source breakdown", 'id="source-breakdown"' in body),
                ("disclaimer", 'HYPOTHETICAL' in body),
                ("loads pnl-daily", 'pnl-daily.json' in body),
                ("loads signal portfolio", 'signal-portfolio-state.json' in body),
                ("auto-refresh", 'setInterval(load' in body),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")

        # 2. Performance tab on other key pages
        r.heading("2) Performance tab visible on other pages")
        for p in ["today.html", "brief.html", "accuracy.html", "allocator.html", "intelligence.html"]:
            code, body, err = fetch(f"https://justhodl.ai/{p}")
            has = 'href="/performance.html"' in body or 'href="performance.html"' in body
            r.log(f"  {'✓' if has else '✗'} {p:25s}  Performance link present: {has}")

        # 3. Confirm pnl-tracker output is current
        r.heading("3) Loop 2 P&L tracker — current state")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/pnl-daily.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {d.get('generated_at')}")
            r.log(f"  inception: {d.get('inception')} ({d.get('days_since_inception')} days)")
            r.log(f"  phase: {d.get('current_phase')}")
            r.log(f"  regime: {d.get('current_regime')}")
            r.log(f"  Khalid Strategy: ${d.get('khalid_strategy', {}).get('current_value_usd')} ({d.get('khalid_strategy', {}).get('return_pct')}%)")
            r.log(f"  Buy & Hold:      ${d.get('buy_and_hold', {}).get('current_value_usd')} ({d.get('buy_and_hold', {}).get('return_pct')}%)")
            r.log(f"  System Alpha: {d.get('system_alpha')}%  (delta: {d.get('delta_pct')}%)")
            r.log(f"  Action: {d.get('current_action_required')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4. Confirm signal portfolio
        r.heading("4) Loop 2c Signal Portfolio — current state")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/signal-portfolio-state.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {d.get('generated_at')}  first_seen: {d.get('first_seen')}")
            r.log(f"  initial_nav:  ${d.get('initial_nav')}")
            r.log(f"  current_nav:  ${d.get('current_nav')}  ({d.get('current_nav_pct_chg')}%)")
            r.log(f"  unrealized:   ${d.get('unrealized_pnl_dollars')}")
            r.log(f"  open: {len(d.get('open_positions', []))}  closed: {len(d.get('all_closed_positions', []))}")

            # Top 5 open positions
            r.log("")
            r.log("  Top 5 open positions:")
            for p in (d.get("open_positions") or [])[:5]:
                r.log(f"    {p.get('ticker'):6s}  {p.get('source'):20s}  {p.get('direction'):4s}  entry=${p.get('entry_price')}  stop=${p.get('stop_price')}  target=${p.get('target_price')}  qty={p.get('qty')}")

            # Source breakdown
            from collections import Counter
            srcs = Counter(p.get("source") or "?" for p in (d.get("open_positions") or []))
            r.log("")
            r.log("  Source breakdown:")
            for s, n in srcs.most_common():
                r.log(f"    {s:25s}  n={n}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
