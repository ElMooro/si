"""Try unified snapshot on individual option tickers."""
import json
import urllib.request
from ops_report import report

POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"


def main():
    with report("debug_polygon_options2") as r:
        # Step 1: get a few SPY contracts from /reference
        r.heading("List SPY contracts (next monthly)")
        url = f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=SPY&expiration_date.gte=2026-05-15&expiration_date.lte=2026-06-30&limit=50&apiKey={POLYGON_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "test/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            d = json.loads(resp.read())
        r.log(f"  status: {d.get('status')}  len: {len(d.get('results', []))}")
        sample_tickers = []
        for c in d.get("results", [])[:50]:
            sample_tickers.append(c["ticker"])
        r.log(f"  first 5: {sample_tickers[:5]}")

        # Step 2: try unified snapshot on a single option ticker
        r.heading("Unified snapshot on first option ticker — does it return IV/greeks?")
        if sample_tickers:
            tk = sample_tickers[0]
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={tk}&apiKey={POLYGON_KEY}"
            r.log(f"  url: {url[:140]}")
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "test/1.0"})
                with urllib.request.urlopen(req, timeout=20) as resp:
                    body = resp.read().decode()
                d = json.loads(body)
                r.log(f"  status: {d.get('status')}")
                results = d.get("results", [])
                if results:
                    r.log(f"  first result keys: {list(results[0].keys())}")
                    r.log(f"  full: {json.dumps(results[0], default=str, indent=2)[:1500]}")
            except Exception as e:
                r.log(f"  ✗ {e}")

        # Step 3: try unified snapshot for many options via comma list
        r.heading("Unified snapshot batch (10 tickers)")
        if sample_tickers:
            joined = ",".join(sample_tickers[:10])
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={joined}&apiKey={POLYGON_KEY}"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "test/1.0"})
                with urllib.request.urlopen(req, timeout=20) as resp:
                    body = resp.read().decode()
                d = json.loads(body)
                r.log(f"  results len: {len(d.get('results', []))}")
                for it in d.get("results", [])[:3]:
                    r.log(f"  ticker={it.get('ticker')}  type={it.get('type')}  keys={list(it.keys())[:10]}")
            except Exception as e:
                r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
