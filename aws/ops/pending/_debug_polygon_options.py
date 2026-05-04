"""Debug Polygon options snapshot — try the actual response shape."""
import json
import urllib.request
from ops_report import report

POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"


def main():
    with report("debug_polygon_options") as r:
        # Try a few endpoints
        endpoints = [
            ("v3 snapshot direct", f"https://api.polygon.io/v3/snapshot/options/SPY?greeks=true&limit=10&apiKey={POLYGON_KEY}"),
            ("v3 reference contracts", f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=SPY&limit=10&apiKey={POLYGON_KEY}"),
            ("v3 unified snapshot", f"https://api.polygon.io/v3/snapshot?ticker.any_of=SPY&apiKey={POLYGON_KEY}"),
        ]
        for name, url in endpoints:
            r.heading(name)
            r.log(f"  url: {url[:120]}")
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "test/1.0"})
                with urllib.request.urlopen(req, timeout=20) as resp:
                    body = resp.read().decode()
                d = json.loads(body)
                r.log(f"  status: {d.get('status')}")
                r.log(f"  keys: {list(d.keys())[:10]}")
                results = d.get("results", [])
                r.log(f"  results len: {len(results) if isinstance(results, list) else 'not_list'}")
                if isinstance(results, list) and results:
                    r.log(f"  first result keys: {list(results[0].keys())[:15]}")
                    r.log(f"  first result: {json.dumps(results[0], default=str)[:600]}")
                else:
                    r.log(f"  raw response (first 500): {body[:500]}")
            except Exception as e:
                r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
