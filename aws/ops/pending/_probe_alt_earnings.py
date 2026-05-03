"""Probe alternative earnings sources: Polygon, Yahoo, Nasdaq, Finnhub free."""
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from ops_report import report

POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"


def get(url, timeout=10, headers=None):
    headers = headers or {"User-Agent": "JustHodl Research"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(), resp.status


def main():
    with report("probe_alt_earnings") as r:
        r.heading("Alternative earnings sources")

        # 1. Polygon: ticker events (includes earnings)
        r.section("1. Polygon ticker events (AAPL)")
        url = f"https://api.polygon.io/vX/reference/tickers/AAPL/events?apikey={POLYGON_KEY}"
        try:
            body, status = get(url)
            r.log(f"  status: {status}")
            data = json.loads(body)
            r.log(f"  type: {type(data.get('results')).__name__}")
            results = data.get("results", {})
            events = results.get("events") if isinstance(results, dict) else results
            if isinstance(events, list) and events:
                r.log(f"  n_events: {len(events)}")
                r.log(f"  first: {json.dumps(events[0])[:300]}")
            elif isinstance(events, dict):
                r.log(f"  events keys: {list(events.keys())}")
                r.log(f"  sample: {json.dumps(events)[:500]}")
            else:
                r.log(f"  raw: {json.dumps(data)[:500]}")
        except Exception as e:
            r.fail(f"  err: {e}")

        # 2. Polygon financials (includes filing dates)
        r.section("2. Polygon financials (AAPL)")
        url = f"https://api.polygon.io/vX/reference/financials?ticker=AAPL&apikey={POLYGON_KEY}&limit=4"
        try:
            body, status = get(url)
            data = json.loads(body)
            results = data.get("results") or []
            r.log(f"  n_results: {len(results)}")
            for f in results[:2]:
                r.log(f"    period:{f.get('start_date')}/{f.get('end_date')} filed:{f.get('filing_date')} acceptance:{f.get('acceptance_datetime')}")
        except Exception as e:
            r.fail(f"  err: {e}")

        # 3. Yahoo Finance v7 calendar (scraping)
        r.section("3. Yahoo Finance (AAPL)")
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=AAPL&fields=earningsTimestamp,earningsTimestampStart,earningsTimestampEnd,trailingAnnualDividendRate"
        try:
            body, status = get(url, headers={"User-Agent": "Mozilla/5.0"})
            data = json.loads(body)
            results = data.get("quoteResponse", {}).get("result", [])
            r.log(f"  n_results: {len(results)}")
            for q in results:
                ts = q.get("earningsTimestamp")
                if ts:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    r.log(f"  {q.get('symbol')} earnings: {dt.isoformat()}")
        except Exception as e:
            r.fail(f"  err: {e}")

        # 4. Nasdaq earnings calendar (free, web)
        r.section("4. Nasdaq earnings calendar today")
        today = datetime.now(timezone.utc).date().isoformat()
        url = f"https://api.nasdaq.com/api/calendar/earnings?date={today}"
        try:
            body, status = get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0)",
                "Accept": "application/json",
            })
            data = json.loads(body)
            r.log(f"  status: {status}")
            d = data.get("data", {})
            rows = d.get("rows") or []
            r.log(f"  n_rows today: {len(rows)}")
            for row in rows[:5]:
                r.log(f"    {row.get('symbol')}: {row.get('time')} EPS_est:{row.get('epsForecast')} fiscal:{row.get('fiscalQuarterEnding')}")
        except Exception as e:
            r.fail(f"  err: {e}")

        # 5. Yahoo earnings calendar HTML (last resort)
        r.section("5. Yahoo earnings calendar query")
        url = f"https://finance.yahoo.com/calendar/earnings?day={today}"
        try:
            body, status = get(url, headers={"User-Agent": "Mozilla/5.0"})
            r.log(f"  status: {status}, body size: {len(body)}b")
        except Exception as e:
            r.fail(f"  err: {e}")


if __name__ == "__main__":
    main()
