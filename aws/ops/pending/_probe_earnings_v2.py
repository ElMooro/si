"""Probe Nasdaq + Polygon financials more thoroughly."""
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from ops_report import report

POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"


def get(url, timeout=15, headers=None):
    headers = headers or {"User-Agent": "Mozilla/5.0 (Windows NT 10.0)"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(), resp.status


def main():
    with report("probe_earnings_v2") as r:
        r.heading("Earnings v2 — Nasdaq weekday + Polygon financials")

        # Nasdaq next 5 weekdays
        r.section("1. Nasdaq earnings — next 5 weekdays")
        today = datetime.now(timezone.utc).date()
        for offset in range(7):
            d = today + timedelta(days=offset)
            if d.weekday() >= 5:
                continue
            url = f"https://api.nasdaq.com/api/calendar/earnings?date={d.isoformat()}"
            try:
                body, _ = get(url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Accept": "application/json",
                    "Origin": "https://www.nasdaq.com",
                    "Referer": "https://www.nasdaq.com/market-activity/earnings",
                })
                data = json.loads(body)
                rows = (data.get("data") or {}).get("rows") or []
                if rows:
                    r.log(f"  {d.isoformat()} ({d.strftime('%a')}): {len(rows)} reports — sample:")
                    for row in rows[:3]:
                        r.log(f"    {row.get('symbol'):6s} {row.get('name','')[:30]:30s} time:{row.get('time'):8s} EPS_est:{row.get('epsForecast')}")
            except Exception as e:
                r.log(f"  {d}: err {e}")

        # Polygon financials with filed dates — that's our source for RECENT
        r.section("2. Polygon financials for major tickers (recent filings)")
        for tkr in ["AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN"]:
            url = f"https://api.polygon.io/vX/reference/financials?ticker={tkr}&apikey={POLYGON_KEY}&limit=2&order=desc&sort=filing_date"
            try:
                body, _ = get(url)
                data = json.loads(body)
                results = data.get("results") or []
                if results:
                    f = results[0]
                    fin = f.get("financials", {})
                    income = fin.get("income_statement", {})
                    eps_basic = (income.get("basic_earnings_per_share") or {}).get("value")
                    eps_dil = (income.get("diluted_earnings_per_share") or {}).get("value")
                    rev = (income.get("revenues") or {}).get("value")
                    r.log(f"  {tkr}: filed:{f.get('filing_date')} period:{f.get('start_date')}/{f.get('end_date')} EPS:{eps_dil} REV:{rev}")
            except Exception as e:
                r.log(f"  {tkr}: err {e}")


if __name__ == "__main__":
    main()
