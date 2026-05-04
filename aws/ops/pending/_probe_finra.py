"""
Diagnose FINRA short volume fetch — n_tickers_finra=0 in smoke test.
"""
import urllib.request
from datetime import datetime, timedelta, timezone
from ops_report import report


def http_get_raw(url, timeout=20, ua="Mozilla/5.0 (compatible; justhodl-short-interest/1.0)"):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def main():
    with report("probe_finra_short_volume") as r:
        r.heading("Diagnose FINRA short volume fetch")

        # Try last 5 trading days
        today = datetime.now(timezone.utc).date()
        for offset in range(1, 8):
            d = today - timedelta(days=offset)
            if d.weekday() >= 5:
                continue
            yyyymmdd = d.strftime("%Y%m%d")
            urls_to_try = [
                f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{yyyymmdd}.txt",
                f"https://cdn.finra.org/equity/regsho/daily/FNYXshvol{yyyymmdd}.txt",
                f"https://cdn.finra.org/equity/regsho/daily/FNRAshvol{yyyymmdd}.txt",
                f"https://cdn.finra.org/equity/regsho/daily/FORFshvol{yyyymmdd}.txt",
                f"https://cdn.finra.org/equity/regsho/daily/FNQCshvol{yyyymmdd}.txt",
            ]
            r.section(f"Date: {d.isoformat()} ({d.strftime('%a')})")
            for u in urls_to_try:
                try:
                    status, body = http_get_raw(u, timeout=15)
                    sz = len(body)
                    head = body[:300].decode("utf-8", errors="replace")
                    r.log(f"  {u.split('/')[-1]:40s} status:{status} size:{sz:>8,}")
                    if sz > 0:
                        # Show first 2 data lines
                        lines = head.splitlines()[:3]
                        for line in lines:
                            r.log(f"      {line[:150]}")
                except Exception as e:
                    r.log(f"  {u.split('/')[-1]:40s} ✗ {str(e)[:80]}")
            break  # only do first valid trading day


if __name__ == "__main__":
    main()
