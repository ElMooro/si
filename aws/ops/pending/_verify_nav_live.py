# rerun-marker: 1777899709
"""Verify ticker.html + feedback.html links are live on justhodl.ai across 10 pages."""
import urllib.request
from ops_report import report

PAGES = [
    "index.html", "insiders.html", "read.html",
    "risk.html", "system.html", "13f.html", "today.html",
    "signals.html", "intelligence.html", "edge.html",
    "ticker.html", "feedback.html",
]


def main():
    with report("verify_nav_live") as r:
        r.heading("Verify ticker/feedback in nav across 10+ pages")
        ok_count = 0
        ticker_count = 0
        feedback_count = 0
        for p in PAGES:
            url = f"https://justhodl.ai/{p}"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "audit/1.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    body = resp.read().decode("utf-8", errors="ignore")
                t = body.count('href="/ticker.html"')
                f = body.count('href="/feedback.html"')
                if t > 0:
                    ticker_count += 1
                if f > 0:
                    feedback_count += 1
                ok_count += 1
                r.ok(f"  ✓ {p:30s} status={resp.status}  ticker_links={t}  feedback_links={f}")
            except Exception as e:
                r.log(f"  ✗ {p}: {e}")
        r.log(f"")
        r.log(f"  pages ok:                {ok_count}/{len(PAGES)}")
        r.log(f"  pages with ticker link:  {ticker_count}")
        r.log(f"  pages with feedback link:{feedback_count}")


if __name__ == "__main__":
    main()
