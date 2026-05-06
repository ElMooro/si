"""Run inside GH Actions runner — verify news.html is live + nav patches deployed."""
import urllib.request
from ops_report import report


def check(url, expect_substring=None):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "audit/1.0", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode(errors="ignore")
            return resp.status, len(body), body.count(expect_substring) if expect_substring else None
    except Exception as e:
        return f"ERR:{e}", 0, None


def main():
    with report("verify_news_deploy") as r:
        r.heading("Verify news.html on GH Pages")
        st, sz, _ = check("https://justhodl.ai/news.html")
        r.log(f"  https://justhodl.ai/news.html → status={st} size={sz:,}b")

        r.heading("Verify nav patches across all 16 pages")
        pages = [
            "today.html", "read.html", "signals.html", "ticker.html",
            "feedback.html", "13f.html", "insiders.html",
            "accuracy.html", "sectors.html", "vol.html", "momentum.html",
            "intelligence.html", "edge.html", "risk.html", "system.html", "index.html",
        ]
        ok = 0
        for p in pages:
            st, sz, count = check(f"https://justhodl.ai/{p}", expect_substring='/news.html')
            status_icon = "✓" if (st == 200 and count and count >= 1) else "✗"
            r.log(f"  {status_icon} {p:22s} status={st} size={sz:>6,}b news_links={count}")
            if status_icon == "✓":
                ok += 1
        r.log(f"\n  {ok}/16 pages confirmed with news.html nav link")


if __name__ == "__main__":
    main()
