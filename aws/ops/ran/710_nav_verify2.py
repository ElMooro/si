"""ops/710 — re-verify nav links, bypassing CDN edge cache via a unique
query string (Cloudflare keys cache on full URL incl. query)."""
import json, os, time, urllib.request
from datetime import datetime, timezone

PAGES = ["index.html", "desk.html", "themes.html", "brief.html", "research.html",
         "vol.html", "today.html", "portfolio.html"]


def fetch(page):
    cb = int(time.time() * 1000)
    try:
        req = urllib.request.Request(
            f"https://justhodl.ai/{page}?cb={cb}",
            headers={"User-Agent": "JustHodl-Verify/1.0",
                     "Cache-Control": "no-cache", "Pragma": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)[:160]


def main():
    report = {"checked_at": datetime.now(timezone.utc).isoformat(), "pages": {}}
    for p in PAGES:
        code, html = fetch(p)
        ok = isinstance(html, str) and code == 200
        report["pages"][p] = {
            "http_status": code,
            "has_defcon_link": ok and '"/defcon.html"' in html,
            "has_baggers_link": ok and '"/baggers.html"' in html,
        }
    report["all_ok"] = all(v.get("has_defcon_link") and v.get("has_baggers_link")
                            for v in report["pages"].values())
    report["pages_ok"] = sum(1 for v in report["pages"].values()
                              if v.get("has_defcon_link") and v.get("has_baggers_link"))
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/710_nav_verify2.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"DONE -> 710_nav_verify2.json :: all_ok={report['all_ok']} "
          f"({report['pages_ok']}/{len(PAGES)})")


if __name__ == "__main__":
    main()
