"""ops/709 — confirm defcon.html + baggers.html nav links are live."""
import json, os, urllib.request
from datetime import datetime, timezone

PAGES = ["index.html", "desk.html", "themes.html", "brief.html", "research.html"]


def fetch(page):
    try:
        req = urllib.request.Request(f"https://justhodl.ai/{page}",
                                      headers={"User-Agent": "JustHodl-Verify/1.0"})
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
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/709_nav_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 709_nav_verify.json :: all_ok=" + str(report["all_ok"]))


if __name__ == "__main__":
    main()
