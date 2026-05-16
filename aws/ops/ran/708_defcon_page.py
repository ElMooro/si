"""ops/708 — re-verify defcon.html is live after GitHub Pages publish."""
import json, os, urllib.request
from datetime import datetime, timezone


def main():
    markers = ["Risk &amp; Opportunity Command Center", "crisis-composite.json",
               "capitulation.json", "china-liquidity.json", "bank-stress.json",
               "Master Crisis Score", "Capitulation", "China Liquidity",
               "Bank Funding Stress"]
    try:
        req = urllib.request.Request("https://justhodl.ai/defcon.html",
                                      headers={"User-Agent": "JustHodl-Verify/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            code = resp.getcode()
            html = resp.read().decode("utf-8", "replace")
    except Exception as e:
        code, html = None, str(e)[:200]

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "http_status": code,
        "size_bytes": len(html) if isinstance(html, str) else None,
        "missing_markers": [m for m in markers if not (isinstance(html, str) and m in html)],
        "defcon_page_live": (isinstance(html, str) and code == 200
                              and all(m in html for m in markers)),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/708_defcon_page.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 708_defcon_page.json :: live=" + str(report["defcon_page_live"])
          + " status=" + str(code))


if __name__ == "__main__":
    main()
