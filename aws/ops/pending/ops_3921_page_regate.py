"""
ops_3921 — regate ONLY the served-page check from 3920 (engine fixes all
passed; the page check raced pages.yml + edge purge). Marker: FLEET INPUTS.
"""
import sys, time, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"


def main():
    with report("3921_page_regate") as rep:
        rep.heading("ops 3921 — served risk-gate.html renders FLEET INPUTS")
        ok, size = False, 0
        for a in range(9):
            try:
                req = urllib.request.Request(
                    f"https://justhodl.ai/risk-gate.html?v={int(time.time())}{a}",
                    headers={"User-Agent": UA, "Cache-Control": "no-cache"})
                html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
                size = len(html)
                if "FLEET INPUTS" in html and "score_fused" in html:
                    ok = True; rep.ok(f"  attempt {a+1}: markers live, {size} bytes"); break
                rep.log(f"  attempt {a+1}: {size} bytes, markers not yet present")
            except Exception as e:
                rep.log(f"  attempt {a+1}: {str(e)[:80]}")
            time.sleep(20)
        if not ok:
            rep.fail("page still stale after 9 attempts"); sys.exit(1)
        rep.ok("PASS_ALL — v2.1 fully closed: engine + page")


if __name__ == "__main__":
    main()
