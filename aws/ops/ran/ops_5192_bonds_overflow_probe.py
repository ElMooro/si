"""ops_5192 -- which element overflows bonds.html at 390px? (READ-ONLY, headless Chrome)"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

with report("ops_5192_bonds_overflow_probe") as R:
    R.heading("ops 5192 -- bonds.html 390px overflow probe")
    try:
        import playwright  # noqa: F401
    except Exception:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        try:
            b = p.chromium.launch(channel="chrome", headless=True)
        except Exception:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            b = p.chromium.launch(headless=True)
        pg = b.new_page(viewport={"width": 390, "height": 844})
        pg.goto("https://justhodl.ai/bonds.html", wait_until="domcontentloaded", timeout=60000)
        pg.wait_for_timeout(8000)
        res = pg.evaluate("""() => {
            const W = document.documentElement.clientWidth;
            const out = [];
            for (const el of document.querySelectorAll('body *')) {
                const r = el.getBoundingClientRect();
                if (r.width > 0 && r.right > W + 2 && !el.closest('svg') ) out.push({tag: el.tagName, id: el.id, cls: String(el.className).slice(0, 60), right: Math.round(r.right), w: Math.round(r.width)});
            }
            out.sort((a, b) => b.right - a.right);
            const before = document.documentElement.scrollWidth - W;
            const wr = document.getElementById('wr'); if (wr) wr.remove();
            const after = document.documentElement.scrollWidth - W;
            return {W, before, after, worst: out.slice(0, 12)};
        }""")
        R.log("   viewport %s overflow with war room=%dpx, without=%dpx" % (res["W"], res["before"], res["after"]))
        for w in res["worst"]:
            R.log("   %s#%s .%s right=%s w=%s" % (w["tag"], w["id"], w["cls"], w["right"], w["w"]))
        b.close()
    R.ok("done")
    if False:
        sys.exit(1)
