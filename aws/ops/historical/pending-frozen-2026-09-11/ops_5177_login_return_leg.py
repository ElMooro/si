"""ops_5177 -- the PKCE-correct return-leg test for Google sign-in (READ-ONLY).

ops 5173 (post-restore) proved: project up, Google provider enabled, the
authorize hop lands on Google's account chooser with the valid client and
redirect URI, site wiring + CSP clean, keep-alive armed. Its return-leg
check was inconclusive by construction: supabase-js only exchanges a
?code= when the PKCE code_verifier from the SAME browser's sign-in start is
present. So here the browser starts the Google sign-in (verifier stored),
then comes back to the site with a fake ?code= -- supabase-js must now call
POST /auth/v1/token?grant_type=pkce and get a 4xx for the fake code. That
call happening is the proof the return path works end to end.
"""
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SITE = "https://justhodl.ai"
FAILS = []

with report("ops_5177_login_return_leg") as R:
    R.heading("ops 5177 -- Google sign-in return leg (PKCE-correct)")
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
        for page_path in ("/my-portfolio.html", "/chart-pro.html?s=VLO&tf=1W"):
            ctx = b.new_context(viewport={"width": 1440, "height": 1000})
            pg = ctx.new_page()
            token_calls = []
            pg.on("response", lambda rs: token_calls.append((rs.status, rs.url[:110])) if "/auth/v1/token" in rs.url else None)
            pg.goto(SITE + page_path, wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(4000)
            pg.click("#jh-signin-btn")
            pg.wait_for_timeout(600)
            try:
                with pg.expect_navigation(timeout=20000):
                    pg.click("#jh-google")
            except Exception as e:
                R.warn("   %s: no navigation to Google: %s" % (page_path, str(e)[:100]))
            pg.wait_for_timeout(1500)
            at_google = "accounts.google.com" in pg.url
            verifier = pg.evaluate("() => Object.keys(localStorage).filter(k => k.includes('code-verifier'))") if not at_google else None
            # back to the site with a fake code in the same browser context (verifier is in localStorage)
            sep = "&" if "?" in page_path else "?"
            pg.goto(SITE + page_path + sep + "code=ops5177-fake-code", wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(7000)
            keys = pg.evaluate("() => Object.keys(localStorage).filter(k => k.startsWith('sb-'))")
            R.log("   %-30s went to Google=%s  token calls=%s  sb keys=%s" % (page_path, at_google, token_calls, keys[:4]))
            if not at_google:
                FAILS.append("%s: Google button did not reach Google (%s)" % (page_path, verifier))
            if not token_calls:
                FAILS.append("%s: no /auth/v1/token exchange on return with a stored verifier" % page_path)
            elif token_calls[0][0] < 400:
                R.warn("   unexpected token status %s for a fake code" % token_calls[0][0])
            else:
                R.ok("   %s: return leg exchanges the code (%s for the fake code = the path is live)" % (page_path, token_calls[0][0]))
            ctx.close()
        b.close()
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN: Google sign-in path is live end to end (start -> Google -> return -> token exchange)")
