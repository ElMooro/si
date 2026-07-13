"""ops 3247 — 3245's verifier searched for a RUNTIME-composed id
('jh-fusion-'+C.field) in page SOURCE, which can never match. The
diagnostic proved the snippet IS served fresh. Correct check: the
source-literal config string field:"<name>" per page."""
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3247)"}
PAGES = {
    "risk-regime.html": 'field:"europe_sovereign"',
    "eurodollar.html": 'field:"euro_policy_corridor"',
    "chart-macro.html": 'field:"global_confidence"',
    "defcon.html": 'field:"btp_bund_canary"',
}

with report("3247_surface_verified") as rep:
    fails = []
    rep.heading("ops 3247 — fusion cards verified against source "
                "literals")
    for pg, lit in PAGES.items():
        ok = False
        for _ in range(3):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/{pg}?t={int(time.time())}",
                    headers=UA), timeout=15).read().decode("utf-8",
                                                           "replace")
                if lit in h and "ops 3245" in h:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(8)
        if ok:
            rep.ok(f"{pg}: fusion card in served HTML")
        else:
            fails.append(f"{pg}: literal absent")
    rep.kv(live=len(PAGES) - len(fails), of=len(PAGES),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
