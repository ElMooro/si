"""ops 3245 — the series-fusion loop closed data→screen: each of the
four new model-input blocks now renders as a card on its native desk
page (independent fetch, additive, zero coupling to existing render
code). Live-verify the snippet in served HTML for all four pages; the
block VALUES were already live-verified in 3244's feeds."""
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3245)"}
PAGES = {
    "risk-regime.html": "jh-fusion-europe_sovereign",
    "eurodollar.html": "jh-fusion-euro_policy_corridor",
    "chart-macro.html": "jh-fusion-global_confidence",
    "defcon.html": "jh-fusion-btp_bund_canary",
}

with report("3245_fusion_surfaced") as rep:
    fails = []
    rep.heading("ops 3245 — fusion cards live on the four desk pages")
    pending = dict(PAGES)
    for i in range(26):
        time.sleep(15)
        for pg, marker in list(pending.items()):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/{pg}?t={int(time.time())}",
                    headers=UA), timeout=15).read().decode("utf-8",
                                                           "replace")
                if marker in h:
                    rep.ok(f"{pg}: card live (~{(i + 1) * 15}s)")
                    pending.pop(pg)
            except Exception:
                pass
        if not pending:
            break
    for pg in pending:
        fails.append(f"{pg}: marker not live in window")
    rep.kv(live=len(PAGES) - len(pending), of=len(PAGES),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
