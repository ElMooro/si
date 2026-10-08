"""ops 3205 — the chip, definitively: 3203/3204 windows missed it because
the bake's research fetch used a custom UA Cloudflare filters; it now uses
the exact shape fetch_age already proves works (Mozilla UA + cache-buster,
with the outcome printed to bake logs). This ops waits out the pages
deploy and confirms the HIS RESEARCH payload in a live rail."""
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3205)"}


def get(url):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=15).read().decode(
        "utf-8", "replace")


with report("3205_chip_verify") as rep:
    fails = []
    rep.heading("ops 3205 — HIS RESEARCH chip live, definitively")
    ok = False
    for i in range(26):
        time.sleep(15)
        try:
            h = get(f"https://justhodl.ai/flows.html?t={int(time.time())}")
            if '"research"' in h and "panels.html" in h:
                ok = True
                rep.ok(f"chip live in flows.html rail after ~{(i+1)*15}s")
                break
        except Exception:
            pass
    if not ok:
        fails.append("chip still absent after fixed-UA bake — read the "
                     "pages.yml bake log line '[rail] research chip …'")
    rep.kv(n_fails=len(fails), verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
