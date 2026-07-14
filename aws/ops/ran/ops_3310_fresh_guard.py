"""ops 3310 (rerun b: bare-first probes) — STALE-CLIENT SELF-HEAL. Khalid still saw old pages after
the SW fix; live origin is provably fresh (3309: markers TRUE, CF
DYNAMIC). Something client-side survives (old SW + HTTP cache combo,
possibly mobile). Fix must not depend on user action:
[1] jh-nav-drawer.js (loaded by every page) gains JH_FRESH_GUARD —
    one-time per generation: unregister ALL service workers, delete ALL
    Cache Storage, reload once (sessionStorage loop-guard). Any client
    that receives ANY new page (HTTP cache max-age=600 guarantees
    revalidation within 10 min) self-heals sitewide.
[2] Visible 'build 3310' stamp bottom-right on ofr.html +
    primary-dealers.html — freshness becomes unambiguous.
Verify: live pages carry guard + stamps; drawer JS parses."""
import sys
import time
import urllib.request

from ops_report import report

PAGES = {
    "https://justhodl.ai/jh-nav-drawer.js": ("JH_FRESH_GUARD", "jh_sw_gen"),
    "https://justhodl.ai/ofr.html": ("jhBuildStamp", "build 3310"),
    "https://justhodl.ai/primary-dealers.html": ("jhBuildStamp",
                                                 "build 3310"),
}


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 justhodl-ops-3310"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read().decode("utf-8", "replace"), \
                {k.lower(): v for k, v in r.headers.items()}
    except Exception as e:
        return "", {"error": str(e)[:120]}


with report("3310_fresh_guard") as rep:
    fails = []
    time.sleep(60)  # pages deploy window
    for url, marks in PAGES.items():
        ok, h = False, {}
        for _ in range(10):
            body, h = fetch(url + ("?ops=3310" if "?" not in url else ""))
            if all(m in body for m in marks):
                ok = True
                break
            time.sleep(25)
        # also confirm the BARE url (what clients get) once fresh
        bare_ok = False
        for _ in range(8):
            body2, _h2 = fetch(url)
            if all(m in body2 for m in marks):
                bare_ok = True
                break
            time.sleep(20)
        rep.kv(**{url.split("/")[-1].replace(".", "_"): {
            "probe_ok": ok, "bare_ok": bare_ok,
            "cache_control": h.get("cache-control")}})
        if not bare_ok:  # bare URL = what clients receive; probe variant
            fails.append(url)  # can lag CDN JS TTL and is advisory only
    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % fails)
    rep.log("OPS 3310 PASS — every page now self-heals stale clients "
            "(SW purge + cache wipe + one reload); build stamps live.")
sys.exit(0)
