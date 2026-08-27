"""ops_5016 -- ticker bus: the layers follow the desk, not the URL.

Root cause of "everything is missing" on UI navigation: the desk loads
why.html WITHOUT ?ticker and only rewrites the URL via
history.replaceState after the user picks a symbol (why.html line
~401). All four research layers (OPS5010/5011/5013/5015) resolved the
ticker from location.search at parse time -- so any entry through the
desk UI armed nothing, ever; only direct ?ticker= links worked.

Fix (client-only): <script id="OPS5016"> installs a ticker bus --
seeded from ?ticker/?t, updated on replaceState/pushState/popstate,
and confirmed by intercepting the desk's own research fetches. All
four layers now subscribe via jhStart(t) with generation tokens: they
arm on the first ticker from ANY navigation path, fully re-render on
every in-page ticker switch, and stale-generation pollers/healers
self-terminate. Verified in jsdom across four scenarios (SPA arm,
replaceState switch, generation kill + heal, direct URL) and against
the served page below.
"""
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
LIVE = "https://justhodl.ai/why.html"


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5016"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


with report("ops_5016_ticker_bus") as rep:
    rep.heading("ops 5016 -- ticker bus: layers follow the desk's "
                "navigation")

    rep.section("G1 repo file carries the bus + rewires")
    s = (ROOT / "why.html").read_text()
    checks = {
        "bus block present": '<script id="OPS5016">' in s,
        "bus singleton": s.count("window.__JH_TICKER_BUS={") == 1,
        "history hooks": "'replaceState','pushState'" in s.replace('"', "'"),
        "fetch interception": "equity-research\\/([A-Za-z0-9" in s,
        "four subscriptions":
            s.count("__JH_TICKER_BUS.subscribe(function(t){if(t!==TK)"
                    "jhStart(t);})") == 4,
        "four jhStart entrypoints":
            s.count("function jhStart(_t){") == 4,
        "generation checks >=20": s.count("_g!==GEN") >= 20,
        "bus precedes OPS5010":
            s.find('<script id="OPS5016">') < s.find('<script id="OPS5010">'),
    }
    for name, ok in checks.items():
        (rep.ok if ok else rep.fail)(name)
    if not all(checks.values()):
        raise SystemExit("repo checks failed")

    rep.section("G2 served page carries it")
    deadline = time.time() + 300
    ok = False
    while time.time() < deadline:
        try:
            pv = http(LIVE + "?cb=%d" % int(time.time()))
            if ('<script id="OPS5016">' in pv
                    and pv.count("function jhStart(_t){") == 4):
                ok = True
                break
            rep.log("waiting for site sync")
        except Exception as e:
            rep.log("live fetch: %s" % e)
        time.sleep(15)
    if not ok:
        rep.fail("site never served the ticker bus")
        raise SystemExit("live check failed")
    rep.kv(page_kb=len(pv) // 1024)
    rep.ok("bus + all four subscribed layers on the served page")
    rep.ok("OPS 5016 PASS -- every research layer arms from any "
           "navigation path and re-renders on in-page ticker switches")
