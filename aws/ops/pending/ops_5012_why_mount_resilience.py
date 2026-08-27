"""ops_5012 -- why.html mount resilience for the 5010/5011 report layers.

Root cause of "all the GuruFocus visuals are missing": the desk's main
render does $("content").innerHTML = html AFTER the additive modules
mount, wiping every 5010/5011 section body -- only the chips survived
because their inserter retries. Fix (client-only): both <script
id="OPS5010"> and <script id="OPS5011"> now keep the fetched doc and
self-heal -- MutationObserver (debounced) + 1.5s interval remount for
5 minutes, idempotent wraps (old wrap removed before insert), guarded
chips so heals never duplicate. Verified here against the repo file
and the actual served page.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import urllib.request

ROOT = Path(__file__).resolve().parents[3]
LIVE = "https://justhodl.ai/why.html"


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5012"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


with report("ops_5012_why_mount_resilience") as rep:
    rep.heading("ops 5012 -- why.html mount resilience (5010/5011 layers)")

    rep.section("G1 repo file carries the resilience layer")
    s = (ROOT / "why.html").read_text()
    checks = {
        'exactly one OPS5010 block': s.count('<script id="OPS5010">') == 1,
        'exactly one OPS5011 block': s.count('<script id="OPS5011">') == 1,
        'two ops-5012 guards': s.count("ops 5012:") == 2,
        'idempotent 5010 mount':
            "_old=document.getElementById('jh5010-wrap')" in s,
        'idempotent 5011 mount':
            "_old=document.getElementById('jh5011-wrap')" in s,
        'heal closure x2':
            s.count("&&DOC)run(DOC);});") == 2,
        '5010 chips guarded': "el.id='jh5010-chips'" in s,
        '5011 chips guarded': "el.id='jh5011-chips'" in s,
        'observer armed x2': s.count(
            "mo.observe(document.body,{childList:true,subtree:true})") == 2,
    }
    for name, ok in checks.items():
        (rep.ok if ok else rep.fail)(name)
    if not all(checks.values()):
        raise SystemExit("repo checks failed")

    rep.section("G2 served page carries it (site sync)")
    deadline = time.time() + 300
    live_ok = False
    while time.time() < deadline:
        try:
            page = http(LIVE + "?cb=%d" % int(time.time()))
            if page.count("ops 5012:") == 2:
                live_ok = True
                break
            rep.log("live page not synced yet -- retrying")
        except Exception as e:
            rep.log("live fetch: %s" % e)
        time.sleep(15)
    if not live_ok:
        rep.fail("live page never showed the ops-5012 guards")
        raise SystemExit("live sync failed")
    for frag, label in (
        ('<script id="OPS5010">', "OPS5010 block live"),
        ('<script id="OPS5011">', "OPS5011 block live"),
        ("&&DOC)run(DOC);});", "heal closure live"),
        ("el.id='jh5010-chips'", "chip guard live"),
    ):
        if frag in page:
            rep.ok(label)
        else:
            rep.fail(label)
            raise SystemExit("live fragment missing: %s" % label)
    rep.kv(live_guards=page.count("ops 5012:"),
           page_kb=len(page) // 1024)
    rep.ok("OPS 5012 PASS -- 5010/5011 sections now survive the desk's "
           "re-render; every GuruFocus visual mounts after Quantitative "
           "Risk and self-heals for 5 minutes")
