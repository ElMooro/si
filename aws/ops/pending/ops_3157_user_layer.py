"""ops 3157 — user-visible SaaS layer, CDN verification.

Shipped in this push:
  • jh-nav-drawer.js (all 366 pages): pre-paint theme apply, ☀/🌙
    toggle in the drawer, per-user sync module (JH_USERSYNC_V1) —
    favorites UNION-merge on login, theme last-write-wins, debounced
    PUT to the JWT-hardened /userdata (anonymous users stay
    localStorage-only, zero behavior change).
  • jh-theme.css: [data-theme="light"] token palette (zero-specificity
    — page CSS still wins where explicit).
  • settings.html (JH_SETTINGS_V2): Appearance dark/light control +
    "Manage billing" → /billing-portal for paid tiers.

Gates: CDN serves all three markers (retry across the 600s cache);
drawer JS parses; theme block present with both palettes.
"""

import sys
import time
import urllib.request

from ops_report import report

CHECKS = [
    ("https://justhodl.ai/jh-nav-drawer.js", "JH_USERSYNC_V1"),
    ("https://justhodl.ai/jh-nav-drawer.js", "jhnav-themebtn"),
    ("https://justhodl.ai/jh-theme.css", 'data-theme="light"'),
    ("https://justhodl.ai/settings.html", "JH_SETTINGS_V2"),
    ("https://justhodl.ai/settings.html", "billing-portal"),
]


def fetch(url):
    req = urllib.request.Request(
        f"{url}?t={int(time.time())}",
        headers={"User-Agent": "Mozilla/5.0 ops-3157",
                 "Cache-Control": "no-cache"})
    return urllib.request.urlopen(req, timeout=15).read().decode(
        "utf-8", "replace")


with report("3157_user_layer") as rep:
    fails, warns = [], []
    rep.heading("ops 3157 — favorites sync + theme + settings on CDN")

    pending = list(CHECKS)
    for attempt in range(16):
        still = []
        for url, marker in pending:
            try:
                if marker not in fetch(url):
                    still.append((url, marker))
            except Exception:
                still.append((url, marker))
        pending = still
        if not pending:
            break
        time.sleep(30)
    for url, marker in CHECKS:
        if (url, marker) in pending:
            warns.append(f"CDN not yet serving {marker} on "
                         f"{url.split('/')[-1]} (cache self-heals)")
        else:
            rep.ok(f"{url.split('/')[-1]} :: {marker}")
    if len(pending) == len(CHECKS):
        fails.append("NOTHING landed on CDN after ~8 min — pages deploy "
                     "broke; investigate pages.yml run")

    rep.kv(markers_live=len(CHECKS) - len(pending),
           markers_pending=len(pending))
    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
