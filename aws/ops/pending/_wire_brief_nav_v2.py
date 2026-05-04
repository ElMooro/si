"""Add Brief tab to nav for pages that didn't match Today-tab pattern.

These pages use various nav styles (topnav with Home link, desk.html div nav,
intelligence.html nav-link emoji style). Universal approach: insert a Brief link
right after the first nav-internal page anchor.
"""
import os
import re
from ops_report import report

PAGES_REMAINING = [
    "ath.html", "auction-crisis.html", "bonds.html", "crisis.html", "desk.html",
    "edge.html", "insiders.html", "intelligence.html", "read.html", "regime.html",
    "risk.html", "signals.html", "system.html",
]


def already_has_brief(content):
    return bool(re.search(r'href="/?brief\.html"', content, re.IGNORECASE))


def patch_topnav_after_home(content):
    """topnav: <a href="/">Home</a> → insert <a href="/brief.html">Brief</a> right after."""
    pat = re.compile(r'(<a\s+href="/?"[^>]*>Home</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, False
    insertion = '\n  <a href="/brief.html">Brief</a>'
    return content[:m.end()] + insertion + content[m.end():], True


def patch_topnav_first_anchor(content):
    """topnav: insert before first <a href="/something.html"> inside .topnav nav."""
    # Find the topnav opening (nav with class topnav OR div with class nav)
    nav_open = re.search(r'<(nav|div)[^>]*class="(topnav|nav)"[^>]*>', content, re.IGNORECASE)
    if not nav_open:
        return content, False
    # Find first internal-page anchor inside the nav (after the open tag)
    after = content[nav_open.end():]
    pat = re.compile(r'<a\s+href="/?[a-z0-9_-]+\.html"[^>]*>', re.IGNORECASE)
    m = pat.search(after)
    if not m:
        return content, False
    insertion = '<a href="/brief.html">Brief</a>\n  '
    abs_pos = nav_open.end() + m.start()
    return content[:abs_pos] + insertion + content[abs_pos:], True


def patch_intelligence_emoji(content):
    """intelligence.html: insert before first nav-link <a class="nav-link" href="/...">."""
    pat = re.compile(r'<a\s+href="/?[a-z0-9_-]+\.html"\s+class="nav-link"[^>]*>', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, False
    insertion = '<a href="/brief.html" class="nav-link">📋 Brief</a>\n'
    return content[:m.start()] + insertion + content[m.start():], True


def main():
    with report("wire_brief_nav_v2") as r:
        r.heading("Add Brief tab to remaining pages with non-tab nav styles")
        results = {}
        for page in PAGES_REMAINING:
            if not os.path.exists(page):
                results[page] = "MISSING"
                continue
            with open(page) as f:
                content = f.read()
            if already_has_brief(content):
                results[page] = "already_has"
                continue
            patched = False
            for fn in [patch_intelligence_emoji, patch_topnav_after_home, patch_topnav_first_anchor]:
                content, ok = fn(content)
                if ok:
                    patched = True
                    results[page] = fn.__name__
                    break
            if patched:
                with open(page, "w") as f:
                    f.write(content)
            else:
                results[page] = "no_match"

        ok = sum(1 for v in results.values() if v.startswith("patch"))
        r.log(f"  patched: {ok}")
        r.log(f"  per page:")
        for p, s in sorted(results.items()):
            r.log(f"    {p:25s}  {s}")


if __name__ == "__main__":
    main()
