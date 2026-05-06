"""Add 'Brief' tab to navigation across all 18+ JustHodl pages.

Multiple nav styles exist:
  - Modern tabs (most pages): <a class="tab" href="/today.html">Today</a>
  - Topnav style: <a href="today.html">Today</a> in <nav class="topnav">
  - Caps style: <li><a href="today.html">TODAY</a></li>
  - Emoji style: e.g., <a class="nav-link" href="today.html">📅 Today</a>

For each nav style, insert 'Brief' RIGHT AFTER 'Today' so it's the prime CTA.
This also marks the active state on brief.html itself.
"""
import os
import re
import boto3
from ops_report import report

REPO = "/home/runner/work/si/si"  # GitHub Actions checkout path
if not os.path.isdir(REPO):
    REPO = "."

# Pages that should have the Brief tab (skip: index, error pages, deprecated old)
PAGES = [
    "today.html", "read.html", "signals.html", "insiders.html", "13f.html",
    "ticker.html", "accuracy.html", "sectors.html", "allocator.html",
    "vol.html", "news.html", "momentum.html", "research.html", "feedback.html",
    "desk.html", "intelligence.html", "risk.html", "system.html", "edge.html",
    "ath.html", "auction-crisis.html", "bonds.html", "crisis.html", "regime.html",
]

# Pattern variants: (search regex, replacement_fn)
def patch_modern_tabs(content, page):
    """<a class="tab" href="/today.html">Today</a>  → insert Brief right after."""
    # Already there?
    if re.search(r'href="/?brief\.html"', content):
        return content, "already_has_brief"
    # Find Today tab
    pat = re.compile(r'(<a\s+class="tab(?:\s+active)?"\s+href="/today\.html">Today</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, "no_today_tab"
    is_brief_page = page == "brief.html"
    cls = 'tab active' if is_brief_page else 'tab'
    insertion = f'\n    <a class="{cls}" href="/brief.html">Brief</a>'
    new = content[:m.end()] + insertion + content[m.end():]
    return new, "ok_modern"

def patch_topnav(content, page):
    """<nav class="topnav">...<a href="today.html">Today</a> → insert Brief."""
    if re.search(r'href="/?brief\.html"', content):
        return content, "already_has_brief"
    pat = re.compile(r'(<a\s+href="/?today\.html"[^>]*>Today</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, "no_today_tab"
    insertion = f'\n      <a href="brief.html">Brief</a>'
    new = content[:m.end()] + insertion + content[m.end():]
    return new, "ok_topnav"

def patch_caps_li(content, page):
    """<li><a href="today.html">TODAY</a></li> → insert Brief."""
    if re.search(r'href="/?brief\.html"', content, re.IGNORECASE):
        return content, "already_has_brief"
    pat = re.compile(r'(<li><a\s+href="/?today\.html">TODAY</a></li>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, "no_today_tab"
    insertion = '\n      <li><a href="brief.html">BRIEF</a></li>'
    new = content[:m.end()] + insertion + content[m.end():]
    return new, "ok_caps"

def patch_emoji_nav(content, page):
    """<a class="nav-link" href="today.html">📅 Today</a> → insert Brief."""
    if re.search(r'href="/?brief\.html"', content, re.IGNORECASE):
        return content, "already_has_brief"
    pat = re.compile(r'(<a\s+class="nav-link"\s+href="/?today\.html"[^>]*>[^<]*Today[^<]*</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, "no_today_tab"
    insertion = '\n      <a class="nav-link" href="brief.html">📋 Brief</a>'
    new = content[:m.end()] + insertion + content[m.end():]
    return new, "ok_emoji"


def main():
    with report("wire_brief_nav") as r:
        r.heading("Add Brief tab to navigation across all pages")
        os.chdir(REPO)
        results = {}
        for page in PAGES + ["brief.html"]:
            if not os.path.exists(page):
                results[page] = "MISSING"
                continue
            with open(page) as f:
                content = f.read()
            original = content
            for fn in [patch_modern_tabs, patch_topnav, patch_caps_li, patch_emoji_nav]:
                content, status = fn(content, page)
                if status.startswith("ok") or status == "already_has_brief":
                    break
            if content != original:
                with open(page, "w") as f:
                    f.write(content)
            results[page] = status

        # Tally
        ok = sum(1 for v in results.values() if v.startswith("ok"))
        already = sum(1 for v in results.values() if v == "already_has_brief")
        missing = sum(1 for v in results.values() if v == "MISSING")
        notfound = sum(1 for v in results.values() if v == "no_today_tab")
        r.log(f"  ok (patched):       {ok}")
        r.log(f"  already had brief:  {already}")
        r.log(f"  missing files:      {missing}")
        r.log(f"  no today nav found: {notfound}")
        r.log("")
        r.log("Per-page result:")
        for p, s in sorted(results.items()):
            r.log(f"  {p:25s}  {s}")


if __name__ == "__main__":
    main()
