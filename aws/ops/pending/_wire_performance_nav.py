"""Add 'Performance' tab to navigation across all pages.

Insertion point: right AFTER 'Brief' tab so the order becomes
Today / Brief / Performance / Read / ...
"""
import os
import re
from ops_report import report

PAGES = [
    "today.html", "brief.html", "read.html", "signals.html", "insiders.html", "13f.html",
    "ticker.html", "accuracy.html", "sectors.html", "allocator.html",
    "vol.html", "news.html", "momentum.html", "research.html", "feedback.html",
    "desk.html", "intelligence.html", "edge.html",
]


def already_has(content):
    return bool(re.search(r'href="/?performance\.html"', content, re.IGNORECASE))


def patch_modern_tabs(content, page):
    if already_has(content):
        return content, "already_has"
    pat = re.compile(r'(<a\s+class="tab(?:\s+active)?"\s+href="/brief\.html">Brief</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    is_perf_page = page == "performance.html"
    cls = 'tab active' if is_perf_page else 'tab'
    insertion = f'\n    <a class="{cls}" href="/performance.html">Performance</a>'
    new = content[:m.end()] + insertion + content[m.end():]
    return new, "ok_modern"


def patch_topnav(content, page):
    """Insert after first occurrence of <a href="/brief.html">Brief</a> in topnav-style."""
    if already_has(content):
        return content, "already_has"
    pat = re.compile(r'(<a\s+href="/?brief\.html">Brief</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    insertion = '\n  <a href="/performance.html">Performance</a>'
    return content[:m.end()] + insertion + content[m.end():], "ok_topnav"


def patch_intelligence_emoji(content, page):
    """intelligence.html: insert before the first nav-link page anchor."""
    if already_has(content):
        return content, "already_has"
    pat = re.compile(r'<a\s+href="/?brief\.html"\s+class="nav-link"[^>]*>[^<]*</a>', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    insertion = '<a href="/performance.html" class="nav-link">📈 Performance</a>\n'
    return content[:m.end()] + '\n' + insertion + content[m.end():], "ok_emoji"


def main():
    with report("wire_performance_nav") as r:
        r.heading("Add Performance tab to navigation")
        results = {}
        for page in PAGES:
            if not os.path.exists(page):
                results[page] = "MISSING"
                continue
            with open(page) as f:
                content = f.read()
            patched = False
            for fn in [patch_modern_tabs, patch_intelligence_emoji, patch_topnav]:
                new_content, status = fn(content, page)
                if status:
                    if status.startswith("ok") and new_content != content:
                        with open(page, "w") as f:
                            f.write(new_content)
                        patched = True
                    results[page] = status
                    break
            if not patched and page not in results:
                results[page] = "no_match"

        r.log(f"  Per-page result:")
        ok = 0
        for p, s in sorted(results.items()):
            if s and s.startswith("ok"):
                ok += 1
            r.log(f"    {p:25s}  {s}")
        r.log(f"")
        r.log(f"  → {ok}/{len(PAGES)} patched")


if __name__ == "__main__":
    main()
