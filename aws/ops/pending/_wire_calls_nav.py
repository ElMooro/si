"""Add 'Calls' tab to nav across all pages."""
import os
import re
from ops_report import report

PAGES = [
    "today.html", "brief.html", "performance.html", "weights.html",
    "accuracy.html", "sectors.html", "allocator.html", "vol.html", "news.html",
    "momentum.html", "research.html", "feedback.html",
    "13f.html", "ticker.html", "insiders.html", "signals.html",
    "read.html", "desk.html", "edge.html", "intelligence.html",
]


def already_has(content):
    return bool(re.search(r'href="/?calls\.html"', content, re.IGNORECASE))


def patch_modern(content, page):
    if already_has(content):
        return content, "already_has"
    # Insert AFTER Brief tab
    pat = re.compile(r'(<a\s+class="tab(?:\s+active)?"\s+href="/brief\.html">Brief</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    is_self = page == "calls.html"
    cls = 'tab active' if is_self else 'tab'
    insertion = f'\n    <a class="{cls}" href="/calls.html">Calls</a>'
    return content[:m.end()] + insertion + content[m.end():], "ok_modern"


def patch_topnav(content, page):
    if already_has(content):
        return content, "already_has"
    pat = re.compile(r'(<a\s+href="/?brief\.html">Brief</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    return content[:m.end()] + '\n  <a href="/calls.html">Calls</a>' + content[m.end():], "ok_topnav"


def patch_emoji(content, page):
    if already_has(content):
        return content, "already_has"
    pat = re.compile(r'<a\s+href="/?brief\.html"\s+class="nav-link"[^>]*>[^<]*</a>', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    return content[:m.end()] + '\n<a href="/calls.html" class="nav-link">📞 Calls</a>', "ok_emoji"


def main():
    with report("wire_calls_nav") as r:
        r.heading("Add Calls tab to nav")
        results = {}
        for page in PAGES + ["calls.html"]:
            if not os.path.exists(page):
                results[page] = "MISSING"
                continue
            with open(page) as f:
                content = f.read()
            for fn in [patch_modern, patch_emoji, patch_topnav]:
                new, status = fn(content, page)
                if status:
                    if status.startswith("ok") and new != content:
                        with open(page, "w") as f:
                            f.write(new)
                    results[page] = status
                    break
            if page not in results:
                results[page] = "no_match"
        ok = sum(1 for v in results.values() if v.startswith("ok"))
        r.log(f"  patched: {ok}")
        for p, s in sorted(results.items()):
            r.log(f"    {p:25s}  {s}")


if __name__ == "__main__":
    main()
