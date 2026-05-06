"""
Wire 'Clusters' into canonical sidebar nav — flexible regex that handles
class="tab" attribute and other variations seen in the codebase.
"""
import os, re, time, urllib.request

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


CANONICAL_PAGES = [
    "brief.html", "calls.html", "performance.html", "sizing.html",
    "backtest.html", "weights.html", "horizons.html",
    "themes.html", "nobrainers.html",
    "13f.html", "accuracy.html", "allocator.html", "sectors.html",
    "momentum.html", "news.html", "research.html", "vol.html", "ticker.html",
    "today.html", "feedback.html",
]


def patch_page(path):
    if not os.path.exists(path):
        return "not_found"
    with open(path, "r", encoding="utf-8") as f:
        s = f.read()

    # Skip if already has clusters link
    if "insider-clusters.html" in s:
        return "already_wired"

    # Find any <a> tag pointing to /nobrainers.html (regardless of attributes)
    # Use a more permissive pattern that captures the whole anchor element
    m = re.search(r'(<a\s+[^>]*href="/nobrainers\.html"[^>]*>[^<]+</a>)', s)
    if not m:
        # Fall back to insiders.html
        m = re.search(r'(<a\s+[^>]*href="/insiders\.html"[^>]*>[^<]+</a>)', s)
    if not m:
        return "no_anchor"

    full_anchor = m.group(1)
    # Replicate the same attribute structure for the new anchor
    # Extract class= attribute if present
    cls_match = re.search(r'class="([^"]+)"', full_anchor)
    cls = f' class="{cls_match.group(1)}"' if cls_match else ""

    # Build the new anchor
    new_anchor = f'<a{cls} href="/insider-clusters.html">Clusters</a>'
    # If the original had a newline+indentation pattern, replicate it
    indent = ""
    idx = s.find(full_anchor)
    if idx >= 0:
        # Look back for indentation
        prev_nl = s.rfind("\n", 0, idx)
        if prev_nl >= 0:
            indent = s[prev_nl:idx]  # includes the newline

    insert = full_anchor + indent + new_anchor
    new = s.replace(full_anchor, insert, 1)
    if new == s:
        return "replace_failed"

    with open(path, "w", encoding="utf-8") as f:
        f.write(new)
    return "patched"


def main():
    section("1) Wire Clusters into canonical pages (flexible regex)")
    counts = {"patched": 0, "already_wired": 0, "not_found": 0, "no_anchor": 0, "replace_failed": 0}
    for p in CANONICAL_PAGES:
        s = patch_page(p)
        sym = {"patched": "✓", "already_wired": "-", "not_found": "⚠", "no_anchor": "❌", "replace_failed": "❌"}[s]
        log(f"  {sym} {p}: {s}")
        counts[s] += 1
    log("")
    log(f"  patched: {counts['patched']}  already: {counts['already_wired']}  no_anchor: {counts['no_anchor']}  not_found: {counts['not_found']}")

    section("2) Spot-check: verify nav additions")
    for p in CANONICAL_PAGES[:5]:
        if not os.path.exists(p):
            continue
        with open(p, "r") as f:
            s = f.read()
        has_clusters = "/insider-clusters.html" in s
        log(f"  {p}: clusters_link={has_clusters}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "wire_insider_clusters_nav_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
