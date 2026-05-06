"""
Wire compound-signals, deep-value, and eps-velocity links into canonical sidebar nav
across all main pages. Uses flexible regex that handles class= attribute variants.
"""
import os, re, time, urllib.request

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


# Pages where the new links should appear
CANONICAL_PAGES = [
    "index.html", "desk.html", "brief.html", "calls.html", "performance.html",
    "sizing.html", "backtest.html", "weights.html", "horizons.html",
    "themes.html", "nobrainers.html", "insider-clusters.html", "insiders.html",
    "smart-money.html", "13f.html", "accuracy.html", "allocator.html",
    "sectors.html", "momentum.html", "news.html", "research.html",
    "vol.html", "ticker.html", "today.html", "feedback.html",
]

# Each new page → label
NEW_LINKS = [
    ("/compound-signals.html", "Compound"),
    ("/deep-value.html", "Deep Value"),
    ("/eps-velocity.html", "EPS Velocity"),
]


def patch_page(path):
    if not os.path.exists(path):
        return "not_found"
    with open(path, "r", encoding="utf-8") as f:
        s = f.read()

    added = []
    for href, label in NEW_LINKS:
        if href in s:
            continue  # already wired

        # Try to find an anchor pointing to /insider-clusters.html (most pages have it now)
        pattern = re.search(r'(<a\s+[^>]*href="/insider-clusters\.html"[^>]*>[^<]+</a>)', s)
        if not pattern:
            # Fall back to /nobrainers.html
            pattern = re.search(r'(<a\s+[^>]*href="/nobrainers\.html"[^>]*>[^<]+</a>)', s)
        if not pattern:
            # Fall back to /smart-money.html
            pattern = re.search(r'(<a\s+[^>]*href="/smart-money\.html"[^>]*>[^<]+</a>)', s)
        if not pattern:
            continue  # skip this link for this page

        full_anchor = pattern.group(1)
        # Extract class= attribute if present, replicate it
        cls_match = re.search(r'class="([^"]+)"', full_anchor)
        # Drop "active" from the class to avoid making the new link "active"
        if cls_match:
            cls = re.sub(r'\s*active\s*', ' ', cls_match.group(1)).strip()
            cls_attr = f' class="{cls}"' if cls else ""
        else:
            cls_attr = ""

        # Detect uppercase style (e.g. NOBRAINERS) — use uppercase label if pattern matches
        anchor_text = re.search(r'>([^<]+)<', full_anchor).group(1) if re.search(r'>([^<]+)<', full_anchor) else label
        if anchor_text.isupper():
            new_label = label.upper()
        else:
            new_label = label

        new_anchor = f'<a{cls_attr} href="{href}">{new_label}</a>'

        # Find the indentation pattern of the previous anchor (from line start to anchor)
        idx = s.find(full_anchor)
        if idx >= 0:
            prev_nl = s.rfind("\n", 0, idx)
            indent = s[prev_nl:idx] if prev_nl >= 0 else "\n  "
        else:
            indent = "\n  "

        insert = full_anchor + indent + new_anchor
        new = s.replace(full_anchor, insert, 1)
        if new != s:
            s = new
            added.append(label)

    if added:
        with open(path, "w", encoding="utf-8") as f:
            f.write(s)
        return f"patched ({', '.join(added)})"
    else:
        # Did all links already exist?
        all_present = all(href in s for href, _ in NEW_LINKS)
        return "all_already_present" if all_present else "no_anchor_found"


def main():
    section("1) Wire compound, deep-value, eps-velocity into canonical pages")
    counts = {"patched": 0, "all_already_present": 0, "no_anchor_found": 0, "not_found": 0}
    for p in CANONICAL_PAGES:
        s = patch_page(p)
        if s.startswith("patched"):
            counts["patched"] += 1
            sym = "✓"
        elif s == "all_already_present":
            counts["all_already_present"] += 1
            sym = "-"
        elif s == "no_anchor_found":
            counts["no_anchor_found"] += 1
            sym = "❌"
        else:
            counts["not_found"] += 1
            sym = "⚠"
        log(f"  {sym} {p}: {s}")
    log("")
    log(f"  patched: {counts['patched']}  all_present: {counts['all_already_present']}  no_anchor: {counts['no_anchor_found']}  not_found: {counts['not_found']}")

    section("2) Verify new links present in spot-check pages")
    for p in ["brief.html", "desk.html", "calls.html", "themes.html", "nobrainers.html"]:
        if not os.path.exists(p):
            continue
        with open(p, "r") as f:
            content = f.read()
        cs = "compound-signals.html" in content
        dv = "deep-value.html" in content
        ev = "eps-velocity.html" in content
        log(f"  {p}: compound={cs} deep-value={dv} eps-velocity={ev}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "wire_new_pages_into_nav.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
