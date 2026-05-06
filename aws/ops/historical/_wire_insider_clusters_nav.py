"""
Wire 'Clusters' link to insider-clusters.html across canonical pages.
Verify the new page is reachable on GitHub Pages.
"""
import os, re, time, urllib.request

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


# Define an authoritative set of pages where 'Clusters' should appear.
# These are the canonical sidebar pages
CANONICAL_PAGES = [
    "index.html",
    "desk.html",
    "brief.html",
    "calls.html",
    "performance.html",
    "sizing.html",
    "backtest.html",
    "weights.html",
    "horizons.html",
    "themes.html",
    "nobrainers.html",
    "insiders.html",
    "13f.html",
    "accuracy.html",
    "allocator.html",
    "sectors.html",
    "momentum.html",
    "news.html",
    "research.html",
    "vol.html",
    "ticker.html",
    "today.html",
    "feedback.html",
]


def patch_page(path):
    if not os.path.exists(path):
        return None, "not_found"
    with open(path, "r", encoding="utf-8") as f:
        s = f.read()

    # Skip if already has clusters link
    if "insider-clusters.html" in s:
        return s, "already_wired"

    # Find best insertion point — after nobrainers.html link if present
    patterns = [
        # most pages: after nobrainers
        (r'(<a href="/nobrainers\.html">[^<]+</a>\s*)',
         r'\1<a href="/insider-clusters.html">Clusters</a>\n  '),
        # uppercase NOBRAINERS pages (index)
        (r'(<a href="/nobrainers\.html">NOBRAINERS</a>\s*)',
         r'\1\n    <a href="/insider-clusters.html">CLUSTERS</a>'),
        # if no nobrainers link — fall back to after insiders
        (r'(<a href="/insiders\.html"[^>]*>[^<]+</a>\s*)',
         r'\1<a href="/insider-clusters.html">Clusters</a>\n  '),
    ]
    for pat, rep in patterns:
        new = re.sub(pat, rep, s, count=1)
        if new != s:
            with open(path, "w", encoding="utf-8") as f:
                f.write(new)
            return new, "patched"

    return None, "no_anchor"


def main():
    section("1) Wire Clusters into canonical pages")
    patched = 0
    skipped = 0
    failed = 0
    for p in CANONICAL_PAGES:
        _, status = patch_page(p)
        if status == "patched":
            log(f"  ✓ {p}")
            patched += 1
        elif status == "already_wired":
            log(f"  - {p}: already wired")
            skipped += 1
        elif status == "not_found":
            log(f"  ⚠ {p}: not found")
            failed += 1
        else:
            log(f"  ❌ {p}: {status}")
            failed += 1
    log("")
    log(f"  patched: {patched}  skipped: {skipped}  failed: {failed}")

    section("2) Verify reachability (after deploy)")
    # the Action that runs this won't have the deploy effects yet
    # but at least we can confirm the file is in repo
    for f in ["insider-clusters.html", "insiders.html"]:
        if os.path.exists(f):
            sz = os.path.getsize(f)
            log(f"  {f}: {sz:,}b in repo")

    # Also try live curl
    section("3) Live curl from inside Action")
    for url in [
        "https://justhodl.ai/insider-clusters.html",
        "https://justhodl.ai/insiders.html",
        "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/insider-clusters.json",
    ]:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                log(f"  {r.status}  {r.headers.get('Content-Length','?'):>10}b  {url}")
        except Exception as e:
            log(f"  ❌ {url}: {e}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "wire_insider_clusters_nav.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
