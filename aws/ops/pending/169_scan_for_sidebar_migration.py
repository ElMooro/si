#!/usr/bin/env python3
"""
Step 169 — SCAN PASS for site-wide sidebar migration.

User feedback: 'website is still a mess, needs A LOT OF WORK'.
1 of 48 pages has the new sidebar. To migrate the rest in one
mass commit, first I need to know what I'm dealing with.

This step:
  A. Inventories every HTML page (top-level + subdir index.html)
  B. Classifies each into one of 4 categories:
       SAFE     — no existing nav/header, easy injection
       NAV      — has existing top-bar nav (need to replace, not duplicate)
       SIDEBAR  — already has its own sidebar (manual review needed)
       STUB     — 1-line stubs / empty (skip, candidates for delete)
       MIGRATED — already has sidebar.js (skip, idempotent)
  C. For each, captures relevant metadata: title, line count, existing
     nav element if any, body classes, sticky/fixed positioning hints
  D. Reports back so I can decide migration strategy

Pure read-only diagnostic. No file changes. Result drives next step.
"""
import re
import json
from pathlib import Path

from ops_report import report

REPO = Path(__file__).resolve().parents[3]


def find_html_files():
    """Top-level *.html plus depth-2 */index.html (excluding aws/)."""
    files = []
    for f in REPO.glob("*.html"):
        files.append(f)
    for f in REPO.glob("*/index.html"):
        if "aws" in f.parts:
            continue
        files.append(f)
    return sorted(files)


def classify(html: str, path: Path):
    """Return (category, info_dict)."""
    info = {}
    line_count = html.count("\n") + 1
    info["lines"] = line_count

    # STUB: very short files
    if line_count <= 10:
        return "STUB", info

    # MIGRATED: already has sidebar.js
    if "/assets/sidebar.js" in html or 'src="/assets/sidebar.js"' in html:
        return "MIGRATED", info

    # title
    title_match = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
    info["title"] = title_match.group(1).strip() if title_match else "(no title)"

    # body classes / styles
    body_match = re.search(r"<body([^>]*)>", html, re.IGNORECASE)
    info["body_attrs"] = body_match.group(1).strip() if body_match else ""

    # Existing nav?
    has_nav = bool(re.search(r"<nav[\s>]", html, re.IGNORECASE))
    has_header = bool(re.search(r"<header[\s>]", html, re.IGNORECASE))
    has_sidebar_class = bool(re.search(r'class="[^"]*sidebar', html, re.IGNORECASE))
    has_navigation_div = bool(re.search(r'class="[^"]*navigation', html, re.IGNORECASE))
    has_topbar = bool(re.search(r'class="[^"]*topbar|class="[^"]*top-bar', html, re.IGNORECASE))

    info["has_nav"] = has_nav
    info["has_header"] = has_header
    info["has_sidebar_class"] = has_sidebar_class
    info["has_navigation_div"] = has_navigation_div
    info["has_topbar"] = has_topbar

    # Try to find the existing nav structure for context
    if has_nav:
        nav_match = re.search(r"<nav[^>]*>([\s\S]{0,200})", html, re.IGNORECASE)
        info["nav_preview"] = (nav_match.group(0)[:120] + "...") if nav_match else ""

    # Sidebar in CSS or HTML
    if has_sidebar_class:
        return "SIDEBAR", info

    # Has its own nav/header
    if has_nav or has_header or has_topbar:
        return "NAV", info

    return "SAFE", info


with report("scan_for_sidebar_migration") as r:
    r.heading("SCAN PASS — classify all HTML pages for migration")

    files = find_html_files()
    r.log(f"  Total HTML pages found: {len(files)}")

    by_category = {"SAFE": [], "NAV": [], "SIDEBAR": [], "STUB": [], "MIGRATED": []}
    inventory = []

    for path in files:
        try:
            html = path.read_text(encoding='utf-8')
        except Exception as e:
            r.warn(f"  read fail {path}: {e}")
            continue

        category, info = classify(html, path)
        rel = str(path.relative_to(REPO))
        info["path"] = rel
        info["category"] = category
        by_category[category].append(rel)
        inventory.append(info)

    r.section("Categorization summary")
    r.log(f"  STUB     (≤10 lines, can delete):   {len(by_category['STUB']):>3}")
    r.log(f"  MIGRATED (already has sidebar):     {len(by_category['MIGRATED']):>3}")
    r.log(f"  SAFE     (no existing nav):         {len(by_category['SAFE']):>3}")
    r.log(f"  NAV      (has top-nav, replace):    {len(by_category['NAV']):>3}")
    r.log(f"  SIDEBAR  (has sidebar, review):     {len(by_category['SIDEBAR']):>3}")

    r.section("STUBs (delete candidates)")
    for f in sorted(by_category["STUB"]):
        info = next(x for x in inventory if x["path"] == f)
        r.log(f"  {f:50} {info['lines']:>5} lines")

    r.section("MIGRATED")
    for f in sorted(by_category["MIGRATED"]):
        info = next(x for x in inventory if x["path"] == f)
        r.log(f"  ✅ {f:50} {info['lines']:>5} lines")

    r.section("SAFE (auto-migrate, no risk)")
    for f in sorted(by_category["SAFE"]):
        info = next(x for x in inventory if x["path"] == f)
        title = (info.get("title", "") or "")[:50]
        r.log(f"  {f:42} ({info['lines']:>4}L) {title}")

    r.section("NAV (auto-migrate, replace existing nav)")
    for f in sorted(by_category["NAV"]):
        info = next(x for x in inventory if x["path"] == f)
        title = (info.get("title", "") or "")[:45]
        flags = []
        if info.get("has_nav"): flags.append("nav")
        if info.get("has_header"): flags.append("header")
        if info.get("has_topbar"): flags.append("topbar")
        r.log(f"  {f:42} ({info['lines']:>4}L) [{','.join(flags):20}] {title}")

    r.section("SIDEBAR (manual review)")
    for f in sorted(by_category["SIDEBAR"]):
        info = next(x for x in inventory if x["path"] == f)
        title = (info.get("title", "") or "")[:45]
        r.log(f"  {f:42} ({info['lines']:>4}L) {title}")

    # Write inventory to disk for next step to consume
    out_path = REPO / "aws" / "ops" / "site" / "inventory.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(inventory, indent=2))
    r.log(f"\n  Inventory written to {out_path.relative_to(REPO)}")

    r.kv(
        total=len(files),
        safe=len(by_category["SAFE"]),
        nav=len(by_category["NAV"]),
        sidebar=len(by_category["SIDEBAR"]),
        stub=len(by_category["STUB"]),
        migrated=len(by_category["MIGRATED"]),
    )
    r.log("Done")
