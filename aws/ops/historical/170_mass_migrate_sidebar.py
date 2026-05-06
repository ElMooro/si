#!/usr/bin/env python3
"""
Step 170 — Mass-migrate all HTML pages to the shared sidebar.

Step 169 categorized 48 pages:
  SAFE     11 (no existing nav, easy)
  NAV      26 (has existing nav, need to suppress)
  STUB     10 (≤10 lines, no content)
  MIGRATED  1 (desk-v2.html, skip)
  SIDEBAR   0

This step migrates all 47 remaining pages. For each:

  1. Inject sidebar.css <link> into <head> (after existing stylesheets,
     before closing </head>)
  2. Inject sidebar.js <script> before </body>
  3. Inject sidebar mount point + .jh-page wrapper around body content
  4. For NAV pages, inject suppression CSS that hides:
       body > nav, body > header, body > .topbar, body > .top-bar,
       body > .top-nav, .navbar (when not inside .jh-sidebar)
     — but ONLY top-of-body ones. Inner navs stay.
  5. Make script idempotent: skip files that already have sidebar.js

Uses BeautifulSoup for robust HTML parsing. Each file's edit:
  - Reads HTML
  - Parses with bs4
  - Mutates DOM tree
  - Writes back, preserving original whitespace where possible

Idempotent: re-running produces same output.

After this runs, every page has:
  - Same sidebar nav on the left
  - Same color tokens
  - Page content shifted right by 220px
  - Old nav hidden via CSS

The old nav HTML is NOT removed — it's just hidden. This is
intentional: less risk of breaking page-specific JavaScript that
might reference it. If Khalid wants to actually delete it later,
that's a follow-up cleanup.
"""
import json
import os
import re
import sys
import subprocess
from pathlib import Path

from ops_report import report

REPO = Path(__file__).resolve().parents[3]


def ensure_bs4():
    """Install bs4 if not present (CI runner)."""
    try:
        import bs4
        return bs4
    except ImportError:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            "--quiet", "--break-system-packages",
            "beautifulsoup4",
        ])
        import bs4
        return bs4


bs4 = ensure_bs4()
from bs4 import BeautifulSoup, NavigableString


SIDEBAR_CSS_LINK = '<link rel="stylesheet" href="/assets/sidebar.css">'
SIDEBAR_MOUNT = '<div id="sidebar"></div>'
SIDEBAR_SCRIPT = '<script src="/assets/sidebar.js"></script>'

# CSS that hides legacy top-of-body nav elements
SUPPRESS_LEGACY_NAV = """<style id="jh-suppress-legacy">
/* JustHodl: hide legacy page navs in favour of shared sidebar */
body > nav,
body > header,
body > .topbar,
body > .top-bar,
body > .top-nav,
body > .navbar,
body > .navigation,
.jh-page > nav:first-child,
.jh-page > header:first-child,
.jh-page > .topbar:first-child,
.jh-page > .top-bar:first-child,
.jh-page > .navbar:first-child{
  display: none !important;
}
/* Reset any body padding-top that was reserving space for fixed nav */
body{
  padding-top: 0 !important;
  margin: 0 !important;
}
</style>"""


def already_migrated(html: str) -> bool:
    return "/assets/sidebar.js" in html


def find_html_files():
    files = []
    for f in REPO.glob("*.html"):
        files.append(f)
    for f in REPO.glob("*/index.html"):
        if "aws" in f.parts:
            continue
        files.append(f)
    return sorted(files)


def is_stub(html: str) -> bool:
    return html.count("\n") + 1 <= 10


def migrate_html(html: str) -> tuple:
    """
    Mutate HTML to add the sidebar shell.
    Returns (new_html, action_summary).
    """
    if already_migrated(html):
        return html, "skip:already-migrated"

    # Use bs4 with the html5lib parser for robustness on messy HTML
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        return html, f"skip:parse-fail:{e}"

    head = soup.find("head")
    body = soup.find("body")

    if not body:
        return html, "skip:no-body"

    # Make sure <head> exists
    if not head:
        head = soup.new_tag("head")
        if soup.html:
            soup.html.insert(0, head)
        else:
            return html, "skip:no-head"

    # 1. Add sidebar.css link to <head>
    existing_link = head.find("link", attrs={"href": "/assets/sidebar.css"})
    if not existing_link:
        link_tag = soup.new_tag("link",
                                attrs={"rel": "stylesheet",
                                       "href": "/assets/sidebar.css"})
        head.append(link_tag)

    # 2. Add suppression CSS to <head>
    existing_suppress = head.find("style", id="jh-suppress-legacy")
    if not existing_suppress:
        # Use BeautifulSoup to parse just the style tag
        style_soup = BeautifulSoup(SUPPRESS_LEGACY_NAV, "html.parser")
        style_tag = style_soup.find("style")
        head.append(style_tag)

    # 3. Inject sidebar mount as first child of <body>
    existing_mount = body.find("div", id="sidebar")
    if not existing_mount:
        sidebar_div = soup.new_tag("div", id="sidebar")
        body.insert(0, sidebar_div)

    # 4. Wrap remaining body content (everything after the sidebar mount + script-skip)
    # in <div class="jh-page">. Skip elements that should stay outside the wrapper:
    #   - the sidebar div itself
    #   - <script src="/assets/sidebar.js"> (we add it later)
    existing_page = body.find("div", class_="jh-page")
    if not existing_page:
        page_div = soup.new_tag("div", attrs={"class": "jh-page"})

        # Collect all body children that aren't the sidebar mount
        children_to_wrap = []
        for child in list(body.children):
            if isinstance(child, NavigableString):
                children_to_wrap.append(child)
                continue
            if child.name == "div" and child.get("id") == "sidebar":
                continue
            if child.name == "script":
                src = child.get("src", "")
                if src.endswith("/assets/sidebar.js"):
                    continue
            children_to_wrap.append(child)

        # Move them into the page wrapper
        for child in children_to_wrap:
            page_div.append(child.extract())

        # Append wrapper to body (after the sidebar mount which is first child)
        body.append(page_div)

    # 5. Add sidebar.js as last child of body
    existing_script = body.find("script", attrs={"src": "/assets/sidebar.js"})
    if not existing_script:
        script_tag = soup.new_tag("script", attrs={"src": "/assets/sidebar.js"})
        body.append(script_tag)

    return str(soup), "migrated"


with report("mass_migrate_sidebar") as r:
    r.heading("Mass-migrate all HTML pages to shared sidebar")

    files = find_html_files()
    r.log(f"  Total HTML pages: {len(files)}")

    counts = {"migrated": 0, "skipped": 0, "stub": 0, "failed": 0}
    detail = []

    for path in files:
        rel = str(path.relative_to(REPO))
        try:
            html = path.read_text(encoding='utf-8')
        except Exception as e:
            r.warn(f"  read fail {rel}: {e}")
            counts["failed"] += 1
            continue

        # Stubs: skip — they're empty redirect pages
        if is_stub(html):
            counts["stub"] += 1
            detail.append((rel, "stub-skipped"))
            continue

        new_html, action = migrate_html(html)

        if action.startswith("skip:"):
            counts["skipped"] += 1
            detail.append((rel, action))
            continue

        if action == "migrated":
            try:
                path.write_text(new_html, encoding='utf-8')
                counts["migrated"] += 1
                detail.append((rel, "ok"))
            except Exception as e:
                counts["failed"] += 1
                detail.append((rel, f"write-fail:{e}"))

    r.section("Migration results")
    r.log(f"  Migrated: {counts['migrated']}")
    r.log(f"  Skipped:  {counts['skipped']}")
    r.log(f"  Stubs:    {counts['stub']}")
    r.log(f"  Failed:   {counts['failed']}")

    r.section("Per-file results")
    for rel, action in sorted(detail):
        marker = "✅" if action == "ok" else ("○" if action.startswith("skip") or action == "stub-skipped" else "❌")
        r.log(f"  {marker} {rel:48} {action}")

    r.kv(
        total=len(files),
        migrated=counts['migrated'],
        skipped=counts['skipped'],
        stubs=counts['stub'],
        failed=counts['failed'],
    )
    r.log("Done — every non-stub page should now have the shared sidebar")
