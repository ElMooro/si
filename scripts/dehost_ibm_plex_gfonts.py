#!/usr/bin/env python3
"""Removes IBM+Plex+Mono / IBM+Plex+Sans specifically from Google Fonts
<link> tags sitewide, now that they're self-hosted via jh-theme.css. Any
OTHER font families a legacy page still requests (Fraunces, Inter, etc.)
are left completely untouched — this is scoped ONLY to the two canonical
families, not a general font-hosting rewrite of every historical page.
If a link's family list becomes empty, the whole <link> (and its sibling
preconnects, if now unused by any remaining Google Fonts link) is removed.
"""
import glob, re, sys

LINK_RE = re.compile(r'<link[^>]*href="(https://fonts\.googleapis\.com/css2\?[^"]*)"[^>]*>')
FAMILY_RE = re.compile(r'family=([^&]*)')
DROP = ("IBM+Plex+Mono", "IBM+Plex+Sans")


def strip_plex(url):
    base, _, query = url.partition("?")          # split the URL prefix off FIRST —
    parts = query.split("&")                       # otherwise part[0] is "prefix+family=X"
    kept = []                                      # and never matches "starts with family="
    for p in parts:
        m = FAMILY_RE.match(p)
        if m and m.group(1).split(":")[0] in DROP:
            continue
        kept.append(p)
    remaining_families = [p for p in kept if p.startswith("family=")]
    return base + "?" + "&".join(kept), bool(remaining_families)


def process(path):
    s = open(path, encoding="utf-8", errors="replace").read()
    orig = s
    any_plex_removed = False
    for full_tag, url in [(m.group(0), m.group(1)) for m in LINK_RE.finditer(s)]:
        if not any(d in url for d in DROP):
            continue
        any_plex_removed = True
        new_url, has_remaining = strip_plex(url)
        if has_remaining:
            new_tag = full_tag.replace(url, new_url)
            s = s.replace(full_tag, new_tag, 1)
        else:
            s = s.replace(full_tag, "", 1)
    if any_plex_removed:
        # if NO google fonts <link> remains at all, the preconnect hints are now dead weight
        if "fonts.googleapis.com/css2" not in s:
            s = re.sub(r'<link[^>]*fonts\.g(?:oogleapis|static)\.com[^>]*>\s*', "", s)
    if s != orig:
        open(path, "w", encoding="utf-8").write(s)
        return True
    return False


def main(build_dir="."):
    changed = 0
    for f in glob.glob(f"{build_dir}/*.html"):
        if process(f):
            changed += 1
    print(f"de-hosted IBM Plex Google-Fonts refs on {changed} pages")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
