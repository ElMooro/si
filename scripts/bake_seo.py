#!/usr/bin/env python3
"""bake_seo.py <site_dir> -- external-audit SEO layer (2026-07-10).
Runs against _site per PAGES.YML DIRECTORY DOCTRINE. Per page:
canonical link, og:title/og:url/og:description, meta description
(derived from <title> when absent). Also regenerates sitemap.xml from
the actual page set every deploy (the static one froze at 344).
Ops dashboards keep their noindex and stay out of the sitemap."""
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SITE = Path(sys.argv[1] if len(sys.argv) > 1 else "_site")
BASE = "https://justhodl.ai"
NOINDEX = {"errors.html", "observability.html", "system.html",
           "dep-graph.html", "fleet-health.html", "404.html",
           "redirect.html"}
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
locs, touched = [], 0
for f in sorted(SITE.glob("*.html")):
    try:
        h = f.read_text(encoding="utf-8", errors="replace")
    except Exception:
        continue
    m = re.search(r"<title>(.*?)</title>", h, re.S)
    title = re.sub(r"\s+", " ", m.group(1)).strip() if m else f.stem
    url = "%s/%s" % (BASE, f.name)
    add = []
    if 'rel="canonical"' not in h:
        add.append('<link rel="canonical" href="%s">' % url)
    if 'property="og:title"' not in h:
        add.append('<meta property="og:title" content="%s">'
                   % title.replace('"', "'")[:120])
        add.append('<meta property="og:url" content="%s">' % url)
        add.append('<meta property="og:type" content="website">')
    if 'name="description"' not in h:
        desc = ("%s -- institutional market intelligence on "
                "JustHodl.AI." % title.split("|")[0].split(
                    "\u00b7")[0].strip())[:158]
        add.append('<meta name="description" content="%s">'
                   % desc.replace('"', "'"))
        add.append('<meta property="og:description" content="%s">'
                   % desc.replace('"', "'"))
    if add and "</title>" in h:
        h = h.replace("</title>", "</title>\n" + "\n".join(add), 1)
        f.write_text(h, encoding="utf-8")
        touched += 1
    if (f.name not in NOINDEX
            and 'content="noindex' not in h):
        locs.append(url)
sm = ['<?xml version="1.0" encoding="UTF-8"?>',
      '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
for u in locs:
    sm.append("<url><loc>%s</loc><lastmod>%s</lastmod></url>"
              % (u, today))
sm.append("</urlset>")
(SITE / "sitemap.xml").write_text("\n".join(sm), encoding="utf-8")
print("[bake_seo] %d pages touched, sitemap %d urls"
      % (touched, len(locs)))
