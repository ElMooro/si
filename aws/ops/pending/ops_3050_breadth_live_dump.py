#!/usr/bin/env python3
"""ops 3050 -- breadth-thrust page: dump the FULL live JSON payload
(2.8KB) + fingerprint the live-served HTML vs repo (bake steps mutate
pages in _site; live may differ). Evidence for exact client-side
reproduction in the sandbox."""
import hashlib
import json
import re
import sys
import urllib.request

from ops_report import report


def get(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 ops-3050"})
    return urllib.request.urlopen(req, timeout=25).read()


with report("3050_breadth_live_dump") as rep:
    rep.section("1. Full live payload")
    body = get("https://justhodl.ai/data/breadth-thrust.json?cb=3050")
    rep.log("PAYLOAD>>>" + body.decode("utf-8", "replace") + "<<<END")

    rep.section("2. Live page fingerprint")
    page = get("https://justhodl.ai/breadth-thrust.html?cb=3050"
               ).decode("utf-8", "replace")
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", page, re.S)
    inline = [s for s in scripts if s.strip()]
    rep.kv(page_bytes=len(page), n_script_tags=len(scripts),
           n_inline=len(inline),
           has_build="function build(d)" in page,
           has_urlbase="URL_BASE" in page,
           title_ok="Breadth Thrust" in page)
    main = max(inline, key=len) if inline else ""
    rep.kv(main_script_sha=hashlib.sha256(
        main.encode()).hexdigest()[:16],
        main_script_len=len(main))
    # first 300 chars of any script that is NOT the main one (injected?)
    for i, s in enumerate(inline):
        if s is not main and len(s) > 40:
            rep.log("extra_script_%d_head: %s" % (i, s[:200]))
    rep.log("done")
    if False:
        sys.exit(1)
sys.exit(0)
