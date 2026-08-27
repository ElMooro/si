"""ops 5021 — stale-doc resilience live verification.

Ships with the why.html change that makes every stale-gated layer
render directly from the LIVE refresh response (the Worker's 10-min
bucket ignores cache-busting params, and the old 26x12s poll died
before the bucket could turn over; a failed first CDN fetch also left
bare skeletons). This op: G0 repo markers; P1 polls the served page
until the new markers are live; P2 confirms the S3 TSLA doc is on the
current schema so the reported ticker is closed end-to-end.
"""
import json
import re
import time
import urllib.request

import boto3

from ops_report import report

S3B = "justhodl-dashboard-live"
PAGE = "https://justhodl.ai/why.html"
s3 = boto3.client("s3", region_name="us-east-1")

with report("ops_5021_resilience_gate") as rep:
    rep.heading("ops 5021 -- LIVE-direct stale resilience gate")

    rep.section("G0 repo markers")
    src = open("why.html", encoding="utf-8").read()
    ok = True
    for name, pat, want in (
            ("poll window 60", r"if\(_pt>60\)", 6),
            ("recovery catch", r"First fetch failed", 6),
            ("LIVE-direct render (stale + recovery)",
             r"&refresh=1'\)\.then\(", 12),
            ("5010/5011 run-binding",
             r"\{DOC=nd;run\(DOC\);\}\}\)\.catch", 4),
    ):
        n = len(re.findall(pat, src))
        (rep.ok if n == want else rep.fail)(
            "%s: %d/%d" % (name, n, want))
        ok = ok and (n == want)
    if not ok:
        raise SystemExit("repo markers wrong")

    rep.section("P1 served page carries the resilience layer")
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36")
    CANDIDATES = [PAGE, "https://elmooro.github.io/si/why.html"]
    live_ok = False
    for i in range(16):
        try:
            url = CANDIDATES[i % len(CANDIDATES)]
            req = urllib.request.Request(
                url + "?v=%d" % time.time(), headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                page = r.read().decode("utf-8", "replace")
            if page.count("if(_pt>60)") == 6 and \
                    page.count("First fetch failed") == 6:
                rep.ok("served page live after %ds via %s (%d KB)"
                       % (i * 20, url.split("/")[2],
                          len(page) // 1024))
                live_ok = True
                break
        except Exception as e:
            rep.log("poll %d: %s" % (i, e))
        time.sleep(20)
    if not live_ok:
        raise SystemExit("served page did not pick up the change")

    rep.section("P2 TSLA doc on current schema in S3")
    obj = s3.get_object(Bucket=S3B, Key="equity-research/TSLA.json")
    doc = json.loads(obj["Body"].read())
    rep.kv(tsla_schema=doc.get("schema_version"),
           tsla_gfx=bool((doc.get("gf_extras") or {}).get("available")),
           tsla_kb=obj["ContentLength"] // 1024,
           modified=str(obj["LastModified"])[:19])
    if doc.get("schema_version") != "2.9.3":
        raise SystemExit("TSLA S3 doc not on 2.9.3")
    rep.ok("TSLA closed end-to-end: fresh doc in S3 + page that "
           "renders straight from LIVE on any stale or failed fetch")
