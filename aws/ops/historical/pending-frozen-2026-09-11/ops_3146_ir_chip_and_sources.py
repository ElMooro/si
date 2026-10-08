"""ops 3146 — IR quadrant chip verify + 3145-warn source audit.

Two small closures:
  1. industry-rotation.html gained a per-row divergence chip
     (STEALTH / DISTRO / CAPIT with z-vs-ret tooltip). CDN marker check
     (IR_QCHIP_V1) — warn-only, GH Pages cache self-heals <=10 min.
  2. 3145 warned "C: no overlay hits on today's top names — verify
     sources non-empty". Prove overlap-zero is legitimate: count
     kill-theses tickers and squeeze board tickers, intersect with
     master-ranker top-25, and print the sets.
"""

import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3146_ir_chip_and_sources") as rep:
    fails, warns = [], []
    rep.heading("ops 3146 — IR quadrant chip + overlay-source audit")

    rep.section("1. Overlay sources vs master-ranker top names")
    kt = s3_json("data/kill-theses.json")
    kt_tks = sorted({str(t.get("ticker") or "").upper()
                     for t in kt.get("theses") or [] if t.get("ticker")})
    sq = s3_json("data/squeeze-fuel.json")
    sq_rows = (sq.get("board") or sq.get("rows") or sq.get("items") or [])
    sq_tks = sorted({str(r.get("ticker") or "").upper()
                     for r in sq_rows if isinstance(r, dict)})[:80]
    mr = s3_json("data/master-ranker.json")
    tops = (mr.get("top_tickers") or mr.get("ranked")
            or mr.get("leaderboard") or [])
    mr_tks = sorted({str(t.get("ticker") or "").upper() for t in tops})
    rep.kv(kill_theses_n=len(kt_tks), squeeze_n=len(sq_tks),
           mr_top_n=len(mr_tks),
           kill_overlap=len(set(kt_tks) & set(mr_tks)),
           squeeze_overlap=len(set(sq_tks) & set(mr_tks)))
    rep.log(f"kill-theses tickers: {', '.join(kt_tks[:20])}")
    rep.log(f"master-ranker top:   {', '.join(mr_tks[:25])}")
    if not kt_tks:
        fails.append("kill-theses feed EMPTY — premortem engine needs a look")
    elif not set(kt_tks) & set(mr_tks):
        rep.ok("kill overlap 0 is legitimate: premortem covers "
               f"{len(kt_tks)} best-ideas names, disjoint from today's "
               "ranked leaders — overlay fires when universes cross")
    if not sq_tks:
        fails.append("squeeze-fuel board EMPTY")
    elif not set(sq_tks) & set(mr_tks):
        rep.ok("squeeze overlap 0 legitimate: high-SI board disjoint "
               "from quality leaders today")

    rep.section("2. IR page chip on CDN (warn-only)")
    seen = False
    for _ in range(3):
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/industry-rotation.html?t={int(time.time())}",
                headers={"User-Agent": "Mozilla/5.0 ops-3146",
                         "Cache-Control": "no-cache"})
            html = urllib.request.urlopen(req, timeout=15).read().decode(
                "utf-8", "replace")
            if "IR_QCHIP_V1" in html:
                seen = True
                break
        except Exception:
            pass
        time.sleep(25)
    if seen:
        rep.ok("industry-rotation.html serves the quadrant chip build")
    else:
        warns.append("CDN still on prior page — max-age=600 self-heals; "
                     "chip data is already in the feed either way")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
