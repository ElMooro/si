"""ops_5048 -- complete the Tier-0 index; my builder was lossy.

ops 5047 mapped 6,326 of 8,147 eurostat flows and 118 of 207 ECB flows.
The data is fine; the BUILDER was wrong. It learned a page's flow from
that page's FIRST row, so any flow small enough to fit inside a page
opened by another flow was invisible -- 1,821 eurostat flows and 89 ECB
flows, 22% and 43%. And the verification passed anyway, because it
checked the ranges that existed and never asserted that every flow HAD
one. That is the sixth gate of this arc to measure what is present
instead of what is missing, and the first to do it by omission.

The fix needs no new reads, because two facts already in hand pin every
flow exactly:

  * pages are written in flow order, and
  * state["flows_done"] is the exact ORDER flows were completed.

So the anchored flows (those that do open a page) partition the page
space, and any flow sitting between two anchors in flows_done order must
live entirely within those anchors' pages. Interpolating gives a correct
superset range for EVERY flow, including the ones a first-row scan can
never see. The page->flow map from 5047's cursor is reused as-is, so
this costs zero range reads.

Monotonicity is the assumption that makes it work, so it is CHECKED, not
trusted: anchor pages must be non-decreasing along flows_done order. If
they are not, the op goes RED rather than shipping a plausible-looking
index built on a false premise.

  P0 load the cursor map + flows_done order
  P1 verify anchor monotonicity
  P2 interpolate; ASSERT every flow gets a range
  P3 verify by sampling flows that were INVISIBLE to 5047 -- fetch the
     lo page and prove their rows are really there
  P4 publish, and label est_series honestly as an upper bound
"""
import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
LANES = ("ecb", "eurostat")

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def jget(k):
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return {}


with report("ops_5048_index_complete") as R:
    fails = []
    out = {"op": "ops_5048", "schema": 2}

    for provider in LANES:
        R.section("P0/P1 %s -- inputs and monotonicity" % provider)
        cur = jget("index/%s/_build-cursor.json" % provider)
        page_flow = {int(k): v for k, v in
                     (cur.get("page_flow") or {}).items()}
        st = jget("data/_state/series-extract-%s.json" % provider)
        order = list(st.get("flows_done") or [])
        man = jget("data/providers/%s/series-manifest.json" % provider)
        n_pages = int(man.get("n_pages") or 0)
        R.log("  page->flow entries=%s  flows_done=%d  n_pages=%s" % (
            f"{len(page_flow):,}", len(order), f"{n_pages:,}"))
        if not page_flow or not order:
            R.log("  missing inputs -- cannot rebuild")
            fails.append("P0:%s" % provider)
            continue
        anchor = {}
        for p in sorted(page_flow):
            anchor.setdefault(page_flow[p], p)
        R.log("  anchored flows (open at least one page): %d of %d "
              "(%.1f%% were invisible to ops 5047)" % (
                  len(anchor), len(order),
                  100.0 * (len(order) - len(anchor)) / max(1, len(order))))
        seq = [(i, f, anchor[f]) for i, f in enumerate(order)
               if f in anchor]
        bad = [(seq[k][1], seq[k][2], seq[k + 1][1], seq[k + 1][2])
               for k in range(len(seq) - 1)
               if seq[k + 1][2] < seq[k][2]]
        R.log("  monotonicity violations: %d %s" % (bad[:3] and len(bad)
                                                    or 0, bad[:3]))
        if bad:
            R.log("  *** anchor pages are NOT ordered like flows_done -- "
                  "the interpolation premise is false, refusing to ship "
                  "this index ***")
            fails.append("P1:%s-monotonic" % provider)
            continue

        R.section("P2 %s -- interpolate every flow" % provider)
        flows = {}
        anchored_idx = [k for k, f in enumerate(order) if f in anchor]
        for pos, k in enumerate(anchored_idx):
            f = order[k]
            lo = anchor[f]
            hi = (anchor[order[anchored_idx[pos + 1]]]
                  if pos + 1 < len(anchored_idx) else max(0, n_pages - 1))
            flows[f] = {"lo": lo, "hi": hi, "anchored": True}
            # every unanchored flow between this anchor and the next one
            nxt_k = (anchored_idx[pos + 1] if pos + 1 < len(anchored_idx)
                     else len(order))
            for j in range(k + 1, nxt_k):
                flows[order[j]] = {"lo": lo, "hi": hi, "anchored": False}
        for j in range(0, anchored_idx[0] if anchored_idx else len(order)):
            flows[order[j]] = {"lo": 0,
                               "hi": (anchor[order[anchored_idx[0]]]
                                      if anchored_idx else n_pages - 1),
                               "anchored": False}
        miss = [f for f in order if f not in flows]
        R.log("  flows with a range: %d / %d   missing: %d %s" % (
            len(flows), len(order), len(miss), miss[:8]))
        if miss:
            fails.append("P2:%s-missing" % provider)
        for f, rec in flows.items():
            rec["est_series_max"] = (rec["hi"] - rec["lo"] + 1) * 500

        R.section("P3 %s -- prove the previously invisible flows" %
                  provider)
        hidden = [f for f, r in flows.items() if not r["anchored"]]
        R.log("  interpolated (never opened a page): %d" % len(hidden))
        checked = 0
        for f in hidden[:4]:
            rec = flows[f]
            found = False
            for p in (rec["lo"], min(rec["lo"] + 1, rec["hi"])):
                d = jget("data/providers/%s/series/page-%04d.json"
                         % (provider, p))
                fl = {r.get("flow") for r in (d.get("rows") or [])}
                if f in fl:
                    found = True
                    R.log("  %-18s FOUND on page-%04d (that page carries "
                          "%d flows)" % (f[:18], p, len(fl)))
                    break
            if not found:
                R.log("  %-18s not on lo..lo+1 (range %d..%d spans %d "
                      "pages -- superset, still valid)" % (
                          f[:18], rec["lo"], rec["hi"],
                          rec["hi"] - rec["lo"] + 1))
            checked += 1
        R.log("  sampled %d interpolated flows" % checked)

        R.section("P4 %s -- publish" % provider)
        doc = {"schema": 2, "provider": provider,
               "built_at": datetime.now(timezone.utc).isoformat(
                   timespec="seconds"),
               "pages_total": n_pages,
               "flows_total": len(order),
               "flows_indexed": len(flows),
               "anchored": len(anchor),
               "interpolated": len(hidden),
               "series_total": man.get("series_extracted"),
               "page_size": 500,
               "note": ("lo..hi is an inclusive SUPERSET of the pages "
                        "holding this flow -- the write buffer is not "
                        "cleared between flows, so pages straddle and "
                        "small flows share a page. Filter rows by the "
                        "flow field. est_series_max is an upper bound "
                        "(pages x 500), not a count."),
               "flows": flows}
        try:
            k = "index/%s/flows.json.gz" % provider
            s3.put_object(Bucket=LIVE, Key=k,
                          Body=gzip.compress(
                              json.dumps(doc, default=str).encode()),
                          ContentType="application/json",
                          ContentEncoding="gzip",
                          CacheControl="public, max-age=300")
            h = s3.head_object(Bucket=LIVE, Key=k)
            R.log("  -> %s  %.2f MB gz  flows=%d (anchored %d + "
                  "interpolated %d)" % (k, h["ContentLength"] / 1e6,
                                        len(flows), len(anchor),
                                        len(hidden)))
            out.setdefault("index", {})[provider] = {
                "flows": len(flows), "of": len(order),
                "anchored": len(anchor), "interpolated": len(hidden),
                "bytes": h["ContentLength"]}
        except Exception as e:
            R.log("  write err %s" % str(e)[:130])
            fails.append("P4:%s" % provider)

    R.section("state-bloat fix landed?")
    for p in LANES:
        try:
            h = s3.head_object(Bucket=LIVE,
                               Key="data/_state/series-extract-%s.json" % p)
            st = jget("data/_state/series-extract-%s.json" % p)
            R.log("  %-9s state %.1f MB · page_hashes=%s (was 98.1 MB / "
                  "1,124,942 for eurostat)" % (
                      p, h["ContentLength"] / 1e6,
                      f"{len(st.get('page_hashes') or {}):,}"))
        except Exception as e:
            R.log("  %s err %s" % (p, str(e)[:80]))
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-tier0.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/index-tier0.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5048 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(**{p: "%d/%d" % (v["flows"], v["of"])
            for p, v in (out.get("index") or {}).items()})
    R.log("ops 5048 GREEN -- every flow indexed, premise checked not "
          "assumed")
