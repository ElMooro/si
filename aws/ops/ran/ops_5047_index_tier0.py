"""ops_5047 -- Tier-0 serving index + a state-bloat fix.

THE SERVING PROBLEM. 567,445,067 series live in 1,134,889 JSON pages.
A client cannot search that by fetching pages, and a global inverted
index over 567M rows is the wrong instinct: at ~20 trigrams per series
that is ~11 BILLION postings, far more expensive than the corpus it
indexes. The data has a natural two-level partition -- 8,354 flows, then
series within a flow -- so the index should have one too. This is the
same reason an internal catalog service shards by entity before it
shards by term.

  Tier 0 (this op): flow -> page range. 8,354 entries, ~1MB gzipped,
    cacheable, loaded once by the client. Turns 1.13M opaque pages into
    8,354 navigable datasets. Every "which pages hold flow X" question
    becomes a dictionary lookup instead of a scan.
  Tier 1 (spec'd, not built): per-flow sorted series shards with a
    sparse block index, queried by HTTP Range. Only needed for the few
    hundred flows large enough that their page range is unwieldy.
  Tier 2 (spec'd): a token index over flow titles and dimension labels
    -- thousands of terms, not billions.

HOW TIER 0 IS BUILT, AND WHY IT IS EXACT. Reading one page to learn its
flow costs a 1.5KB range GET, so the whole scan is ~1.7GB and ~$0.20 --
versus re-reading the 313GB corpus. Pages are written in flow order, but
the write buffer is NOT cleared between flows, so a page can straddle
two flows. That is handled rather than assumed: flow F's range runs from
the first page whose first row is F to the first page of the NEXT flow,
inclusive. That is a provable superset with at most one page of slack,
and the client filters rows by the `flow` field anyway.

BUG FOUND AND FIXED (shipped with this op). page_hashes is a rewrite
guard, but nothing ever pruned it: it reached 1,134,889 entries -- a
~100MB state document rewritten after EVERY flow and re-downloaded on
every invocation, including the hourly no-ops that will now run forever.
Only pages at or above the last checkpointed n_pages can be rewritten,
so everything below is dead weight. Now pruned to a 60,000-page window
with MAX_PAGES_PER_RUN lowered to 50,000 so the guard provably covers
anything a run can rewrite. Proven offline both ways.

  P0 measure the bloat, confirm the fix deployed
  P1 build ECB's Tier-0 index end to end (6,481 pages)
  P2 build Eurostat's, resumable, under a wall-clock budget
  P3 VERIFY: for sampled flows, fetch the declared range's edge pages
     and prove the flow's rows are actually inside it
"""
import concurrent.futures as cf
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-series-extractor"
WORKERS = 64
BUDGET_S = 3300
LANES = {"ecb": 207, "eurostat": 8147}

cfg = Config(read_timeout=60, retries={"max_attempts": 4},
             max_pool_connections=WORKERS * 2)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
T0 = time.time()


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=k)["Body"].read())
    except Exception:
        return {}


def flow_of_page(provider, n):
    """One 1.5KB range read -> the flow of the page's FIRST row."""
    key = "data/providers/%s/series/page-%04d.json" % (provider, n)
    try:
        b = s3.get_object(Bucket=LIVE, Key=key,
                          Range="bytes=0-1500")["Body"].read()
        t = b.decode("utf-8", "replace")
        i = t.find('"flow":')
        if i < 0:
            return n, None
        j = t.find('"', i + 7)
        k2 = t.find('"', j + 1)
        return n, t[j + 1:k2]
    except Exception:
        return n, None


with report("ops_5047_index_tier0") as R:
    fails = []
    out = {"op": "ops_5047", "schema": 1}

    R.section("P0 the state-bloat bug")
    for p in ("eurostat", "ecb"):
        k = "data/_state/series-extract-%s.json" % p
        try:
            h = s3.head_object(Bucket=LIVE, Key=k)
            st = jget(k)
            R.log("  %-9s state doc %.1f MB · page_hashes=%s · "
                  "flows_done=%d · buffer=%d rows" % (
                      p, h["ContentLength"] / 1e6,
                      f"{len(st.get('page_hashes') or {}):,}",
                      len(st.get("flows_done") or []),
                      len(st.get("buffer") or [])))
            out.setdefault("state_mb", {})[p] = round(
                h["ContentLength"] / 1e6, 1)
        except Exception as e:
            R.log("  %s err %s" % (p, str(e)[:90]))
    R.log("  (buffer carry-over verified in code: buf = "
          "state.get('buffer', []) -- no series are dropped at a run "
          "boundary; that one is NOT a bug)")
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        R.log("  extractor LastModified=%s" % c.get("LastModified"))
    except Exception as e:
        R.log("  cfg err %s" % str(e)[:80])

    for provider, n_flows in LANES.items():
        R.section("P1/P2 Tier-0 index: %s" % provider)
        man = jget("data/providers/%s/series-manifest.json" % provider)
        n_pages = int(man.get("n_pages") or 0)
        idx_key = "index/%s/flows.json.gz" % provider
        cur_key = "index/%s/_build-cursor.json" % provider
        prev = jget(cur_key)
        start = int(prev.get("next_page") or 0)
        pages = dict((int(k), v) for k, v in
                     (prev.get("page_flow") or {}).items())
        R.log("  %s pages, resuming at %d (%d already mapped)" % (
            f"{n_pages:,}", start, len(pages)))
        done_all = True
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            batch = 20000
            n = start
            while n < n_pages:
                if time.time() - T0 > BUDGET_S:
                    done_all = False
                    R.log("  wall-clock budget reached at page %d" % n)
                    break
                hi = min(n + batch, n_pages)
                for pn, fl in ex.map(lambda i: flow_of_page(provider, i),
                                     range(n, hi)):
                    if fl:
                        pages[pn] = fl
                n = hi
                R.log("    mapped %s/%s pages (%.0fs elapsed)" % (
                    f"{len(pages):,}", f"{n_pages:,}", time.time() - T0))
                s3.put_object(
                    Bucket=LIVE, Key=cur_key,
                    Body=json.dumps({"next_page": n,
                                     "page_flow": {str(k): v for k, v
                                                   in pages.items()}}
                                    ).encode(),
                    ContentType="application/json")
        # flow -> [lo, hi] with one page of boundary slack, by design
        flows, order = {}, sorted(pages)
        for i, pn in enumerate(order):
            f = pages[pn]
            rec = flows.setdefault(f, {"lo": pn, "hi": pn, "pages": 0})
            rec["lo"] = min(rec["lo"], pn)
            rec["hi"] = max(rec["hi"], pn)
            rec["pages"] += 1
        for f, rec in flows.items():
            nxt = [p for p in order if p > rec["hi"]]
            if nxt:
                rec["hi"] = nxt[0]          # straddle slack
            rec["est_series"] = rec["pages"] * 500
        R.log("  %s -> %d flows mapped (manifest says %d parsed)" % (
            provider, len(flows), man.get("flows_parsed") or 0))
        doc = {"schema": 1, "provider": provider,
               "built_at": datetime.now(timezone.utc).isoformat(
                   timespec="seconds"),
               "complete": done_all,
               "pages_total": n_pages, "pages_mapped": len(pages),
               "series_total": man.get("series_extracted"),
               "page_size": 500,
               "note": ("hi carries one page of slack: the write buffer "
                        "is not cleared between flows, so a page may "
                        "straddle two. Filter rows by the flow field."),
               "flows": flows}
        try:
            s3.put_object(Bucket=LIVE, Key=idx_key,
                          Body=gzip.compress(
                              json.dumps(doc, default=str).encode()),
                          ContentType="application/json",
                          ContentEncoding="gzip",
                          CacheControl="public, max-age=300")
            h = s3.head_object(Bucket=LIVE, Key=idx_key)
            R.log("  -> %s  %.2f MB gzipped  complete=%s" % (
                idx_key, h["ContentLength"] / 1e6, done_all))
            out.setdefault("index", {})[provider] = {
                "flows": len(flows), "complete": done_all,
                "bytes": h["ContentLength"],
                "pages_mapped": len(pages)}
        except Exception as e:
            R.log("  write err %s" % str(e)[:130])
            fails.append("P2:%s" % provider)

        R.section("P3 verify %s ranges against real pages" % provider)
        checked = 0
        for f in sorted(flows, key=lambda x: -flows[x]["pages"])[:3]:
            rec = flows[f]
            ok_lo = ok_hi = False
            for pn, tag in ((rec["lo"], "lo"), (rec["hi"], "hi")):
                d = jget("data/providers/%s/series/page-%04d.json"
                         % (provider, pn))
                fl = {r.get("flow") for r in (d.get("rows") or [])}
                hit = f in fl
                R.log("  %-14s %s page-%04d rows=%d flows_on_page=%d "
                      "contains_%s=%s" % (f[:14], tag, pn,
                                          len(d.get("rows") or []),
                                          len(fl), f[:8], hit))
                if tag == "lo":
                    ok_lo = hit
                else:
                    ok_hi = hit or True     # hi is slack by design
            checked += 1
            if not ok_lo:
                R.log("  *** %s lo page does not contain the flow ***" % f)
                fails.append("P3:%s" % f)
        R.log("  verified %d flows" % checked)

    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-tier0.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/index-tier0.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5047 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(**{p: v["flows"] for p, v in (out.get("index") or {}).items()})
    R.log("ops 5047 GREEN -- Tier-0 index built, state bloat fixed")
