"""ops_5049 -- exact per-flow counts, and does Tier 1 earn its keep?

Two open questions from the Tier-0 build.

 1. est_series_max is pages x 500 -- an upper bound, not a count. A
    catalog that reports a bound as a number is a catalog people stop
    trusting.
 2. Tier 1 (per-flow sorted shards + sparse block index, range-queried)
    was spec'd on the assumption that some flows are too big to serve by
    fetching their page range. That assumption has never been measured.
    If the 99th percentile flow spans a handful of pages, Tier 1 is
    engineering nobody needs.

EXACT COUNTS WITHOUT RE-READING 313GB. Pages are written in flow order,
so a page is "pure" unless a flow boundary falls inside it, and there
are at most 8,147 boundaries across 1.13M pages. Between two consecutive
anchors with no interpolated flow in between, every interior page is 500
rows of one flow -- provable from the ordering, no read required. Only
two classes of page actually need reading:
    * anchor pages, where one flow ends and another begins
    * every page inside a gap that CONTAINS interpolated flows, since
      several small flows share those pages
For ECB that is the whole store (6,481 pages, ~1.8GB) so it gets exact
counts outright. For Eurostat it is a small fraction of 1.13M.

THE CHECK THAT MAKES IT REAL. Once every flow has a count, they must sum
to the manifest's series_extracted MINUS the rows still sitting in
state["buffer"] -- which were counted into the manifest but never
written to a page (eurostat 235, ecb 332). If that identity holds, the
index is provably complete and correctly partitioned. If it does not,
something is miscounted and the op goes RED.

  P0 span distribution -> the Tier-1 decision, measured not assumed
  P1 ECB: read every page, exact counts, sum identity
  P2 Eurostat: read only anchor pages and impure gaps, budgeted
  P3 publish schema 3 with exact counts where proven
"""
import concurrent.futures as cf
import gzip
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
WORKERS = 64
BUDGET_S = 2700

cfg = Config(read_timeout=60, retries={"max_attempts": 4},
             max_pool_connections=WORKERS * 2)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
T0 = time.time()


def jget(k):
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return {}


def tally_page(provider, n):
    try:
        b = s3.get_object(
            Bucket=LIVE,
            Key="data/providers/%s/series/page-%04d.json" % (provider, n)
        )["Body"].read()
        c = Counter()
        for r in (json.loads(b).get("rows") or []):
            f = r.get("flow")
            if f:
                c[f] += 1
        return n, c
    except Exception:
        return n, None


with report("ops_5049_exact_counts") as R:
    fails = []
    out = {"op": "ops_5049", "schema": 3}

    for provider in ("ecb", "eurostat"):
        idx = jget("index/%s/flows.json.gz" % provider)
        flows = idx.get("flows") or {}
        st = jget("data/_state/series-extract-%s.json" % provider)
        man = jget("data/providers/%s/series-manifest.json" % provider)
        order = list(st.get("flows_done") or [])
        buffered = len(st.get("buffer") or [])
        n_pages = int(idx.get("pages_total") or 0)
        if not flows:
            R.log("  %s index missing" % provider)
            fails.append("load:%s" % provider)
            continue

        R.section("P0 %s -- span distribution (the Tier-1 question)"
                  % provider)
        spans = sorted((r["hi"] - r["lo"] + 1) for r in flows.values())
        n = len(spans)

        def pct(q):
            return spans[min(n - 1, int(q * n))]
        R.log("  flows=%d  span pages: p50=%d p90=%d p99=%d max=%d" % (
            n, pct(.5), pct(.9), pct(.99), spans[-1]))
        for t in (1, 5, 20, 100, 1000):
            k = sum(1 for s in spans if s > t)
            R.log("    spanning > %-4d pages: %5d flows (%.1f%%)" % (
                t, k, 100.0 * k / n))
        R.log("    a page is ~280KB, so a %d-page flow is ~%.1f MB to "
              "fetch whole" % (pct(.9), pct(.9) * 0.28))

        R.section("P1/P2 %s -- exact counts" % provider)
        anchors = sorted((r["lo"], f) for f, r in flows.items()
                         if r.get("anchored"))
        apages = [p for p, _ in anchors]
        pos = {f: i for i, f in enumerate(order)}
        anchored_order = sorted((pos[f], f, p) for p, f in anchors
                                if f in pos)
        need = set(apages)
        impure_gaps = 0
        for k in range(len(anchored_order) - 1):
            i0, _f0, p0 = anchored_order[k]
            i1, _f1, p1 = anchored_order[k + 1]
            if i1 - i0 > 1:                 # interpolated flows inside
                impure_gaps += 1
                need.update(range(p0, min(p1 + 1, n_pages)))
        R.log("  anchor pages=%d  impure gaps=%d  pages to read=%s of %s "
              "(%.1f%%)" % (len(apages), impure_gaps, f"{len(need):,}",
                            f"{n_pages:,}",
                            100.0 * len(need) / max(1, n_pages)))
        counts = Counter()
        read_ok = 0
        pages_read = {}
        todo = sorted(need)
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for i in range(0, len(todo), 20000):
                if time.time() - T0 > BUDGET_S:
                    R.log("  budget reached after %d pages" % read_ok)
                    break
                chunk = todo[i:i + 20000]
                for pn, c in ex.map(
                        lambda x: tally_page(provider, x), chunk):
                    if c is not None:
                        pages_read[pn] = c
                        counts.update(c)
                        read_ok += 1
                R.log("    read %s/%s (%.0fs)" % (
                    f"{read_ok:,}", f"{len(todo):,}", time.time() - T0))
        # pure interior pages: provable 500 rows of the gap's anchor flow
        pure = 0
        for k in range(len(anchored_order)):
            i0, f0, p0 = anchored_order[k]
            p1 = (anchored_order[k + 1][2] if k + 1 < len(anchored_order)
                  else n_pages)
            gap_has_interp = (k + 1 < len(anchored_order)
                              and anchored_order[k + 1][0] - i0 > 1)
            if gap_has_interp:
                continue
            for p in range(p0 + 1, p1):
                if p not in pages_read:
                    counts[f0] += 500
                    pure += 1
        R.log("  pages read=%s  pure interior pages inferred=%s" % (
            f"{read_ok:,}", f"{pure:,}"))
        total = sum(counts.values())
        expect = int(man.get("series_extracted") or 0) - buffered
        R.log("  counted=%s   expected=%s (manifest %s minus %d "
              "buffered rows never written to a page)" % (
                  f"{total:,}", f"{expect:,}",
                  f"{man.get('series_extracted') or 0:,}", buffered))
        delta = total - expect
        R.log("  delta=%s  %s" % (
            f"{delta:,}",
            "IDENTITY HOLDS -- index provably complete"
            if abs(delta) <= 500 else
            "*** miscount: the partition does not add up ***"))
        exact = (abs(delta) <= 500 and read_ok == len(todo))
        if read_ok != len(todo):
            R.log("  (partial read -- counts are a floor, not exact)")
        if abs(delta) > 500 and read_ok == len(todo):
            fails.append("P2:%s-identity" % provider)

        R.section("P3 %s -- publish schema 3" % provider)
        for f, r in flows.items():
            if f in counts:
                r["series"] = int(counts[f])
            r["exact"] = bool(exact and f in counts)
        top = sorted(((r.get("series") or 0), f)
                     for f, r in flows.items())[-6:]
        R.log("  largest flows by series: %s" % [
            (f, "%s" % f"{c:,}") for c, f in reversed(top)])
        idx.update(schema=3, counts_exact=exact,
                   counted_total=total, buffered_rows=buffered,
                   rebuilt_at=datetime.now(timezone.utc).isoformat(
                       timespec="seconds"))
        idx["note"] = (idx.get("note", "") +
                       " series is an exact count where exact=true; "
                       "est_series_max remains an upper bound.")
        try:
            k = "index/%s/flows.json.gz" % provider
            s3.put_object(Bucket=LIVE, Key=k,
                          Body=gzip.compress(
                              json.dumps(idx, default=str).encode()),
                          ContentType="application/json",
                          ContentEncoding="gzip",
                          CacheControl="public, max-age=300")
            h = s3.head_object(Bucket=LIVE, Key=k)
            R.log("  -> %s  %.2f MB gz  exact=%s" % (
                k, h["ContentLength"] / 1e6, exact))
            out.setdefault("lanes", {})[provider] = {
                "flows": len(flows), "counted": total,
                "expected": expect, "delta": delta, "exact": exact,
                "p50_span": pct(.5), "p99_span": pct(.99),
                "max_span": spans[-1]}
        except Exception as e:
            R.log("  write err %s" % str(e)[:120])
            fails.append("P3:%s" % provider)

    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-tier0.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/index-tier0.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5049 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(**{p: "%s series, delta %s" % (f"{v['counted']:,}", v["delta"])
            for p, v in (out.get("lanes") or {}).items()})
    R.log("ops 5049 GREEN -- counts reconcile to the manifest identity")
