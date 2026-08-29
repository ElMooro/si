"""ops_5041 -- finish change 2: confirm the seed, then verify the card.

ops 5040 established change 1 outright: the per-provider doc and the hub
row both read series.count = 551,382,113 with counted=True, other
providers untouched (fred 277,453 / oecd 1,546 / bis 29). The card no
longer says 8,152.

Change 2's plumbing is deployed and working -- it simply had nothing to
read. The manifest showed pages=0 pages_bytes=0 because the seed had not
checkpointed yet: flows advanced 6344 -> 6482 up to 19:45 under the
PREVIOUS invocation's code, then the first new-code run began, and a run
only persists state when it completes a flow or trips its budget. My
8-minute wait ended inside that gap. Nothing was broken; I measured too
early. Again.

Two changes in how this op measures, both learned the hard way today:
  * pages_seed_error is printed UNCONDITIONALLY. ops 5040 only printed
    it when pages_seeded was truthy, so a failing seed would have been
    invisible -- exactly the wrong way round.
  * the catalog is invoked with InvocationType="Event" and verified by
    polling the hub's as_of. Two sync invokes in a row died with
    "Connection was closed before we received a valid response" at 793s
    and 534s even though the function itself completed and wrote its
    output. A long sync invoke is a bad way to observe a batch job.

  P0 state + manifest, everything printed whether set or not
  P1 wait for the seed to land (up to ~14 min)
  P2 fire the catalog asynchronously, wait for as_of to move
  P3 the card: series, n_keys, derived, note; hub totals before/after
  P4 regression: coverage, document size, other providers
"""
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
CAT = "justhodl-provider-catalog"
HUB = "data/provider-catalog.json"
PDOC = "data/providers/eurostat.json"
MAN = "data/providers/eurostat/series-manifest.json"
STATE_KEY = "data/_state/series-extract-eurostat.json"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)


def jget(key):
    return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())


with report("ops_5041_seed_and_card") as R:
    fails = []
    out = {"op": "ops_5041"}

    R.section("P0 state + manifest, printed unconditionally")
    try:
        st = jget(STATE_KEY)
        R.log("  flows_done=%d n_pages=%s series_count=%s updated_at=%s"
              % (len(st.get("flows_done") or []), st.get("n_pages"),
                 st.get("series_count"), st.get("updated_at")))
        R.log("  pages_seeded    = %r" % st.get("pages_seeded"))
        R.log("  pages_seed_error= %r" % st.get("pages_seed_error"))
        R.log("  pages_objects   = %r" % st.get("pages_objects"))
        R.log("  pages_bytes     = %r" % st.get("pages_bytes"))
        R.log("  holes=%d failed_flows=%d write_errors_last_run=%s" % (
            len(st.get("missing_pages") or []),
            len(st.get("failed_flows") or []),
            st.get("write_errors_this_run")))
    except Exception as e:
        R.log("  state err %s" % str(e)[:130])
    try:
        t0 = (jget(HUB).get("totals") or {})
        R.log("  BEFORE hub totals: keys=%s gb=%s datasets=%s" % (
            f"{t0.get('keys') or 0:,}", t0.get("gb"), t0.get("datasets")))
        out["before_totals"] = t0
        out["before_as_of"] = jget(HUB).get("as_of")
    except Exception as e:
        R.log("  hub err %s" % str(e)[:110])

    R.section("P1 wait for the seed")
    seeded = False
    for i in range(28):                       # ~14 min
        try:
            m = jget(MAN)
            pg = int(m.get("pages") or 0)
            pb = int(m.get("pages_bytes") or 0)
            stx = jget(STATE_KEY)
            if pg > 0 and pb > 0:
                seeded = True
                R.log("  SEEDED: pages=%s pages_bytes=%.1f GB "
                      "(flows_parsed=%s, series=%s)" % (
                          f"{pg:,}", pb / 1e9, m.get("flows_parsed"),
                          f"{m.get('series_extracted') or 0:,}"))
                out["manifest"] = {"pages": pg, "pages_bytes": pb,
                                   "series": m.get("series_extracted"),
                                   "flows_parsed": m.get("flows_parsed")}
                break
            if i % 3 == 0:
                R.log("  t+%2dmin pages=%s bytes=%s flows=%s seeded=%r "
                      "err=%r" % (i // 2, pg, pb, m.get("flows_parsed"),
                                  stx.get("pages_seeded"),
                                  stx.get("pages_seed_error")))
        except Exception as e:
            R.log("  poll err %s" % str(e)[:90])
        time.sleep(30)
    if not seeded:
        R.log("  seed still absent -- the counted totals cannot appear "
              "until the engine publishes pages/pages_bytes")
        fails.append("P1:seed")

    R.section("P2 run the catalog asynchronously")
    if seeded:
        before_as_of = out.get("before_as_of")
        try:
            r = lam.invoke(FunctionName=CAT, InvocationType="Event",
                           Payload=b"{}")
            R.log("  Event invoke accepted status=%s" % r.get("StatusCode"))
        except Exception as e:
            R.log("  invoke err %s" % str(e)[:130])
            fails.append("P2:invoke")
        moved = False
        for i in range(24):                   # ~12 min
            time.sleep(30)
            try:
                h = jget(HUB)
                if h.get("as_of") != before_as_of:
                    moved = True
                    R.log("  hub rewritten after %ds (as_of %s -> %s)"
                          % ((i + 1) * 30, before_as_of, h.get("as_of")))
                    break
            except Exception:
                pass
        if not moved:
            R.log("  hub as_of unchanged -- catalog did not finish")
            fails.append("P2:nowrite")
    else:
        R.log("  skipped -- nothing new for the catalog to read")

    R.section("P3 the card")
    try:
        d = jget(PDOC)
        ser = d.get("series") or {}
        R.log("  series.count=%s counted=%s" % (
            f"{ser.get('count') or 0:,}", ser.get("counted")))
        R.log("  n_keys=%s total_mb=%s" % (
            f"{d.get('n_keys') or 0:,}", d.get("total_mb")))
        R.log("  derived=%s" % json.dumps(d.get("derived"),
                                          default=str)[:240])
        R.log("  note=%s" % str(d.get("note"))[:200])
        out["card"] = {"series": ser.get("count"),
                       "n_keys": d.get("n_keys"),
                       "total_mb": d.get("total_mb"),
                       "derived": d.get("derived")}
        if seeded and not (d.get("derived") or {}).get("objects"):
            fails.append("P3:derived")
    except Exception as e:
        R.log("  pdoc err %s" % str(e)[:130])
        fails.append("P3")
    try:
        h = jget(HUB)
        t = h.get("totals") or {}
        b = out.get("before_totals") or {}
        R.log("  hub totals keys %s -> %s" % (
            f"{b.get('keys') or 0:,}", f"{t.get('keys') or 0:,}"))
        R.log("  hub totals gb   %s -> %s" % (b.get("gb"), t.get("gb")))
        R.log("  hub datasets    %s -> %s (must NOT absorb series)" % (
            b.get("datasets"), t.get("datasets")))
        e = next((p for p in (h.get("providers") or [])
                  if p.get("slug") == "eurostat"), {})
        R.log("  hub eurostat row: series_count=%s n_keys=%s "
              "coverage_pct=%s datasets=%s" % (
                  f"{e.get('series_count') or 0:,}",
                  f"{e.get('n_keys') or 0:,}", e.get("coverage_pct"),
                  e.get("datasets")))
        if e.get("coverage_pct") is not None and \
                float(e["coverage_pct"]) > 100.5:
            fails.append("P3:coverage")
        out["after_totals"] = t
    except Exception as ex:
        R.log("  hub err %s" % str(ex)[:120])

    R.section("P4 regression")
    try:
        hd = s3.head_object(Bucket=LIVE, Key=PDOC)
        d = jget(PDOC)
        R.log("  %s = %.2f MB, per-key rows=%d" % (
            PDOC, hd["ContentLength"] / 1e6, len(d.get("keys") or [])))
        if hd["ContentLength"] / 1e6 > 25:
            fails.append("P4:docsize")
        h = jget(HUB)
        R.log("  other providers: %s" % [
            (p.get("slug"), p.get("series_count"))
            for p in (h.get("providers") or [])
            if p.get("slug") in ("fred", "oecd", "bis", "statcan")])
    except Exception as e:
        R.log("  regression err %s" % str(e)[:120])
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/eurostat-card-fix.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-card-fix.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5041 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(series=(out.get("card") or {}).get("series"),
         n_keys=(out.get("card") or {}).get("n_keys"),
         derived=bool((out.get("card") or {}).get("derived")))
    R.log("ops 5041 GREEN -- card truthful and totals complete")
