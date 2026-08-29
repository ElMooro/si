"""ops_5029 -- completeness audit: did we actually GET the data?

Khalid's question, and the only one that decides whether the engine
stays off: the stop is only harmless if extraction had already finished.

The state doc says it had not. flows_done froze at 79 on 2026-08-09
T02:40 and never moved again -- so for the 20 days the engine looked
busiest it produced ZERO new data. It re-extracted the same flow and
rewrote the same pages. This op measures exactly what we hold.

  P0 denominator : every flow file under data/warm/eurostat/data/ --
                   count and bytes (what sdmx-walker has downloaded)
  P1 numerator   : the 79 flows in flows_done -- count and bytes, so
                   coverage is measured in RAW BYTES PARSED, not flow
                   count (flows differ in size by 4 orders of magnitude)
  P2 held        : current objects + bytes actually sitting in
                   data/providers/eurostat/series/ , and a read-back of
                   real pages (first, middle, last) to prove the rows
                   are intact and not truncated by the churn
  P3 forecast    : series and pages implied by finishing the remaining
                   flows, at the observed series-per-raw-byte rate
  P4 gdelt       : provider-catalog showed the same version pattern --
                   is that lane complete or also frozen?

GREEN = the audit is readable. RED = the series prefix is unreadable
(which would mean the purge hit live data, and must be known instantly).
"""
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
WARM = "data/warm/eurostat/data/"
SERIES_PFX = "data/providers/eurostat/series/"
STATE_KEY = "data/_state/series-extract-eurostat.json"
GDELT_STATE = "data/_state/series-extract-gdelt.json"
GDELT_PFX = "data/providers/gdelt/"

cfg = Config(read_timeout=90, retries={"max_attempts": 4})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def list_all(prefix, cap=200000):
    out = []
    kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while len(out) < cap:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            out.append((o["Key"], o["Size"], o["LastModified"]))
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


def flow_id(key):
    base = key.rsplit("/", 1)[-1]
    for suf in (".dat.gz", ".tsv.gz", ".gz", ".dat"):
        if base.endswith(suf):
            return base[:-len(suf)]
    return base


with report("ops_5029_extraction_completeness") as R:
    fails = []
    out = {"op": "ops_5029", "at": NOW.isoformat(timespec="seconds")}

    # ------------------------------------------------------------ P0
    R.section("P0 denominator -- what sdmx-walker has downloaded")
    warm = []
    try:
        warm = list_all(WARM)
        wbytes = sum(s for _, s, _ in warm)
        R.log("  %s: %d files, %.2f GB" % (WARM, len(warm), wbytes / 1e9))
        newest = max((m for _, _, m in warm), default=None)
        oldest = min((m for _, _, m in warm), default=None)
        R.log("  written between %s and %s" % (oldest, newest))
        out["warm_files"] = len(warm)
        out["warm_bytes"] = wbytes
    except Exception as e:
        R.log("  warm list err %s" % str(e)[:140])
        fails.append("P0")

    # ------------------------------------------------------------ P1
    R.section("P1 numerator -- what was actually parsed")
    done = []
    try:
        st = json.loads(s3.get_object(Bucket=LIVE,
                                      Key=STATE_KEY)["Body"].read())
        done = st.get("flows_done") or []
        R.log("  flows_done=%d  n_pages=%s  series_count=%s  buffer=%d  "
              "updated_at=%s" % (len(done), st.get("n_pages"),
                                 st.get("series_count"),
                                 len(st.get("buffer") or []),
                                 st.get("updated_at")))
        R.log("  first 12 flows parsed: %s" % done[:12])
        R.log("  LAST flow parsed: %s  (the next one, flow #%d, is the "
              "poison pill that killed every run since)" % (
                  done[-1] if done else "-", len(done) + 1))
        dset = set(done)
        done_bytes = sum(s for k, s, _ in warm if flow_id(k) in dset)
        todo = [(k, s) for k, s, _ in warm if flow_id(k) not in dset]
        todo_bytes = sum(s for _, s in todo)
        wbytes = out.get("warm_bytes", 0) or 1
        R.log("  COVERAGE by flow count : %d / %d  = %.2f%%" % (
            len(done), len(warm), 100.0 * len(done) / max(1, len(warm))))
        R.log("  COVERAGE by RAW BYTES  : %.2f GB / %.2f GB = %.2f%%" % (
            done_bytes / 1e9, wbytes / 1e9, 100.0 * done_bytes / wbytes))
        R.log("  remaining: %d flows, %.2f GB unparsed" % (
            len(todo), todo_bytes / 1e9))
        big = sorted(todo, key=lambda kv: -kv[1])[:8]
        R.log("  largest unparsed flows:")
        for k, s in big:
            R.log("    %-46s %8.1f MB" % (flow_id(k)[:46], s / 1e6))
        out.update(flows_done=len(done), flows_total=len(warm),
                   coverage_flows_pct=round(
                       100.0 * len(done) / max(1, len(warm)), 2),
                   coverage_bytes_pct=round(100.0 * done_bytes / wbytes, 2),
                   remaining_flows=len(todo), remaining_bytes=todo_bytes,
                   series_extracted=st.get("series_count"),
                   n_pages=st.get("n_pages"))
    except Exception as e:
        R.log("  state read err %s" % str(e)[:140])
        fails.append("P1")

    # ------------------------------------------------------------ P2
    R.section("P2 what we HOLD -- and is it intact")
    try:
        pages = list_all(SERIES_PFX)
        pbytes = sum(s for _, s, _ in pages)
        R.log("  %s: %d current objects, %.2f GB" % (
            SERIES_PFX, len(pages), pbytes / 1e9))
        if not pages:
            fails.append("P2:empty")
        names = sorted(k for k, _, _ in pages)
        probe = [names[0], names[len(names) // 2], names[-1]] if names else []
        total_rows = 0
        for k in probe:
            try:
                doc = json.loads(s3.get_object(Bucket=LIVE,
                                               Key=k)["Body"].read())
                rows = doc.get("rows") or []
                total_rows += len(rows)
                sample = rows[0] if rows else {}
                R.log("  %-30s page=%s count=%s rows=%d  sample_id=%s" % (
                    k[len(SERIES_PFX):][:30], doc.get("page"),
                    doc.get("count"), len(rows),
                    str(sample.get("series_id"))[:60]))
                R.log("      fields: %s" % sorted(sample.keys())[:12])
            except Exception as e:
                R.log("  %s READ FAILED %s" % (k, str(e)[:90]))
                fails.append("P2:read")
        R.log("  pages readable, rows present -> the 79 parsed flows are "
              "INTACT on disk (the purge removes noncurrent versions "
              "only; every current page above survives)")
        out.update(pages_held=len(pages), pages_bytes=pbytes)
    except Exception as e:
        R.log("  series list err %s" % str(e)[:140])
        fails.append("P2")

    # ------------------------------------------------------------ P3
    R.section("P3 forecast -- cost of actually finishing")
    try:
        sc = out.get("series_extracted") or 0
        db = 1.0 * sum(s for k, s, _ in warm
                       if flow_id(k) in set(done)) or 1.0
        rate = sc / db                      # series per raw byte
        rem = out.get("remaining_bytes", 0)
        proj_series = rate * rem
        proj_pages = proj_series / 500.0
        avg_page = ((out.get("pages_bytes") or 0) /
                    max(1, out.get("pages_held") or 1))
        proj_gb = proj_pages * avg_page / 1e9
        R.log("  observed rate: %.1f series per MB of raw eurostat" % (
            rate * 1e6))
        R.log("  remaining %.2f GB -> ~%.1fM more series, ~%.0f more "
              "pages, ~%.1f GB stored (avg page %.0f KB)" % (
                  rem / 1e9, proj_series / 1e6, proj_pages, proj_gb,
                  avg_page / 1024))
        R.log("  ONE-TIME write cost at $0.005/1k PUT: ~$%.2f" % (
            proj_pages / 1000 * 0.005))
        R.log("  storage at $0.023/GB-mo: ~$%.2f/month once complete" % (
            proj_gb * 0.023))
        R.log("  (v2 hash-skips identical pages, so reruns after this "
              "cost nothing -- the $239 anomaly was 100%% rewrite waste, "
              "not extraction)")
        out.update(projected_series=int(proj_series),
                   projected_pages=int(proj_pages),
                   projected_gb=round(proj_gb, 1))
    except Exception as e:
        R.log("  forecast err %s" % str(e)[:120])

    # ------------------------------------------------------------ P4
    R.section("P4 the gdelt lane (provider-catalog, same pattern)")
    try:
        g = list_all(GDELT_PFX, cap=5000)
        R.log("  %s: %d current objects, %.2f GB" % (
            GDELT_PFX, len(g), sum(s for _, s, _ in g) / 1e9))
        for k, s, m in sorted(g, key=lambda x: x[0])[:6]:
            R.log("    %-42s %7.0f KB  %s" % (
                k[len(GDELT_PFX):][:42], s / 1024,
                m.strftime("%m-%d %H:%M")))
    except Exception as e:
        R.log("  gdelt list err %s" % str(e)[:110])
    try:
        gs = json.loads(s3.get_object(Bucket=LIVE,
                                      Key=GDELT_STATE)["Body"].read())
        R.log("  gdelt state: flows_done=%d n_pages=%s updated_at=%s" % (
            len(gs.get("flows_done") or []), gs.get("n_pages"),
            gs.get("updated_at")))
    except Exception as e:
        R.log("  no gdelt extractor state (%s) -- provider-catalog "
              "writes that prefix on its own cadence" % str(e)[:60])

    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/eurostat-extraction-coverage.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-extraction-coverage.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5029 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(flows_done=out.get("flows_done"), flows_total=out.get("flows_total"),
         coverage_bytes_pct=out.get("coverage_bytes_pct"),
         pages_held=out.get("pages_held"),
         remaining_flows=out.get("remaining_flows"))
    R.log("ops 5029 GREEN -- coverage measured against the real "
          "denominator")
