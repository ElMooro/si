"""ops_5042 -- is the ECB mirror everything the portal has?

Khalid: "is this all the data we can get from ecb with all the history
or there is more data that we didnt get."

What the code already tells us before touching the network:
  * sdmx-walker pulls ECB as {flow}?format=csvdata with NO
    lastNObservations and NO startPeriod -- so the fast path is full
    history by construction, not a recent window;
  * justhodl-ecb-deep pulls the giant flows in startPeriod/endPeriod
    TIME WINDOWS, which is also full history, just sliced;
  * ops 4898 recorded the portal as hosting 214 dataflows and the card
    reports 156 fast + 58 deep = 214, 58/58 deep complete.
So history depth is probably fine. Two other things could still be
missing, and this op measures both.

  1. REGISTRY DRIFT. 214 was true in early August. The portal adds and
     retires dataflows. This re-pulls /service/dataflow live and diffs
     it against data/warm/ecb/catalog.json.gz.
  2. THE SERIES UNIVERSE -- almost certainly the real gap. The card's
     "214 series" is not 214 series, it is 214 DATAFLOWS: the catalog's
     series_from for ecb counts the dataflow list, exactly the mislabel
     that made Eurostat read 8,152. ECB csvdata is long format, one row
     per OBSERVATION, and a single flow holds thousands of distinct
     series keys. We have the raw observations; we have never built the
     per-series index. Eurostat now has 558M extracted series and ECB
     has zero, from a mirror of comparable size.

  P0 live dataflow registry vs ours -- additions, retirements
  P1 per-flow file inventory: which flows have bytes, which are
     suspiciously small (a truncated or error-body pull), which are
     missing entirely
  P2 sample real files: distinct SERIES KEYS, row counts, and the
     earliest/latest TIME_PERIOD actually present -- history proof from
     the data rather than from the request parameters
  P3 size the extraction: series per MB observed, projected ECB series
     universe, and what an extract_ecb would cost

Read-only. Nothing is fetched into the warehouse here.
"""
import gzip
import io
import json
import random
import re
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
WARM = "data/warm/ecb/"
CAT = "data/warm/ecb/catalog.json.gz"
COV = "data/warm/ecb/coverage.json"
DATAFLOW_URL = "https://data-api.ecb.europa.eu/service/dataflow"
UA = ("Mozilla/5.0 (compatible; JustHodlAI/1.0; "
      "+https://justhodl.ai) ops-5042-audit")
SAMPLE_FLOWS = 12
MAX_READ = 60 * 1024 * 1024

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def http(url):
    # ops 4893 trap: ECB refuses content negotiation -- send NO Accept
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=110) as r:
        return r.read()


def list_all(prefix):
    out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            out.append((o["Key"], o["Size"], o["LastModified"]))
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


with report("ops_5042_ecb_completeness") as R:
    fails = []
    out = {"op": "ops_5042"}

    R.section("P0 live dataflow registry vs ours")
    live_ids = set()
    try:
        raw = http(DATAFLOW_URL)
        R.log("  /service/dataflow -> %d bytes" % len(raw))
        txt = raw.decode("utf-8", "replace")
        for m in re.finditer(
                r'<str:Dataflow[^>]*\bid="([^"]+)"[^>]*'
                r'agencyID="([^"]+)"', txt):
            fid, ag = m.group(1), m.group(2)
            live_ids.add(fid if ag == "ECB" else f"{ag}:{fid}")
        if not live_ids:
            for m in re.finditer(r'\bid="([A-Z0-9_]{2,})"[^>]*'
                                 r'agencyID="ECB"', txt):
                live_ids.add(m.group(1))
        R.log("  live dataflows parsed: %d" % len(live_ids))
        R.log("  sample: %s" % sorted(live_ids)[:14])
    except Exception as e:
        R.log("  live registry FAILED %s" % str(e)[:160])
        fails.append("P0:live")
    ours = set()
    try:
        b = gzip.decompress(s3.get_object(Bucket=LIVE,
                                          Key=CAT)["Body"].read())
        cat = json.loads(b)
        for f in (cat.get("dataflows") or []):
            ours.add(f.get("id") if isinstance(f, dict) else str(f))
        R.log("  our catalog: n_dataflows=%s parsed=%d as_of=%s" % (
            cat.get("n_dataflows"), len(ours), cat.get("as_of")))
    except Exception as e:
        R.log("  our catalog err %s" % str(e)[:130])
    if live_ids and ours:
        added = sorted(live_ids - ours)
        gone = sorted(ours - live_ids)
        R.log("  NEW on the portal, absent from our catalog: %d %s" % (
            len(added), added[:20]))
        R.log("  in our catalog, no longer on the portal: %d %s" % (
            len(gone), gone[:20]))
        out.update(live=len(live_ids), ours=len(ours),
                   added=added[:40], retired=gone[:40])

    R.section("P1 per-flow file inventory")
    objs = list_all(WARM)
    tot = sum(s for _, s, _ in objs)
    R.log("  %s: %d objects, %.2f GB" % (WARM, len(objs), tot / 1e9))
    by_flow = defaultdict(int)
    for k, s, _ in objs:
        base = k[len(WARM):].split("/")[0]
        by_flow[base.split(".")[0].split("__")[0]] += s
    R.log("  distinct flow buckets under the prefix: %d" % len(by_flow))
    tiny = sorted([(f, b) for f, b in by_flow.items() if b < 3000])
    R.log("  suspiciously small (<3KB, likely an error body or empty "
          "pull): %d %s" % (len(tiny), tiny[:12]))
    biggest = sorted(by_flow.items(), key=lambda kv: -kv[1])[:8]
    R.log("  largest: %s" % [(f, "%.0f MB" % (b / 1e6))
                             for f, b in biggest])
    if ours:
        missing = sorted(f for f in ours
                         if f not in by_flow
                         and f.replace(":", "_") not in by_flow)
        R.log("  catalogued flows with NO file at all: %d %s" % (
            len(missing), missing[:20]))
        out["missing_files"] = missing[:40]
    try:
        cv = json.loads(s3.get_object(Bucket=LIVE,
                                      Key=COV)["Body"].read())
        keep = {k: cv.get(k) for k in
                ("fast", "deep", "deep_done", "failures", "as_of",
                 "n_total", "note") if cv.get(k) is not None}
        R.log("  coverage.json: %s" % json.dumps(keep, default=str)[:300])
    except Exception as e:
        R.log("  coverage err %s" % str(e)[:110])

    R.section("P2 sample the real files -- series keys and history")
    cands = [(k, s) for k, s, _ in objs if s > 50000
             and not k.endswith((".json", ".json.gz"))]
    random.seed(5042)
    sample = (sorted(cands, key=lambda kv: -kv[1])[:4]
              + random.sample(cands, min(SAMPLE_FLOWS - 4,
                                         max(0, len(cands) - 4))))
    tot_series, tot_rows, tot_bytes = 0, 0, 0
    for k, sz in sample[:SAMPLE_FLOWS]:
        try:
            body = s3.get_object(Bucket=LIVE, Key=k,
                                 Range="bytes=0-%d" % (MAX_READ - 1)
                                 )["Body"].read()
            if k.endswith(".gz"):
                try:
                    body = gzip.decompress(body)
                except Exception:
                    body = gzip.GzipFile(
                        fileobj=io.BytesIO(body)).read(MAX_READ)
            txt = body.decode("utf-8", "replace")
            lines = txt.splitlines()
            hdr = lines[0].split(",") if lines else []
            ki = next((i for i, c in enumerate(hdr)
                       if c.strip().upper() in ("KEY", "SERIES_KEY",
                                                "SERIESKEY")), None)
            ti = next((i for i, c in enumerate(hdr)
                       if "TIME_PERIOD" in c.strip().upper()), None)
            keys, periods = set(), []
            for ln in lines[1:]:
                p = ln.split(",")
                if ki is not None and len(p) > ki:
                    keys.add(p[ki])
                if ti is not None and len(p) > ti and p[ti]:
                    periods.append(p[ti])
            rows = len(lines) - 1
            tot_series += len(keys)
            tot_rows += rows
            tot_bytes += len(body)
            R.log("  %-46s %7.1f MB  rows=%-8d series=%-7d  %s..%s" % (
                k[len(WARM):][:46], sz / 1e6, rows, len(keys),
                min(periods) if periods else "?",
                max(periods) if periods else "?"))
            if ki is None:
                R.log("      (no KEY column -- header: %s)" % hdr[:8])
        except Exception as e:
            R.log("  %-46s READ ERR %s" % (k[len(WARM):][:46],
                                           str(e)[:70]))
    if tot_bytes:
        per_mb = tot_series / (tot_bytes / 1e6)
        R.log("  sampled %.1f MB -> %d distinct series (%.1f series/MB), "
              "%d observation rows" % (tot_bytes / 1e6, tot_series,
                                       per_mb, tot_rows))
        out.update(sample_series=tot_series, series_per_mb=round(per_mb, 1))

        R.section("P3 size the extraction")
        proj = per_mb * (tot / 1e6)
        R.log("  ECB warm mirror is %.2f GB -> ~%.1fM distinct series "
              "at the observed density" % (tot / 1e9, proj / 1e6))
        R.log("  pages at 500 series/page: ~%.0f  (Eurostat needed "
              "~1.1M pages for 558M series)" % (proj / 500))
        R.log("  the extractor is generic -- EXTRACTORS = "
              "{'eurostat': extract_eurostat}. ECB needs an extract_ecb "
              "because csvdata is LONG format (one row per observation, "
              "series identified by the KEY column) whereas Eurostat TSV "
              "is wide (one row per series, columns are periods)")
        out["projected_series"] = int(proj)
    else:
        R.log("  no readable sample -- cannot size the extraction")
        fails.append("P2:sample")

    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/ecb-completeness.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/ecb-completeness.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5042 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(live_dataflows=out.get("live"), our_dataflows=out.get("ours"),
         added=len(out.get("added") or []),
         missing_files=len(out.get("missing_files") or []),
         projected_series=out.get("projected_series"))
    R.log("ops 5042 GREEN -- ECB completeness measured")
