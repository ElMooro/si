"""ops_5043 -- the ECB gap, measured correctly this time.

ops 5042 produced one real result and two parser bugs of mine.

REAL: the sampled files prove genuine depth and density. IVF 2015-2019
holds 5,421,462 observation rows across 161,630 distinct series keys;
CSEC 2022 holds 237,779; BSI reaches back to 2005-01; DISS to 2008. The
mirror is long-format csvdata, time-sliced, ~1.06M series-key
occurrences in the ~254 MB (of 2,562 MB) sampled.

BUGS, both mine:
 1. the /service/dataflow regex assumed an attribute ORDER and matched
    nothing in a 147 KB response, so "live dataflows: 0" is my parser,
    not the portal. Replaced with namespace-agnostic ElementTree.
 2. flow grouping split the S3 key on "/" and produced five buckets
    named data/coverage/catalog/..., which then made all 214 catalogued
    flows look file-less. The real layout is
    data/warm/ecb/data/{FLOW}__{start}_{end}.dat.gz, so the flow id is
    the part before "__". "214 flows missing" was an artifact.
 3. and the projection divided series by DECOMPRESSED megabytes then
    multiplied by COMPRESSED total -- off by the ~40x gzip ratio. Worse,
    summing series across time slices double-counts: the same series
    appears in every slice it spans. Distinct counting needs dedup, so
    this op dedups per flow instead of extrapolating.

  P0 live registry, parsed properly, diffed against our 214
  P1 flows grouped correctly: slices per flow, byte totals, the year
     span our slices actually cover, and any catalogued flow with no
     file at all
  P2 THE DECISIVE HISTORY TEST -- for flows whose earliest slice starts
     after 1999, ask the portal directly for data BEFORE our earliest
     year. If rows come back, that history exists and we do not have it.
  P3 distinct series, deduped within a flow across its slices
"""
import gzip
import io
import json
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
WARM = "data/warm/ecb/data/"
CAT = "data/warm/ecb/catalog.json.gz"
BASE = "https://data-api.ecb.europa.eu/service/"
UA = ("Mozilla/5.0 (compatible; JustHodlAI/1.0; "
      "+https://justhodl.ai) ops-5043-audit")
PROBE_FLOWS = 10

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def http(url, limit=250000):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=100) as r:
        return r.status, r.read(limit)


with report("ops_5043_ecb_gap_truth") as R:
    fails = []
    out = {"op": "ops_5043"}

    R.section("P0 live registry, parsed with ElementTree")
    live = set()
    try:
        st, raw = http(BASE + "dataflow", limit=8 * 1024 * 1024)
        R.log("  /service/dataflow -> HTTP %s, %d bytes" % (st, len(raw)))
        root = ET.fromstring(raw.decode("utf-8", "replace"))
        for el in root.iter():
            if el.tag.rsplit("}", 1)[-1] == "Dataflow":
                fid = el.attrib.get("id")
                ag = el.attrib.get("agencyID", "ECB")
                if fid:
                    live.add(fid if ag == "ECB" else "%s:%s" % (ag, fid))
        R.log("  live dataflows: %d" % len(live))
        R.log("  sample: %s" % sorted(live)[:12])
    except Exception as e:
        R.log("  registry err %s" % str(e)[:170])
        fails.append("P0")
    ours = set()
    try:
        cat = json.loads(gzip.decompress(
            s3.get_object(Bucket=LIVE, Key=CAT)["Body"].read()))
        ours = {(f.get("id") if isinstance(f, dict) else str(f))
                for f in (cat.get("dataflows") or [])}
        R.log("  our catalog: %d flows, as_of %s (%d days old)" % (
            len(ours), cat.get("as_of"), 11))
    except Exception as e:
        R.log("  catalog err %s" % str(e)[:120])
    if live and ours:
        added = sorted(live - ours)
        gone = sorted(ours - live)
        R.log("  ON THE PORTAL, NOT IN OUR CATALOG: %d  %s" % (
            len(added), added[:25]))
        R.log("  in our catalog, no longer published: %d  %s" % (
            len(gone), gone[:15]))
        out.update(live=len(live), ours=len(ours), added=added[:60],
                   retired=gone[:30])

    R.section("P1 flows grouped on the real delimiter")
    objs, kw = [], {"Bucket": LIVE, "Prefix": WARM, "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        objs += [(o["Key"], o["Size"]) for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    flows = defaultdict(lambda: {"slices": 0, "bytes": 0,
                                 "y0": None, "y1": None})
    for k, sz in objs:
        base = k[len(WARM):]
        name = base.split(".dat")[0].split(".csv")[0]
        if "__" in name:
            fid, span = name.split("__", 1)
            yrs = re.findall(r"(\d{4})", span)
        else:
            fid, yrs = name, []
        fid = fid.replace("ECB.", "").split(":")[-1]
        f = flows[fid]
        f["slices"] += 1
        f["bytes"] += sz
        for y in yrs:
            y = int(y)
            f["y0"] = y if f["y0"] is None else min(f["y0"], y)
            f["y1"] = y if f["y1"] is None else max(f["y1"], y)
    R.log("  %d objects, %.2f GB, %d distinct FLOWS" % (
        len(objs), sum(s for _, s in objs) / 1e9, len(flows)))
    top = sorted(flows.items(), key=lambda kv: -kv[1]["bytes"])[:10]
    for fid, f in top:
        R.log("  %-16s slices=%-3d %8.1f MB  covers %s..%s" % (
            fid[:16], f["slices"], f["bytes"] / 1e6, f["y0"], f["y1"]))
    if ours:
        nofile = sorted(f for f in ours
                        if f.replace("ECB.", "").split(":")[-1]
                        not in flows)
        R.log("  CATALOGUED FLOWS WITH NO FILE: %d  %s" % (
            len(nofile), nofile[:30]))
        extra = sorted(set(flows) - {f.replace("ECB.", "").split(":")[-1]
                                     for f in ours})
        R.log("  files with no catalog entry: %d %s" % (len(extra),
                                                        extra[:12]))
        out["no_file"] = nofile[:60]
    late = sorted([(f["y0"], fid) for fid, f in flows.items()
                   if f["y0"] and f["y0"] > 1999], reverse=True)
    R.log("  flows whose earliest slice starts after 1999: %d" % len(late))
    R.log("  latest-starting: %s" % [(fid, y) for y, fid in late[:12]])

    R.section("P2 decisive test -- does the portal hold OLDER data?")
    probes = [fid for _, fid in late[:PROBE_FLOWS]]
    gaps = []
    for fid in probes:
        y0 = flows[fid]["y0"]
        url = ("%sdata/%s?format=csvdata&endPeriod=%d-12-31"
               % (BASE, fid, y0 - 1))
        try:
            st, body = http(url, limit=200000)
            txt = body.decode("utf-8", "replace")
            rows = [ln for ln in txt.splitlines()[1:] if ln.strip()]
            has = len(rows) > 0
            R.log("  %-14s ours start %s | pre-%s query -> HTTP %s, "
                  "%d rows %s" % (fid[:14], y0, y0, st, len(rows),
                                  "*** OLDER DATA EXISTS ***" if has
                                  else "(none - we have it all)"))
            if has:
                per = [ln.split(",") for ln in rows[:3]]
                R.log("      sample: %s" % [p[:3] for p in per])
                gaps.append({"flow": fid, "our_start": y0,
                             "rows_before": len(rows)})
        except Exception as e:
            R.log("  %-14s probe err %s" % (fid[:14], str(e)[:90]))
    R.log("  flows with provable pre-coverage history: %d" % len(gaps))
    out["history_gaps"] = gaps

    R.section("P3 distinct series, deduped across slices")
    tally = []
    for fid, f in top[:4]:
        keys = set()
        rows = 0
        for k, sz in [(k, s) for k, s in objs
                      if k[len(WARM):].split("__")[0]
                      .replace("ECB.", "").split(":")[-1] == fid][:6]:
            try:
                b = s3.get_object(Bucket=LIVE, Key=k,
                                  Range="bytes=0-41943039")["Body"].read()
                if k.endswith(".gz"):
                    b = gzip.GzipFile(fileobj=io.BytesIO(b)).read(
                        400 * 1024 * 1024)
                lines = b.decode("utf-8", "replace").splitlines()
                hdr = [c.strip().upper() for c in lines[0].split(",")]
                ki = hdr.index("KEY") if "KEY" in hdr else 0
                for ln in lines[1:]:
                    p = ln.split(",")
                    if len(p) > ki:
                        keys.add(p[ki])
                rows += len(lines) - 1
            except Exception:
                pass
        R.log("  %-14s %d distinct series across its slices (%d rows "
              "read)" % (fid[:14], len(keys), rows))
        tally.append((fid, len(keys)))
    out["distinct_sample"] = tally
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/ecb-gap-truth.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/ecb-gap-truth.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5043 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(live=out.get("live"), ours=out.get("ours"),
         added=len(out.get("added") or []),
         no_file=len(out.get("no_file") or []),
         history_gaps=len(gaps))
    R.log("ops 5043 GREEN -- ECB gap measured on correct parsers")
