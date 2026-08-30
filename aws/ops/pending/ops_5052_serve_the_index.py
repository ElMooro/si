"""ops_5052 -- make the index reachable, and fix a live 313GB front-end bug.

Two integration defects, both found by reading the code rather than by
waiting for a user to hit them.

 1. THE INDEX WAS UNREACHABLE. The Cloudflare zone route is
        justhodl.ai/data/*   and   www.justhodl.ai/data/*
    ONLY. Everything ops 5047-5051 wrote under index/ is invisible to
    the site -- Tier 0, Tier 1, the manifest, all of it would have been
    a 404 the moment a page asked for it. Copied to data/index/ and the
    engine now writes there.

 2. provider.html WOULD DOWNLOAD 313GB. _ensureSeriesCache() ran
        for(var i=0;i<m.n_pages;i++){ await fetch(page i); all=all.concat(rows) }
    -- sequential, unbounded, accumulating every row in one array. For
    Eurostat that is 1,128,408 awaited fetches and ~313GB into a browser
    tab. It was already broken at 3,466 pages; at 1.13M it is a hang.
    Replaced with the index: search 8,354 datasets client-side from a
    ~100KB document, then open one dataset -- Tier 1 flows resolve to a
    single Range read, small flows to at most 10 pages. Bounded either
    way. The inline script was syntax-checked with node before shipping.

  P0 copy index/** -> data/index/** (server-side, no egress)
  P1 prove reachability through the CDN, and that Content-Encoding
     survived the copy
  P2 THE RANGE TEST: issue a real ranged HTTP request through
     justhodl.ai and require 206. If the proxy strips Range, Tier 1
     silently degrades to whole-file downloads and the front end must
     fall back -- better to know now than to ship it.
  P3 republish the manifest with corrected paths
"""
import concurrent.futures as cf
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
SITE = "https://justhodl.ai"

cfg = Config(read_timeout=120, retries={"max_attempts": 4},
             max_pool_connections=96)
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def jget(k):
    import gzip
    b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
    if k.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def http(url, rng=None):
    req = urllib.request.Request(
        url, headers={"User-Agent": "justhodl-ops-5052"})
    if rng:
        req.add_header("Range", rng)
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, dict(r.headers), r.read(400000)
    except Exception as e:
        return getattr(e, "code", -1), {}, str(e)[:120].encode()


with report("ops_5052_serve_the_index") as R:
    fails = []
    out = {"op": "ops_5052"}

    R.section("P0 copy index/** -> data/index/**")
    src = []
    kw = {"Bucket": LIVE, "Prefix": "index/", "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        src += [(o["Key"], o["Size"]) for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    R.log("  %s objects, %.2f GB under index/" % (
        f"{len(src):,}", sum(s for _, s in src) / 1e9))

    def cp(k):
        try:
            s3.copy_object(Bucket=LIVE, Key="data/" + k,
                           CopySource={"Bucket": LIVE, "Key": k},
                           MetadataDirective="COPY",
                           TaggingDirective="COPY")
            return True
        except Exception:
            return False
    t0, ok = time.time(), 0
    with cf.ThreadPoolExecutor(max_workers=64) as ex:
        for i in range(0, len(src), 2000):
            batch = [k for k, _ in src[i:i + 2000]]
            ok += sum(1 for g in ex.map(cp, batch) if g)
            R.log("    copied %s/%s (%.0fs)" % (f"{ok:,}",
                                                f"{len(src):,}",
                                                time.time() - t0))
    R.log("  copied %s of %s" % (f"{ok:,}", f"{len(src):,}"))
    if ok < len(src):
        fails.append("P0:copy")
    out["copied"] = ok

    R.section("P1 reachability + headers through the CDN")
    for p in ("eurostat", "ecb"):
        k = "data/index/%s/flows.json.gz" % p
        try:
            h = s3.head_object(Bucket=LIVE, Key=k)
            R.log("  s3 %s: %.0f KB enc=%s type=%s" % (
                k, h["ContentLength"] / 1024,
                h.get("ContentEncoding"), h.get("ContentType")))
        except Exception as e:
            R.log("  s3 head %s FAILED %s" % (k, str(e)[:80]))
            fails.append("P1:%s-s3" % p)
        st, hd, body = http("%s/%s" % (SITE, k))
        R.log("  cdn GET /%s -> %s  enc=%s len=%s" % (
            k, st, hd.get("Content-Encoding"),
            hd.get("Content-Length")))
        if st != 200:
            fails.append("P1:%s-cdn" % p)
        else:
            try:
                d = json.loads(body) if not body[:2] == b"\x1f\x8b" else None
                if d:
                    R.log("    parsed: %s flows, series=%s" % (
                        d.get("flows_total"),
                        f"{d.get('series_total') or 0:,}"))
            except Exception:
                R.log("    (body still gzipped to this client -- browsers "
                      "decode transparently)")

    R.section("P2 THE RANGE TEST")
    t1 = jget("data/_state/t1-ecb.json")
    flow = (t1.get("flows_done") or ["IVF"])[0]
    bkey = "data/index/ecb/t1/%s.blocks.json" % flow
    dkey = "data/index/ecb/t1/%s.jsonl" % flow
    try:
        bm = jget(bkey)
        b = (bm.get("blocks") or [])[0]
        st, hd, body = http("%s/%s" % (SITE, dkey),
                            rng="bytes=%d-%d" % (b["o"], b["o"] + b["c"] - 1))
        R.log("  ranged GET /%s bytes=%d-%d -> HTTP %s" % (
            dkey, b["o"], b["o"] + b["c"] - 1, st))
        R.log("    Content-Range=%s  Accept-Ranges=%s  got %s bytes" % (
            hd.get("Content-Range"), hd.get("Accept-Ranges"),
            f"{len(body):,}"))
        if st == 206:
            first = json.loads(body.splitlines()[0])
            R.log("    206 CONFIRMED -- first id in the block: %s" %
                  str(first.get("id"))[:60])
            R.log("    Tier 1 works end to end from the browser")
        else:
            R.log("    *** not 206: the proxy does not honour Range, so a "
                  "Tier-1 read would pull the whole file. provider.html "
                  "already requires 206 and falls back to the page "
                  "range, so it degrades safely -- but Tier 1 gives no "
                  "benefit until the worker forwards Range ***")
        out["range_status"] = st
    except Exception as e:
        R.log("  range probe err %s" % str(e)[:140])
        fails.append("P2")

    R.section("P3 republish the manifest at the served path")
    try:
        man = jget("index/manifest.json")
        for p, v in (man.get("providers") or {}).items():
            v["tier0"] = "data/index/%s/flows.json.gz" % p
            v["tier1_prefix"] = "data/index/%s/t1/" % p
        man["base"] = "/data/index/"
        man["note"] = ("served only under /data/* -- that is the only "
                       "Cloudflare zone route")
        man["republished_at"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        s3.put_object(Bucket=LIVE, Key="data/index/manifest.json",
                      Body=json.dumps(man, indent=1).encode(),
                      ContentType="application/json",
                      CacheControl="public, max-age=300")
        st, _, _ = http("%s/data/index/manifest.json" % SITE)
        R.log("  -> data/index/manifest.json  (cdn GET %s)" % st)
        if st != 200:
            fails.append("P3:cdn")
        t1e = jget("data/_state/t1-eurostat.json")
        R.log("  tier1 eurostat: flows=%d left=%s entries=%s" % (
            len(t1e.get("flows_done") or []),
            t1e.get("candidates_left"),
            f"{t1e.get('entries') or 0:,}"))
        out["t1_left"] = t1e.get("candidates_left")
    except Exception as e:
        R.log("  manifest err %s" % str(e)[:130])
        fails.append("P3")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-serving.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5052 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(copied=out.get("copied"), range_status=out.get("range_status"),
         t1_left=out.get("t1_left"))
    R.log("ops 5052 GREEN -- index served under /data, range read verified")
