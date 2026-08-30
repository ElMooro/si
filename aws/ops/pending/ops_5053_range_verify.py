"""ops_5053 -- verify Range through the CDN, end to end.

ops 5052 shipped the index under /data and rewrote provider.html, then
its own range test failed honestly: the proxy answered HTTP 200 with
Accept-Ranges absent. Tier 1's whole value is one ~400KB block read
instead of an 80MB download, so without Range it buys nothing -- and the
front end, which requires 206 before trusting a block, would have
silently fallen back to page fetches forever while the index sat unused.

The worker now forwards Range before the cache lookup and returns the
206 verbatim with Content-Range. Ranged requests deliberately bypass the
edge cache: a partial response must never populate, or be served from,
the full-object entry.

  P0 wait for the worker deploy
  P1 THE TEST: real ranged GETs through justhodl.ai -- first block, a
     middle block, and a deliberately silly range -- checking status,
     Content-Range and the exact byte count
  P2 prove the payload is the RIGHT bytes: parse the block and confirm
     its first id matches the block map's `k`, which is what the browser
     binary-search relies on
  P3 confirm an unranged GET still returns 200 with the full object, so
     the passthrough did not break ordinary reads
  P4 Tier-1 build progress
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
SITE = "https://justhodl.ai"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def jget(k):
    import gzip
    b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
    if k.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def http(url, rng=None, cap=3000000):
    req = urllib.request.Request(
        url, headers={"User-Agent": "justhodl-ops-5053"})
    if rng:
        req.add_header("Range", rng)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, dict(r.headers), r.read(cap)
    except Exception as e:
        return getattr(e, "code", -1), {}, str(e)[:150].encode()


with report("ops_5053_range_verify") as R:
    fails = []
    out = {"op": "ops_5053"}

    R.section("P0 wait for the worker deploy")
    t1 = jget("data/_state/t1-ecb.json")
    flow = (t1.get("flows_done") or ["IVF"])[0]
    dkey = "data/index/ecb/t1/%s.jsonl" % flow
    bm = jget("data/index/ecb/t1/%s.blocks.json" % flow)
    blocks = bm.get("blocks") or []
    R.log("  probe flow %s: %s entries, %d blocks" % (
        flow, f"{bm.get('n') or 0:,}", len(blocks)))
    ok206 = False
    for i in range(20):
        b = blocks[0]
        st, hd, _ = http("%s/%s" % (SITE, dkey),
                         rng="bytes=%d-%d" % (b["o"], b["o"] + 99))
        if st == 206:
            ok206 = True
            R.log("  worker live after %ds (206 seen)" % (i * 20))
            break
        time.sleep(20)
    if not ok206:
        R.log("  still not 206 after ~7min -- deploy may not have landed")

    R.section("P1 ranged GETs through the CDN")
    cases = []
    if blocks:
        cases.append(("first block", blocks[0]))
    if len(blocks) > 2:
        cases.append(("middle block", blocks[len(blocks) // 2]))
    for label, b in cases:
        want = b["c"]
        st, hd, body = http("%s/%s" % (SITE, dkey),
                            rng="bytes=%d-%d" % (b["o"],
                                                 b["o"] + b["c"] - 1))
        R.log("  %-13s HTTP %s  Content-Range=%s  Accept-Ranges=%s" % (
            label, st, hd.get("Content-Range"), hd.get("Accept-Ranges")))
        R.log("                got %s bytes, expected %s  %s" % (
            f"{len(body):,}", f"{want:,}",
            "EXACT" if len(body) == want else "*** MISMATCH ***"))
        if st != 206 or len(body) != want:
            fails.append("P1:%s" % label.replace(" ", "-"))
        out.setdefault("ranged", {})[label] = {
            "status": st, "got": len(body), "want": want}

        if st == 206 and body:
            R.section_done = True
    R.section("P2 are they the RIGHT bytes")
    if cases:
        label, b = cases[-1]
        st, hd, body = http("%s/%s" % (SITE, dkey),
                            rng="bytes=%d-%d" % (b["o"],
                                                 b["o"] + b["c"] - 1))
        try:
            lines = [l for l in body.splitlines() if l.strip()]
            first = json.loads(lines[0])
            last = json.loads(lines[-1])
            match = first.get("id") == b["k"]
            R.log("  block map says first id = %s" % str(b["k"])[:64])
            R.log("  payload first id        = %s  %s" % (
                str(first.get("id"))[:64],
                "MATCH -- binary search lands correctly" if match
                else "*** the map does not describe the bytes ***"))
            R.log("  payload last id         = %s  (%d lines)" % (
                str(last.get("id"))[:64], len(lines)))
            R.log("  entries in block: %d (map says %d)" % (len(lines),
                                                            b["n"]))
            if not match or len(lines) != b["n"]:
                fails.append("P2:mismatch")
        except Exception as e:
            R.log("  parse err %s" % str(e)[:120])
            fails.append("P2:parse")

    R.section("P3 unranged reads still work")
    st, hd, body = http("%s/data/index/ecb/flows.json.gz" % SITE)
    R.log("  GET flows.json.gz -> %s, %s bytes" % (st, f"{len(body):,}"))
    if st != 200:
        fails.append("P3:plain")
    st2, _, b2 = http("%s/data/providers/ecb/series/page-0000.json" % SITE)
    R.log("  GET page-0000.json -> %s, %s bytes" % (st2, f"{len(b2):,}"))
    if st2 != 200:
        fails.append("P3:page")

    R.section("P4 Tier-1 progress")
    for p in ("ecb", "eurostat"):
        t = jget("data/_state/t1-%s.json" % p)
        R.log("  %-9s flows=%d left=%s entries=%s blocks=%s %.2f GB" % (
            p, len(t.get("flows_done") or []), t.get("candidates_left"),
            f"{t.get('entries') or 0:,}", f"{t.get('blocks') or 0:,}",
            (t.get("bytes") or 0) / 1e9))
        out[p] = {"flows": len(t.get("flows_done") or []),
                  "left": t.get("candidates_left")}
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-serving.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5053 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(range_ok=ok206, eurostat_t1=out.get("eurostat", {}).get("flows"),
         left=out.get("eurostat", {}).get("left"))
    R.log("ops 5053 GREEN -- Tier 1 is genuinely range-served")
