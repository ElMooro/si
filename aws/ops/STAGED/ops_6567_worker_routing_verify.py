"""ops 6567 - READ-ONLY proof that the worker routing is live (Khalid, 2026-10-20: "route the pages through the worker";
CLAUDE.md rule 5: verify the served page, not the commit).

For a sample of rewritten pages: fetch them from https://justhodl.ai, count data reads that still point at the S3 host
versus https://justhodl.ai/data/, then fetch the first few of those data URLs through the Worker and record status,
cf-cache-status, content-type and size (two fetches of the same URL: the second should be an edge HIT).  Also fetches
one of the eight byte-pinned pages left on S3 by design, to show the exception is as documented.  No AWS writes.
"""
from pathlib import Path
import json
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SITE = "https://justhodl.ai"
PAGES = ["accumulation.html", "accuracy.html", "allocator.html", "alpha-council.html", "analogs.html", "auctions.html",
         "accum-composite.html", "activist-13d.html"]
PINNED = "bonds.html"          # left on S3 on purpose (byte-pinned by its preservation chain)
S3_HOST = re.compile(r"https?://(?:justhodl-dashboard-live\.s3(?:\.us-east-1)?\.amazonaws\.com|s3\.amazonaws\.com/justhodl-dashboard-live)/data/")
WORKER = re.compile(r"https://justhodl\.ai/data/[A-Za-z0-9_./-]+\.json(?:\.gz)?")
UA = "justhodl-ops-6567-verify (+https://github.com/ElMooro/si)"


def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as h:
            body = h.read()
            return h.status, dict((k.lower(), v) for k, v in h.headers.items()), body, round((time.time() - t0) * 1000)
    except urllib.error.HTTPError as e:
        return e.code, dict((k.lower(), v) for k, v in e.headers.items()), b"", round((time.time() - t0) * 1000)
    except Exception as e:  # noqa: BLE001
        return 0, {"error": type(e).__name__}, b"", round((time.time() - t0) * 1000)


def main():
    with report("ops_6567_worker_routing_verify") as r:
        r.heading("ops 6567 - worker routing verification (served pages + edge data fetches)")
        total_s3, total_worker, sample = 0, 0, []
        for page in PAGES:
            if not (ROOT / page).is_file():
                continue
            st, hd, body, ms = fetch("%s/%s" % (SITE, page))
            text = body.decode("utf-8", "replace")
            n_s3, n_w = len(S3_HOST.findall(text)), len(set(WORKER.findall(text)))
            total_s3 += n_s3
            total_worker += n_w
            sample += [u for u in sorted(set(WORKER.findall(text)))[:2]]
            r.log("%s: HTTP %s in %d ms, %d KB; S3-host data reads %d, worker data reads %d%s" % (
                page, st, ms, len(body) // 1024, n_s3, n_w, "" if st == 200 else " (NOT 200)"))
        st, hd, body, ms = fetch("%s/%s" % (SITE, PINNED))
        r.log("%s (byte-pinned, left on S3 by design): HTTP %s, S3-host data reads %d" % (PINNED, st, len(S3_HOST.findall(body.decode("utf-8", "replace")))))
        hits = 0
        seen = []
        for url in sample[:6]:
            a = fetch(url)
            b = fetch(url)
            ok = a[0] == 200 and b[0] == 200
            cache = (a[1].get("cf-cache-status", "-"), b[1].get("cf-cache-status", "-"))
            hits += 1 if ok and cache[1] in ("HIT", "REVALIDATED", "UPDATING") else 0
            seen.append(url)
            r.log("%s: HTTP %s/%s, cf-cache-status %s -> %s, %s, %s KB, %d/%d ms" % (
                url.replace(SITE, ""), a[0], b[0], cache[0], cache[1], a[1].get("content-type", "?"), len(a[2]) // 1024, a[3], b[3]))
        r.kv(pages_checked=len(PAGES), s3_host_data_reads_left=total_s3, worker_data_reads=total_worker,
             data_urls_fetched=len(seen), edge_hits_on_second_fetch=hits)
        if total_s3 == 0 and total_worker > 0 and seen and hits == len(seen):
            r.ok("served pages read data through the Worker and the edge serves repeats as HITs")
        elif total_s3 == 0 and total_worker > 0 and seen:
            r.warn("pages are routed; %d of %d data URLs were not edge HITs on the second fetch (cache fill or bypass - see log)" % (len(seen) - hits, len(seen)))
        else:
            r.fail("routing not proven (see log)")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
