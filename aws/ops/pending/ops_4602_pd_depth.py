"""ops 4602 — PD full-history depth proof (Khalid: 1,500 series / 5MB).

markets-full v2 pulls every seriesbreak per keyid. This op runs ONE
sync tranche (~150 keys), then proves depth on a sampled key: multi-
break, n_obs in the hundreds-to-thousands, first date pre-2015, size
far beyond the 3KB stubs. Projects the converged footprint.
"""
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=640,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        b = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4602_pd_depth") as r:
        r.heading("ops 4602 — PD full-history depth")
        misses = 0

        st = gj("data/_state/fred-scoped-import.json")
        r.log("fred guard: scope=%s ver=%s imported=%s (untouched)"
              % (st.get("import_scope"), st.get("engine_version"),
                 st.get("series_imported")))

        r.section("1. Settle v2, run one sync tranche")
        t0 = time.time()
        while time.time() - t0 < 300:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-nyfed-markets-full"
                )["Configuration"]
                if c.get("LastUpdateStatus") == "Successful" \
                        and c.get("State") == "Active":
                    break
            except Exception:
                pass
            time.sleep(6)
        s0 = gj("data/warm/nyfed-markets/pd-state.json")
        r.log("  before: hist_v=%s done=%s status=%s"
              % (s0.get("hist_v"), len(s0.get("done") or []),
                 s0.get("status")))
        lam.invoke(FunctionName="justhodl-nyfed-markets-full",
                   InvocationType="RequestResponse")
        s1 = gj("data/warm/nyfed-markets/pd-state.json")
        nd = len(s1.get("done") or [])
        r.log("  after: hist_v=%s done=%d/%d status=%s breaks=%s"
              % (s1.get("hist_v"), nd,
                 len(s1.get("catalog") or []), s1.get("status"),
                 (s1.get("seriesbreaks") or [])[:8]))
        misses += contract(r, "pd", s1.get("hist_v") == 2 and nd >= 50,
                           "v2 tranche ran (%d keys deep-pulled)" % nd)

        r.section("2. Depth proof on a sampled key")
        sample = (s1.get("done") or [None])[0]
        if sample:
            k = "data/warm/nyfed-markets/pd/%s.json.gz" % sample
            head = s3.head_object(Bucket=B, Key=k)
            sz = head["ContentLength"]
            d = gj(k)
            r.log("  %s: n_obs=%s breaks_used=%s first=%s last=%s "
                  "size=%.1fKB gz"
                  % (sample, d.get("n_obs"), d.get("breaks_used"),
                     d.get("first"), d.get("last"), sz / 1024))
            misses += contract(r, "depth",
                               (d.get("n_obs") or 0) >= 300
                               and len(d.get("breaks_used") or []) >= 2
                               and str(d.get("first") or "9999") < "2016",
                               "full multi-break history (obs=%s, "
                               "back to %s)"
                               % (d.get("n_obs"), d.get("first")))
            proj = sz * len(s1.get("catalog") or [1539]) / 1e6
            r.log("  projected converged footprint ≈ %.0f MB "
                  "(vs the 5MB stub era)" % proj)
        else:
            misses += contract(r, "depth", False, "no done keys to sample")

        r.section("verdict")
        if misses:
            r.fail("pd depth: %d red" % misses)
            sys.exit(1)
        r.ok("PD full history flowing — hourly tranches converge all "
             "1,539 in ~10 runs; the 5MB mystery is closed")


if __name__ == "__main__":
    main()
