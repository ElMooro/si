"""ops 4659 — PD depth v3b: 4658 rerun with an honest yardstick.

4658 proved the mechanism (PDABTOT 697 obs to 2013-04-03 across 4
merged breaks, 6 validated ids, 0 failures, budget guard held) but
red-flagged itself on a naive MB projection extrapolated from
2022-vintage coupon-detail stubs. gzip crushes weekly history into
KB — MB was never the depth metric; observations and first-dates
are. Contract rewritten; the Event-kick doubles as one more
convergence tranche.

4602 sync-invoked the engine and outran boto3's read timeout; it never
saw that the v2 reconvergence had burned all 1,539 keys SHALLOW under
rev-2's label URLs. Engine v3 (this push): hist_v=3 re-queues the
worklist once, break ids are live-probe-validated, blocked-honest gate,
budget guard, config PD_TRANCHE 20->150. This op Event-kicks (never
sync), waits out the full first-tranche cycle, then proves depth from
the actual per-key docs: breaks_used, first asofdate, n_obs, projected
fleet footprint. Contracts fail loud; sys.exit(1) on any miss.
"""
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-nyfed-markets-full"
STATE = "data/warm/nyfed-markets/pd-state.json"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4659_pd_depth_v3") as r:
        r.heading("ops 4659 — PD full-history depth v3 (4602 redo)")
        misses = 0
        fst = gj("data/_state/fred-scoped-import.json")
        r.log("fred guard (untouched): ver=%s imported=%s status=%s"
              % (fst.get("engine_version"), fst.get("series_imported"),
                 fst.get("status")))

        r.section("1. Deploy settle — env PD_TRANCHE=150 is the signal")
        t0 = time.time()
        env = {}
        while time.time() - t0 < 360:
            try:
                c = lam.get_function(FunctionName=FN)["Configuration"]
                env = (c.get("Environment") or {}).get("Variables") or {}
                if (c.get("State") == "Active"
                        and c.get("LastUpdateStatus") == "Successful"
                        and env.get("PD_TRANCHE") == "150"):
                    break
            except Exception:
                pass
            time.sleep(10)
        r.log("  live env: %s" % env)
        misses += contract(r, "deploy", env.get("PD_TRANCHE") == "150",
                           "v3 config applied (PD_TRANCHE=%s)"
                           % env.get("PD_TRANCHE"))

        r.section("2. Event-kick + wait out the full first v3 cycle")
        before = gj(STATE)
        r.log("  before: hist_v=%s status=%s done=%s"
              % (before.get("hist_v"), before.get("status"),
                 len(before.get("done") or [])))
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event")
            r.log("  kicked (Event — 4602's sync-invoke death not "
                  "repeated)")
        except Exception as e:
            r.warn("  kick failed: %s" % str(e)[:90])
        st, flipped = {}, False
        t1 = time.time()
        while time.time() - t1 < 680:
            time.sleep(30)
            st = gj(STATE)
            if st.get("hist_v") == 3:
                flipped = True
                break
        r.log("  after %.0fs: hist_v=%s status=%s done=%d failures=%d "
              "shallow_n=%s budget=%s"
              % (time.time() - t1, st.get("hist_v"), st.get("status"),
                 len(st.get("done") or []),
                 len(st.get("failures") or {}),
                 st.get("shallow_n"), st.get("budget_break")))
        brks = st.get("seriesbreaks") or []
        r.log("  validated breaks (%d): %s" % (len(brks), brks[:12]))
        pm = st.get("seriesbreaks_probe") or {}
        bad = {k: v for k, v in pm.items() if v != "ok"}
        r.log("  probe map: %d ok, %d rejected (e.g. %s)"
              % (len(pm) - len(bad), len(bad),
                 dict(list(bad.items())[:4])))
        misses += contract(r, "flip", flipped,
                           "hist_v=3 live — worklist re-queued")
        misses += contract(r, "breaks",
                           len(brks) >= 2
                           and not any(" " in str(b) for b in brks),
                           "%d live-validated break ids, none are "
                           "labels" % len(brks))
        misses += contract(r, "block",
                           st.get("status") != "blocked-no-valid-breaks",
                           "not blocked (status=%s)" % st.get("status"))

        r.section("3. Depth proof from actual per-key docs")
        done = list(st.get("done") or [])
        idx = sorted({0, 1, 2, len(done) // 3, len(done) // 2,
                      len(done) - 1}) if done else []
        deep, curonly, sizes, nobs = 0, 0, [], []
        _docs = {}
        if done and "PDABTOT" in done:
            pi = done.index("PDABTOT")
            if pi not in idx:
                idx = [pi] + idx
        for i in idx:
            k = done[i]
            key = "data/warm/nyfed-markets/pd/%s.json.gz" % k
            try:
                raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
                d = json.loads(gzip.decompress(raw))
                bu = d.get("breaks_used") or []
                _docs[k] = d
                sizes.append(len(raw))
                nobs.append(int(d.get("n_obs") or 0))
                if bu == ["<current-only>"]:
                    curonly += 1
                elif len(bu) >= 2:
                    deep += 1
                r.log("  %s: hist_v=%s n_obs=%s first=%s last=%s "
                      "breaks=%s gz=%dB"
                      % (k, d.get("hist_v"), d.get("n_obs"),
                         d.get("first"), d.get("last"), bu, len(raw)))
            except Exception as e:
                r.warn("  %s: %s" % (k, str(e)[:80]))
        mean_gz = (sum(sizes) / len(sizes)) if sizes else 0
        mean_no = (sum(nobs) / len(nobs)) if nobs else 0
        proj_mb = mean_gz * 1539 / 1048576
        r.log("  sample: %d deep / %d current-only · mean n_obs=%.0f "
              "(shallow era ~110) · mean gz=%.0fB -> projected fleet "
              "footprint %.1f MB (was 5.07; tranche-1 skews young "
              "MBS-detail vintages, ancient cores land later)"
              % (deep, curonly, mean_no, mean_gz, proj_mb))
        misses += contract(r, "depth", deep >= 4,
                           "%d of %d sampled docs merged >=2 breaks"
                           % (deep, len(idx)))
        misses += contract(r, "depth", curonly <= 1,
                           "current-only fallbacks in sample: %d"
                           % curonly)
        misses += contract(r, "depth", mean_no >= 180,
                           "mean n_obs %.0f vs shallow-era ~110"
                           % mean_no)
        anc = sum(
            1 for i in idx
            if str(_docs.get(done[i], {}).get("first")
                   or "9999") <= "2016"
            and int(_docs.get(done[i], {}).get("n_obs")
                    or 0) >= 500)
        misses += contract(r, "depth", anc >= 1,
                           "%d sampled doc(s) reach pre-2016 with 500+ "
                           "obs — full-lineage proof (proj %.1f MB "
                           "logged, not contracted: gz makes MB the "
                           "wrong depth metric)" % (anc, proj_mb))
        misses += contract(r, "hygiene",
                           len(st.get("failures") or {}) < 30
                           and (st.get("shallow_n") or 0) <= 1,
                           "failures=%d shallow_n=%s"
                           % (len(st.get("failures") or {}),
                              st.get("shallow_n")))
        rem = max(0, 1539 - len(done))
        r.log("  ETA: %d remaining / 150 per hourly tranche ≈ %.0f h "
              "to full v3 depth" % (rem, rem / 150.0))

        r.section("verdict")
        if misses:
            r.fail("pd depth v3: %d red — see contracts" % misses)
            sys.exit(1)
        r.ok("depth PROVEN on live docs — hourly tranches converge the "
             "rest; data.html footprint climbs off 5.07 MB from the "
             "next provider-catalog refresh")


if __name__ == "__main__":
    main()
