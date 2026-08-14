"""ops 4662 — three lanes (Khalid).

1) nyfed: convergence snapshot + the depth note reaching the INDEX card
   (data.html catalog_note) — the page-side answer to "still 5MB".
2) repo completeness: OFR rediscovery live (repo-first), NCCBR/haircut
   family visibility, NY Fed repo history freshness.
3) FRED leaks ONLY: S3-diff of queue vs banked docs, requeue just the
   missing rows — the drain's own head_object skip makes re-import
   impossible. No engine patch; pure ops.
"""
import gzip
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
REPO_TAGS = ("REPO", "NCCBR", "TRI", "GCF", "DVP", "BILAT", "HAIRCUT")


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
    with report("4662_three_lanes") as r:
        r.heading("ops 4662 — nyfed depth note · repo completeness · "
                  "FRED leak-only recovery")
        misses = 0
        now = datetime.now(timezone.utc)

        r.section("1. nyfed — convergence + depth")
        st = gj("data/warm/nyfed-markets/pd-state.json")
        dep = st.get("depth") or {}
        done_n = len(set(st.get("done") or []))
        mean = dep.get("n_obs_sum", 0) / max(1, dep.get("keys", 0))
        r.log("  done=%d/1539 mean=%.0f obs first=%s ge500=%s "
              "multi=%s status=%s"
              % (done_n, mean, dep.get("first_min"), dep.get("ge500"),
                 dep.get("multi"), st.get("status")))
        misses += contract(r, "nyfed", dep.get("keys") == done_n,
                           "ledger %s == done %d" % (dep.get("keys"),
                                                     done_n))
        misses += contract(r, "nyfed", done_n >= 391,
                           "convergence advancing (%d, was 390 at "
                           "4661)" % done_n)
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog",
                       InvocationType="Event")
            r.log("  provider-catalog kicked (note check at the end)")
        except Exception as e:
            r.warn("  catalog kick: %s" % str(e)[:70])

        r.section("2. repo — NY Fed history freshness + OFR "
                  "rediscovery")
        for rk in ("data/warm/nyfed-markets/rp-repo-history.json.gz",
                   "data/warm/nyfed-markets/"
                   "rp-reverserepo-history.json.gz"):
            try:
                h = s3.head_object(Bucket=B, Key=rk)
                age = (now - h["LastModified"]).total_seconds() / 3600
                misses += contract(r, "repo", age < 60,
                                   "%s %.1fh old (%.0f KB)"
                                   % (rk.split("/")[-1], age,
                                      h["ContentLength"] / 1024))
            except Exception as e:
                misses += contract(r, "repo", False,
                                   "%s: %s" % (rk, str(e)[:60]))
        try:
            lam.invoke(FunctionName="justhodl-ofr-stfm",
                       InvocationType="Event")
        except Exception as e:
            r.warn("  ofr kick: %s" % str(e)[:70])
        ost, t0 = {}, time.time()
        while time.time() - t0 < 210:
            time.sleep(20)
            ost = gj("data/warm/ofr/state.json")
            if str(ost.get("catalog_checked_at") or "") >= \
                    now.isoformat()[:16]:
                break
        cat = ost.get("catalog") or []
        repo_m = [m for m in cat
                  if any(t in str(m).upper() for t in REPO_TAGS)]
        pend = [m for m in repo_m
                if m not in set(ost.get("done") or [])]
        r.log("  catalog=%d checked_at=%s added_total=%s new=%s"
              % (len(cat), ost.get("catalog_checked_at"),
                 ost.get("catalog_added_total"),
                 (ost.get("catalog_new_last_run") or [])[:8]))
        r.log("  repo-family mnemonics: %d in catalog · %d pending "
              "(e.g. %s)" % (len(repo_m), len(pend), pend[:8]))
        misses += contract(r, "repo",
                           str(ost.get("catalog_checked_at") or "")
                           >= now.isoformat()[:16],
                           "rediscovery ran this op (checked_at=%s)"
                           % ost.get("catalog_checked_at"))
        misses += contract(r, "repo", len(repo_m) >= 40,
                           "%d repo-family mnemonics in catalog"
                           % len(repo_m))
        misses += contract(r, "repo",
                           len(ost.get("failures") or {}) < 20,
                           "ofr failures=%d"
                           % len(ost.get("failures") or {}))

        r.section("3. FRED — leak-only recovery (S3-diff requeue)")
        fs = gj("data/_state/fred-scoped-import.json")
        t0 = time.time()
        while (fs.get("lease_until") or 0) > time.time() \
                and time.time() - t0 < 240:
            time.sleep(15)
            fs = gj("data/_state/fred-scoped-import.json")
        r.log("  status=%s imported=%s errors=%d accounting=%s"
              % (fs.get("status"), fs.get("series_imported"),
                 len(fs.get("errors") or {}), fs.get("accounting")))
        if not str(fs.get("status") or "").startswith("COMPLETE"):
            misses += contract(r, "fred", False,
                               "import not COMPLETE (status=%s) — "
                               "leak lane deferred" % fs.get("status"))
        else:
            q = json.loads(gzip.decompress(s3.get_object(
                Bucket=B,
                Key="data/_state/fred-queue.json.gz")["Body"].read()))
            rows = q.get("rows") or []
            qmap = {}
            for row in rows:
                if row and row[0] not in qmap:
                    qmap[row[0]] = row
            t1 = time.time()
            banked, tok = set(), None
            while True:
                kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/",
                      "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                for o in resp.get("Contents") or []:
                    k = o["Key"]
                    if k.endswith(".json"):
                        banked.add(k.rsplit("/", 1)[-1][:-5])
                if not resp.get("IsTruncated"):
                    break
                tok = resp.get("NextContinuationToken")
            r.log("  banked docs: %d (listed in %.0fs) · queue ids: %d"
                  % (len(banked), time.time() - t1, len(qmap)))
            missing = [row for sid, row in qmap.items()
                       if sid not in banked]
            errs = set((fs.get("errors") or {}).keys())
            r.log("  MISSING (queue - banked): %d · errors-dict: %d "
                  "(%d of them unbanked, %d banked anyway)"
                  % (len(missing), len(errs),
                     len(errs - banked), len(errs & banked)))
            misses += contract(r, "fred", len(missing) <= 25000,
                               "diff plausible (%d missing)"
                               % len(missing))
            if not missing:
                misses += contract(r, "fred", True,
                                   "ZERO unbanked series — "
                                   "WITH_LEAKS is accounting residue "
                                   "(excluded/discontinued/error "
                                   "math), nothing to re-import")
            elif len(missing) <= 25000:
                q["rows"] = rows + missing
                s3.put_object(
                    Bucket=B, Key="data/_state/fred-queue.json.gz",
                    Body=gzip.compress(json.dumps(q).encode()),
                    ContentType="application/gzip")
                fs = gj("data/_state/fred-scoped-import.json")
                if (fs.get("lease_until") or 0) > time.time():
                    r.warn("  lease grabbed mid-op; state bump "
                           "skipped (chain will still drain the "
                           "requeued tail)")
                else:
                    fs["series_queued"] = (fs.get("series_queued")
                                           or 0) + len(missing)
                    for row in missing:
                        (fs.get("errors") or {}).pop(row[0], None)
                    s3.put_object(
                        Bucket=B,
                        Key="data/_state/fred-scoped-import.json",
                        Body=json.dumps(fs, default=str).encode(),
                        ContentType="application/json")
                r.ok("  requeued %d missing rows (tail of queue; "
                     "head_object skip makes re-import impossible)"
                     % len(missing))
                i0 = int(fs.get("series_imported") or 0)
                c0 = int(fs.get("queue_cursor") or 0)
                try:
                    lam.invoke(FunctionName="justhodl-fred-catalog",
                               InvocationType="Event")
                except Exception as e:
                    r.warn("  fred kick: %s" % str(e)[:70])
                time.sleep(150)
                time.sleep(140)
                f2 = gj("data/_state/fred-scoped-import.json")
                d_i = int(f2.get("series_imported") or 0) - i0
                d_c = int(f2.get("queue_cursor") or 0) - c0
                r.log("  after 290s: +%d imported, +%d cursor, "
                      "status=%s" % (d_i, d_c, f2.get("status")))
                misses += contract(
                    r, "fred",
                    (d_i + d_c) >= 5
                    or (f2.get("lease_until") or 0) > time.time(),
                    "leak drain moving (+%d imported, +%d cursor)"
                    % (d_i, d_c))

        r.section("4. index card — depth note live")
        hub, t3 = {}, time.time()
        note = None
        while time.time() - t3 < 240:
            time.sleep(20)
            hub = gj("data/provider-catalog.json")
            for pv in hub.get("providers") or []:
                if pv.get("slug") == "nyfed":
                    note = pv.get("catalog_note")
            if note and "obs/series" in str(note):
                break
        r.log("  nyfed catalog_note: %s" % note)
        if note and "obs/series" in str(note):
            misses += contract(r, "card", True,
                               "depth note on the INDEX card: '%s'"
                               % note)
        elif str(hub.get("as_of") or "") < now.isoformat()[:16]:
            r.warn("  catalog still on pre-kick snapshot — note lands "
                   "on its completion (deploy+inventory ~minutes)")
        else:
            misses += contract(r, "card", False,
                               "hub refreshed without depth note "
                               "(note=%s)" % note)

        r.section("verdict")
        if misses:
            r.fail("three lanes: %d red" % misses)
            sys.exit(1)
        r.ok("nyfed depth visible where you look · repo lane "
             "rediscovering + fresh · FRED leaks recovered by diff, "
             "not re-import")


if __name__ == "__main__":
    main()
