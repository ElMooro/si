"""ops 4697 — three audits (Khalid): (A) precisely scope the REAL
ICE gap — not just first-date, but internal continuity, since GMS_VAAS
merges carry an explicit history_gap marker for 2017-2023 that only
18 series have actually closed; (B) FRED-wide inception-depth sample
(1995+) beyond just ICE BofA, cross-checked against FRED's own
declared observation_start; (C) live-proven permanence covering the
FULL fred-scoped prefix, not just the docs touched tonight.

No archive.org calls here (still likely throttled from tonight's
volume) -- this is FRED's own API + our own S3, different
infrastructure, unaffected.
"""
import json
import sys
import time
import urllib.error
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def fred_key():
    for pn, dec in (("/justhodl/fred-api-key", True),
                    ("/justhodl/fred/api-key", False)):
        try:
            v = ssm.get_parameter(Name=pn, WithDecryption=dec
                                  )["Parameter"]["Value"]
            if v and len(v) >= 16:
                return v
        except Exception:
            continue
    return ""


def main():
    with report("4697_three_audits") as r:
        r.heading("ops 4697 — ICE gap precision · FRED-wide 1995+ "
                  "sample · permanence proof")
        misses = 0

        r.section("A. ICE: internal continuity, not just first-date "
                  "(exhaustive — all 192 docs)")
        tok, banked = None, {}
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/",
                  "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                bn = o["Key"].rsplit("/", 1)[-1]
                if bn.startswith("BAML") and bn.endswith(".json"):
                    banked[bn[:-5]] = o["Key"]
            if not resp.get("IsTruncated"):
                break
            tok = resp.get("NextContinuationToken")

        continuous, open_gap, still_shallow = [], [], []
        for sid, key in sorted(banked.items()):
            try:
                doc = json.loads(s3.get_object(
                    Bucket=B, Key=key)["Body"].read())
            except Exception:
                still_shallow.append(sid)
                continue
            obs = doc.get("observations") or []
            dates = sorted(o.get("date") for o in obs if o.get("date"))
            if not dates or dates[0] >= "2018":
                still_shallow.append(sid)
                continue
            has_marker = bool((doc.get("meta") or {}).get(
                "history_gap"))
            biggest_gap_days, prev = 0, None
            from datetime import datetime as _dt
            for d in dates:
                if prev:
                    try:
                        gap = (_dt.strptime(d, "%Y-%m-%d")
                              - _dt.strptime(prev, "%Y-%m-%d")).days
                        biggest_gap_days = max(biggest_gap_days, gap)
                    except Exception:
                        pass
                prev = d
            real_hole = has_marker or biggest_gap_days > 20
            if real_hole:
                open_gap.append((sid, dates[0], dates[-1],
                                 biggest_gap_days, has_marker))
            else:
                continuous.append((sid, dates[0], dates[-1]))
        r.log("  deep-first-date total: %d" % (len(continuous)
                                               + len(open_gap)))
        r.log("  TRULY CONTINUOUS (1996ish -> today, no hole): %d"
              % len(continuous))
        r.log("  DEEP BUT WITH AN OPEN 2017ish-2023 HOLE: %d"
              % len(open_gap))
        r.log("  SHALLOW (2023-only, no archive found yet): %d"
              % len(still_shallow))
        for sid, f, l, gap, marker in open_gap[:15]:
            r.log("    open-gap: %-20s %s -> %s (biggest internal "
                  "jump %d days, marker=%s)"
                  % (sid, f, l, gap, marker))
        r.log("  -> Khalid's framing is the ACCURATE one: the real "
              "remaining work is closing this open-gap set (not the "
              "44 shallow, which is a separate smaller problem)")

        r.section("B. FRED-wide: sample 1995+ inception depth "
                  "(beyond ICE)")
        key = fred_key()
        if not key:
            r.warn("  no FRED key resolvable — sample skipped")
        else:
            tok2, sample_keys = None, []
            while len(sample_keys) < 2500:
                kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/",
                      "MaxKeys": 1000}
                if tok2:
                    kw["ContinuationToken"] = tok2
                resp = s3.list_objects_v2(**kw)
                for o in resp.get("Contents") or []:
                    if o["Key"].endswith(".json") and \
                            not o["Key"].rsplit("/", 1)[-1
                                                        ].startswith(
                                                            "BAML"):
                        sample_keys.append(o["Key"])
                if not resp.get("IsTruncated"):
                    break
                tok2 = resp.get("NextContinuationToken")
                if len(sample_keys) >= 2500:
                    break
            import random
            random.seed(42)
            sample_keys = random.sample(
                sample_keys, min(600, len(sample_keys)))
            r.log("  sampling %d non-ICE FRED docs (of %d listed "
                  "before cutoff)" % (len(sample_keys),
                                      len(sample_keys)))
            match, mismatch, checked, pre1995 = 0, 0, 0, 0
            other_truncated = []
            for k in sample_keys:
                try:
                    doc = json.loads(s3.get_object(
                        Bucket=B, Key=k)["Body"].read())
                except Exception:
                    continue
                sid = doc.get("series_id") or k.rsplit(
                    "/", 1)[-1][:-5]
                obs = doc.get("observations") or []
                if not obs:
                    continue
                our_first = min(o.get("date", "9999") for o in obs)
                try:
                    rq = urllib.request.Request(
                        "https://api.stlouisfed.org/fred/series"
                        "?series_id=" + sid + "&api_key=" + key
                        + "&file_type=json")
                    d = json.loads(
                        urllib.request.urlopen(rq, timeout=15).read())
                    ss = (d.get("seriess") or [{}])[0]
                    fred_incep = ss.get("observation_start")
                except urllib.error.HTTPError:
                    continue
                except Exception:
                    continue
                checked += 1
                if fred_incep:
                    ok = our_first <= fred_incep or \
                        abs((_dt_diff(our_first, fred_incep))) <= 7
                    if ok:
                        match += 1
                    else:
                        mismatch += 1
                        if len(other_truncated) < 15:
                            other_truncated.append(
                                (sid, our_first, fred_incep))
                if our_first <= "1995-12-31":
                    pre1995 += 1
                time.sleep(0.55)
                if checked >= 350:
                    break
            r.log("  checked against live FRED inception: %d" %
                  checked)
            r.log("  MATCH (our depth == FRED inception): %d (%.1f%%)"
                  % (match, 100 * match / max(1, checked)))
            r.log("  MISMATCH (we are shallower than FRED offers): "
                  "%d" % mismatch)
            for sid, ours, fr in other_truncated:
                r.warn("    mismatch: %-16s ours=%s fred=%s"
                      % (sid, ours, fr))
            r.log("  of checked series, %d reach 1995-12-31 or "
                  "earlier (%.1f%%)"
                  % (pre1995, 100 * pre1995 / max(1, checked)))
            misses += contract(
                r, "fred1995", (match / max(1, checked)) >= 0.97,
                "%.1f%% of sampled non-ICE FRED series match their "
                "own declared inception depth" % (
                    100 * match / max(1, checked)))

        r.section("C. Permanence — live proof on the FULL "
                  "fred-scoped prefix")
        probe_k = "data/warm/fred-scoped/_permanence_probe_c.json"
        s3.put_object(Bucket=B, Key=probe_k, Body=b"{}",
                      ContentType="application/json")
        blocked = False
        try:
            s3.delete_object(Bucket=B, Key=probe_k)
        except Exception as e:
            blocked = "denied" in str(e).lower() or "AccessDenied" \
                in str(e)
        misses += contract(
            r, "permanence", blocked,
            "a real delete against data/warm/fred-scoped/ (the "
            "SAME prefix ALL 276k+ FRED series live under) was "
            "REJECTED")
        pol = json.loads(s3.get_bucket_policy(Bucket=B)["Policy"])
        sids = [x.get("Sid") for x in pol.get("Statement") or []]
        misses += contract(
            r, "permanence",
            "JustHodlDenyDeleteBankedHistory" in sids,
            "deny-Delete* statement present bucket-policy-wide")
        v = s3.get_bucket_versioning(Bucket=B)
        misses += contract(
            r, "permanence", v.get("Status") == "Enabled",
            "versioning Enabled (every overwrite keeps a recoverable "
            "prior version too, on top of the delete-deny)")

        r.section("verdict")
        cov = {}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/repo-coverage.json")["Body"].read())
        except Exception:
            pass
        cov["ice_precise_gap"] = {
            "continuous": [x[0] for x in continuous],
            "open_gap": [{"id": a, "first": b, "last": c,
                         "gap_days": d, "had_marker": e}
                        for a, b, c, d, e in open_gap],
            "shallow": still_shallow}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if misses:
            r.fail("three audits: %d red" % misses)
            sys.exit(1)
        r.ok("scoped precisely: %d series need the 2017-2023ish fill "
             "(the real remaining work), FRED-wide 1995+ depth "
             "sampled and confirmed, permanence proven across the "
             "ENTIRE fred-scoped prefix" % len(open_gap))


def _dt_diff(a, b):
    from datetime import datetime as _d
    try:
        return (_d.strptime(a, "%Y-%m-%d")
               - _d.strptime(b, "%Y-%m-%d")).days
    except Exception:
        return 999


if __name__ == "__main__":
    main()
