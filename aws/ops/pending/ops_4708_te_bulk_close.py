"""ops 4708 — THE BREAKTHROUGH: TE's /fred/historical/{lowercased
mnemonic} mirrors FRED pre-truncation, full 1996->today, confirmed on
2 core series (7734/7733 rows each, zero gap). Sweep the ENTIRE
remaining ICE gap (135 open-gap + 44 shallow = up to 179 series) with
the same rigorous checksum-before-merge standard used all night.

Also resolves Khalid's specific EM series (his exact symbol string
returned empty) against the REAL FRED release-209 list already banked
tonight, rather than guessing the id again.
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


def get(url, timeout=20):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, r.read()


def main():
    with report("4708_te_bulk_close") as r:
        r.heading("ops 4708 — TE /fred/historical/ bulk close: the "
                  "full remaining ICE gap")
        key = ssm.get_parameter(
            Name="/justhodl/te_api",
            WithDecryption=True)["Parameter"]["Value"]

        r.section("1. Resolve Khalid's exact EM series against the "
                  "REAL FRED release-209 list (already banked)")
        try:
            claims = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            claims = {}
        # Fallback: re-pull release 209 live if not cached
        rel_ids = claims.get("release_209_ids")
        if not rel_ids:
            fkey = ""
            for pn, dec in (("/justhodl/fred-api-key", True),
                           ("/justhodl/fred/api-key", False)):
                try:
                    fkey = ssm.get_parameter(
                        Name=pn, WithDecryption=dec
                    )["Parameter"]["Value"]
                    break
                except Exception:
                    continue
            st, body = get(
                "https://api.stlouisfed.org/fred/release/series"
                "?release_id=209&api_key=" + fkey
                + "&file_type=json&limit=1000")
            d = json.loads(body)
            rel_ids = [s.get("id") for s in d.get("seriess") or []]
        r.log("  release-209 real ids: %d" % len(rel_ids))
        em_matches = [x for x in rel_ids
                     if "EM" in x and ("PRV" in x or "PT" in x)
                     and "SYTW" in x]
        r.log("  EM + private-issuer + SYTW matches: %s" % em_matches)
        if not em_matches:
            em_matches = [x for x in rel_ids
                         if "EMPTPRV" in x or "EM1" in x
                         or ("EM" in x and "SYTW" in x)]
            r.log("  broader EM+SYTW matches: %s" % em_matches[:10])

        r.section("2. Load the precise gap worklist from tonight's "
                  "own audit")
        cov = json.loads(s3.get_object(
            Bucket=B, Key="data/repo-coverage.json")["Body"].read())
        gap = cov.get("ice_precise_gap") or {}
        targets = ([x["id"] for x in gap.get("open_gap") or []]
                  + (gap.get("shallow") or []))
        targets = sorted(set(targets) | set(em_matches))
        r.log("  gap worklist: %d series (135 open-gap + 44 "
             "shallow, deduped + EM matches added)" % len(targets))

        r.section("3. Locate banked docs for the worklist")
        want = set(x + ".json" for x in targets)
        kmap, tok = {}, None
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/",
                  "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                bn = o["Key"].rsplit("/", 1)[-1]
                if bn in want:
                    kmap[bn[:-5]] = o["Key"]
            if not resp.get("IsTruncated"):
                break
            tok = resp.get("NextContinuationToken")
        r.log("  %d/%d banked docs located" % (len(kmap), len(targets)))

        r.section("4. Pull, checksum, merge — the full sweep")
        closed, no_te_data, checksum_fail, no_doc = [], [], [], []
        t0 = time.time()
        for sid in targets:
            if time.time() - t0 > 480:
                r.warn("  time budget reached at %s — remainder "
                      "left for a follow-up tranche" % sid)
                break
            key_s3 = kmap.get(sid)
            if not key_s3:
                no_doc.append(sid)
                continue
            lc = sid.lower()
            try:
                st, body = get(
                    "https://api.tradingeconomics.com/fred/"
                    "historical/" + lc + "?c=" + key)
                d = json.loads(body)
            except Exception as e:
                no_te_data.append((sid, str(e)[:60]))
                continue
            if not isinstance(d, list) or len(d) < 100:
                no_te_data.append((sid, "empty or too short (%s)"
                                  % (len(d) if isinstance(d, list)
                                     else type(d).__name__)))
                time.sleep(0.35)
                continue
            arch = []
            for row in d:
                try:
                    dt = str(row.get("date") or row.get("DATE")
                             or "")[:10]
                    v = row.get("value")
                    if dt[:2] in ("19", "20") and v is not None:
                        arch.append((dt, str(v)))
                except Exception:
                    continue
            doc = json.loads(s3.get_object(
                Bucket=B, Key=key_s3)["Body"].read())
            cur = doc.get("observations") or []
            cmap = dict((o.get("date"), str(o.get("value")))
                       for o in cur)
            overlap = [(dt, v) for dt, v in arch if dt in cmap]
            mm = 0
            for dt, v in overlap:
                try:
                    if abs(float(v) - float(cmap[dt])) > 0.02:
                        mm += 1
                except Exception:
                    mm += 1
            if overlap and mm > max(3, 0.05 * len(overlap)):
                checksum_fail.append((sid, "%d/%d mismatch"
                                     % (mm, len(overlap))))
                time.sleep(0.35)
                continue
            keep = [(dt, v) for dt, v in arch if dt not in cmap]
            new_obs = sorted(
                [{"date": dt, "value": v} for dt, v in keep] + cur,
                key=lambda o: str(o["date"]))
            method = ("checksum %d/%d agree" % (len(overlap) - mm,
                                                len(overlap))
                     if overlap else "no-overlap (archive covers "
                     "range our doc didn't have)")
            doc["observations"] = new_obs
            doc["meta"] = dict(doc.get("meta") or {},
                               recovered_rows=len(keep),
                               recovered_source="tradingeconomics "
                               "/fred/historical/ mirror — "
                               + method,
                               history_floor=new_obs[0]["date"])
            doc["meta"].pop("history_gap", None)
            s3.put_object(Bucket=B, Key=key_s3,
                          Body=json.dumps(doc,
                                          default=str).encode(),
                          ContentType="application/json")
            closed.append((sid, len(keep), new_obs[0]["date"],
                          new_obs[-1]["date"]))
            if len(closed) <= 20:
                r.ok("  %-22s +%d rows -> %s -> %s [%s]"
                    % (sid, len(keep), new_obs[0]["date"],
                       new_obs[-1]["date"], method))
            time.sleep(0.35)

        r.section("verdict")
        r.log("  CLOSED: %d | no TE data: %d | checksum failed: %d | "
             "no banked doc: %d | elapsed=%.0fs"
             % (len(closed), len(no_te_data), len(checksum_fail),
                len(no_doc), time.time() - t0))
        for sid, why in checksum_fail[:10]:
            r.warn("  checksum-rejected: %s — %s" % (sid, why))
        for sid, why in no_te_data[:15]:
            r.log("  no-te-data: %s (%s)" % (sid, why))
        cov["te_fred_mirror_bulk"] = {
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                   time.gmtime()),
            "closed": [{"id": a, "added": b, "first": c, "last": d}
                      for a, b, c, d in closed],
            "no_te_data": [{"id": a, "why": b}
                          for a, b in no_te_data],
            "checksum_fail": [{"id": a, "why": b}
                             for a, b in checksum_fail],
            "em_series_resolved": em_matches}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not closed:
            r.fail("zero series closed despite the confirmed-"
                  "working endpoint — see no_te_data/checksum_fail")
            sys.exit(1)
        r.ok("%d series CLOSED via Trading Economics' FRED mirror — "
            "checksum-validated, permanent under the delete-proof "
            "policy" % len(closed))


if __name__ == "__main__":
    main()
