"""ops 4672 — ICE deep backfill: 144 series x 1996-2017 (GMS_VAAS).

The find: fagan2888/GMS_VAAS holds 144 ICE BofA .txt files on the
MASTER branch (earlier sweeps used extension:csv and branch main —
which is exactly why they came back empty), each running 1996-12-31 ->
2017-01-12: the AAA/AA/A/BB/B/CCC, effective-yield, SYTW and
total-return families no free CSV archive was known to carry.

Honest limitation: the archive ENDS 2017-01-12 while our banked FRED
window BEGINS 2023-08-15, so for most series there is NO overlap to
splice-validate against and a 2017-2023 hole remains. Handling:
  - overlap present -> splice validated as usual;
  - overlap absent  -> merged with an explicit history_gap marker so
    no engine mistakes the series for continuous.
Union-merge only; delete-proof policy + merge guard keep it permanent.
"""
import json
import sys
import time
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
RAW = ("https://raw.githubusercontent.com/fagan2888/GMS_VAAS/master/"
       "IR_txt_2/data/%s.txt")
IDS = ["BAMLC0A0CM", "BAMLC0A0CMEY", "BAMLC0A0CMSYTW", "BAMLC0A1CAAA", "BAMLC0A1CAAAEY", "BAMLC0A1CAAASYTW", "BAMLC0A2CAA", "BAMLC0A2CAAEY", "BAMLC0A2CAASYTW", "BAMLC0A3CA", "BAMLC0A3CAEY", "BAMLC0A3CASYTW", "BAMLC0A4CBBB", "BAMLC0A4CBBBEY", "BAMLC0A4CBBBSYTW", "BAMLC1A0C13Y", "BAMLC1A0C13YEY", "BAMLC1A0C13YSYTW", "BAMLC2A0C35Y", "BAMLC2A0C35YEY", "BAMLC2A0C35YSYTW", "BAMLC3A0C57Y", "BAMLC3A0C57YEY", "BAMLC3A0C57YSYTW", "BAMLC4A0C710Y", "BAMLC4A0C710YEY", "BAMLC4A0C710YSYTW", "BAMLC7A0C1015Y", "BAMLC7A0C1015YEY", "BAMLC7A0C1015YSYTW", "BAMLC8A0C15PY", "BAMLC8A0C15PYEY", "BAMLC8A0C15PYSYTW", "BAMLEM1BRRAAA2ACRPIEY", "BAMLEM1BRRAAA2ACRPIOAS", "BAMLEM1BRRAAA2ACRPISYTW", "BAMLEM1RAAA2ALCRPIUSEY", "BAMLEM1RAAA2ALCRPIUSOAS", "BAMLEM1RAAA2ALCRPIUSSYTW", "BAMLEM2BRRBBBCRPIEY", "BAMLEM2BRRBBBCRPIOAS", "BAMLEM2BRRBBBCRPISYTW", "BAMLEM2RBBBLCRPIUSEY", "BAMLEM2RBBBLCRPIUSOAS", "BAMLEM2RBBBLCRPIUSSYTW", "BAMLEM3BRRBBCRPIEY", "BAMLEM3BRRBBCRPIOAS", "BAMLEM3BRRBBCRPISYTW", "BAMLEM3RBBLCRPIUSEY", "BAMLEM3RBBLCRPIUSOAS", "BAMLEM3RBBLCRPIUSSYTW", "BAMLEM4BRRBLCRPIEY", "BAMLEM4BRRBLCRPIOAS", "BAMLEM4BRRBLCRPISYTW", "BAMLEM4RBLLCRPIUSEY", "BAMLEM4RBLLCRPIUSOAS", "BAMLEM4RBLLCRPIUSSYTW", "BAMLEM5BCOCRPIEY", "BAMLEM5BCOCRPIOAS", "BAMLEM5BCOCRPISYTW", "BAMLEMALLCRPIASIAUSEY", "BAMLEMALLCRPIASIAUSOAS", "BAMLEMALLCRPIASIAUSSYTW", "BAMLEMCBPIEY", "BAMLEMCBPIOAS", "BAMLEMCBPISYTW", "BAMLEMCLLCRPIUSEY", "BAMLEMCLLCRPIUSOAS", "BAMLEMCLLCRPIUSSYTW", "BAMLEMEBCRPIEEY", "BAMLEMEBCRPIEOAS", "BAMLEMEBCRPIESYTW", "BAMLEMELLCRPIEMEAUSEY", "BAMLEMELLCRPIEMEAUSOAS", "BAMLEMELLCRPIEMEAUSSYTW", "BAMLEMFLFLCRPIUSEY", "BAMLEMFLFLCRPIUSOAS", "BAMLEMFLFLCRPIUSSYTW", "BAMLEMFSFCRPIEY", "BAMLEMFSFCRPIOAS", "BAMLEMFSFCRPISYTW", "BAMLEMHBHYCRPIEY", "BAMLEMHBHYCRPIOAS", "BAMLEMHBHYCRPISYTW", "BAMLEMHGHGLCRPIUSEY", "BAMLEMHGHGLCRPIUSOAS", "BAMLEMHGHGLCRPIUSSYTW", "BAMLEMHYHYLCRPIUSEY", "BAMLEMHYHYLCRPIUSOAS", "BAMLEMHYHYLCRPIUSSYTW", "BAMLEMIBHGCRPIEY", "BAMLEMIBHGCRPIOAS", "BAMLEMIBHGCRPISYTW", "BAMLEMLLLCRPILAUSEY", "BAMLEMLLLCRPILAUSOAS", "BAMLEMLLLCRPILAUSSYTW", "BAMLEMNFNFLCRPIUSEY", "BAMLEMNFNFLCRPIUSOAS", "BAMLEMNFNFLCRPIUSSYTW", "BAMLEMNSNFCRPIEY", "BAMLEMNSNFCRPIOAS", "BAMLEMNSNFCRPISYTW", "BAMLEMPBPUBSICRPIEY", "BAMLEMPBPUBSICRPIOAS", "BAMLEMPBPUBSICRPISYTW", "BAMLEMPTPRVICRPIEY", "BAMLEMPTPRVICRPIOAS", "BAMLEMPTPRVICRPISYTW", "BAMLEMPUPUBSLCRPIUSEY", "BAMLEMPUPUBSLCRPIUSOAS", "BAMLEMPUPUBSLCRPIUSSYTW", "BAMLEMPVPRIVSLCRPIUSEY", "BAMLEMPVPRIVSLCRPIUSOAS", "BAMLEMPVPRIVSLCRPIUSSYTW", "BAMLEMRACRPIASIAEY", "BAMLEMRACRPIASIAOAS", "BAMLEMRACRPIASIASYTW", "BAMLEMRECRPIEMEAEY", "BAMLEMRECRPIEMEAOAS", "BAMLEMRECRPIEMEASYTW", "BAMLEMRLCRPILAEY", "BAMLEMRLCRPILAOAS", "BAMLEMRLCRPILASYTW", "BAMLEMUBCRPIUSEY", "BAMLEMUBCRPIUSOAS", "BAMLEMUBCRPIUSSYTW", "BAMLEMXOCOLCRPIUSEY", "BAMLEMXOCOLCRPIUSOAS", "BAMLEMXOCOLCRPIUSSYTW", "BAMLH0A0HYM2", "BAMLH0A0HYM2EY", "BAMLH0A0HYM2SYTW", "BAMLH0A1HYBB", "BAMLH0A1HYBBEY", "BAMLH0A1HYBBSYTW", "BAMLH0A2HYB", "BAMLH0A2HYBEY", "BAMLH0A2HYBSYTW", "BAMLH0A3HYC", "BAMLH0A3HYCEY", "BAMLH0A3HYCSYTW", "BAMLHE00EHYIEY", "BAMLHE00EHYIOAS", "BAMLHE00EHYISYTW"]


def get(url):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(rq, timeout=40) as f:
        return f.read().decode("utf-8", "replace")


def parse(txt):
    """FRED .txt export: header block, then 'YYYY-MM-DD   value'."""
    out = []
    for ln in txt.splitlines():
        if ln[:2] not in ("19", "20"):
            continue
        parts = ln.split()
        if len(parts) >= 2 and parts[1] not in (".", ""):
            try:
                float(parts[1])
            except ValueError:
                continue
            out.append({"date": parts[0], "value": parts[1]})
    return out


def main():
    with report("4672_ice_deep") as r:
        r.heading("ops 4672 — ICE deep backfill 1996-2017 (144 series)")
        misses = 0

        r.section("1. Locate banked docs for the 144 ids")
        want = set(x + ".json" for x in IDS)
        kmap, tok, t0 = {}, None, time.time()
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
        r.log("  matched %d/%d banked docs in %.0fs"
              % (len(kmap), len(IDS), time.time() - t0))
        if len(kmap) < 100:
            misses += 1
            r.fail("  only %d docs matched — id set or prefix wrong"
                   % len(kmap))

        r.section("2. Fetch, splice-check, merge")
        merged_n = gapped = spliced = skipped = failed = 0
        earliest = "9999"
        for sid in IDS:
            key = kmap.get(sid)
            if not key:
                skipped += 1
                continue
            try:
                arch = parse(get(RAW % sid))
            except Exception as e:
                failed += 1
                r.warn("  %s fetch: %s" % (sid, str(e)[:60]))
                continue
            if len(arch) < 3000:
                failed += 1
                r.warn("  %s: only %d rows parsed — skipped"
                       % (sid, len(arch)))
                continue
            doc = json.loads(s3.get_object(
                Bucket=B, Key=key)["Body"].read())
            cur = doc.get("observations") or []
            cmap = dict((o.get("date"), str(o.get("value")))
                        for o in cur)
            ov = [a for a in arch if a["date"] in cmap]
            mm = 0
            for a in ov[-150:]:
                try:
                    if abs(float(a["value"])
                           - float(cmap[a["date"]])) > 0.02:
                        mm += 1
                except Exception:
                    mm += 1
            if ov and mm > max(3, 0.05 * len(ov[-150:])):
                failed += 1
                r.warn("  %s: splice REJECTED %d/%d — not merged"
                       % (sid, mm, len(ov[-150:])))
                continue
            keep = [a for a in arch if a["date"] not in cmap]
            if not keep:
                skipped += 1
                continue
            new = sorted(keep + cur, key=lambda o: str(o["date"]))
            meta = dict(doc.get("meta") or {},
                        recovered_rows=len(keep),
                        recovered_source="github:fagan2888/GMS_VAAS@"
                                         "master (FRED .txt export "
                                         "1996->2017-01-12)",
                        history_floor=new[0]["date"], merge_v=1)
            if ov:
                spliced += 1
                meta["splice_validated"] = ("%d/%d"
                                            % (len(ov[-150:]) - mm,
                                               len(ov[-150:])))
            else:
                gapped += 1
                a_last = max(a["date"] for a in arch)
                c_first = min(cmap) if cmap else None
                meta["history_gap"] = ("%s..%s — archive ends before "
                                       "our FRED window begins; no "
                                       "overlap validation possible"
                                       % (a_last, c_first))
            doc["observations"] = new
            doc["meta"] = meta
            s3.put_object(Bucket=B, Key=key,
                          Body=json.dumps(doc, default=str).encode(),
                          ContentType="application/json")
            merged_n += 1
            earliest = min(earliest, new[0]["date"])
            if merged_n <= 12:
                r.log("  %-22s +%d rows -> %d obs since %s%s"
                      % (sid, len(keep), len(new), new[0]["date"],
                         " [GAP]" if not ov else " [spliced]"))
            time.sleep(0.15)

        r.section("3. Result")
        r.log("  merged=%d (spliced=%d gap-marked=%d) skipped=%d "
              "failed=%d earliest=%s"
              % (merged_n, spliced, gapped, skipped, failed,
                 earliest))
        if merged_n < 100:
            misses += 1
            r.fail("  only %d merged of %d located"
                   % (merged_n, len(kmap)))
        else:
            r.ok("  %d ICE series now carry 1996-era history"
                 % merged_n)
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/repo-coverage.json")["Body"].read())
        except Exception:
            cov = {}
        cov["ice_backfill"] = {
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                   time.gmtime()),
            "series_merged": merged_n, "splice_validated": spliced,
            "gap_marked": gapped, "earliest": earliest,
            "source": "fagan2888/GMS_VAAS@master",
            "known_hole": "2017-01-13..2023-08-14 on gap-marked "
                          "series"}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")

        r.section("verdict")
        if misses:
            r.fail("ice deep: %d red" % misses)
            sys.exit(1)
        r.ok("ICE family recovered to %s across %d series; the "
             "2017-2023 hole is marked, not hidden"
             % (earliest, merged_n))


if __name__ == "__main__":
    main()
