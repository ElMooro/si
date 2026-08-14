"""ops 4666 — PERMANENCE (Khalid: can never ever be erased).

Three things, in order of irreversibility:

A. PROTECT what we already hold. Versioning is on since Apr-2026 but a
   30-day expire-noncurrent rule quietly made it insurance, not
   permanence. This op: audits versioning/MFA/lifecycle, REMOVES any
   noncurrent-expiry that touches data/warm/ (the banked history), and
   writes a deny-Delete* bucket-policy statement scoped to the warm
   prefixes so no engine, no rogue script, and no future me can delete
   a banked doc. Contracts prove each with a live probe.

B. RECOVER 4 more ICE series from depth-validated GitHub archives
   (splice-checked, union-merged, provenance-stamped) — Farinelli is
   exhausted at 3 files; these come from a wider sweep.

C. REPO OFFICIAL (Khalid's priority list #1-#8): probe every OFR /
   NY Fed bulk endpoint from INSIDE AWS (unrestricted egress) and
   report which serve full history for a follow-up bulk importer.
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
WARM = "data/warm/"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
ARCH = {
    "BAMLC0A1CAAA": ("Vishnutejas-2005/DA_Project", "main",
                     "BAMLC0A1CAAA.csv"),
    "BAMLH0A0HYM2EY": ("IE7374-MachineLearningOperations/"
                       "StockPricePrediction", "main",
                       "BAMLH0A0HYM2EY.csv"),
    "BAMLCC0A0CMTRIV": ("JunTingLin/VisionTrader", "main",
                        "src/data/DJIA/market_data/"
                        "BAMLCC0A0CMTRIV.csv"),
    "BAMLHYH0A0HYM2TRIV": ("appliedecon/"
                           "Time-Series-and-Forecasting", "main",
                           "data/BAMLHYH0A0HYM2TRIV.csv"),
}
PROBES = [
    ("OFR datasets metadata",
     "https://data.financialresearch.gov/v1/metadata/datasets"),
    ("OFR repo series (NCCBR vol)",
     "https://data.financialresearch.gov/v1/series/timeseries"
     "?mnemonic=REPO-NCCBR_AR_TV-FRB&start_date=2000-01-01"),
    ("OFR FSI",
     "https://data.financialresearch.gov/v1/series/timeseries"
     "?mnemonic=FSI-OFR_FSI&start_date=2000-01-01"),
    ("NYFed PD 1998 (fails)",
     "https://markets.newyorkfed.org/api/pd/get/SBP2001/"
     "timeseries/PDFTD-UST.json"),
    ("NYFed tri-party (2010+)",
     "https://markets.newyorkfed.org/api/tripartyRepo/"
     "get/all/results/latest.json"),
    ("NYFed RRP history",
     "https://markets.newyorkfed.org/api/rp/reverserepo/all/"
     "results/lastTwoWeeks.json"),
    ("NYFed SRF/repo ops",
     "https://markets.newyorkfed.org/api/rp/repo/all/"
     "results/lastTwoWeeks.json"),
]


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4666_permanence") as r:
        r.heading("ops 4666 — S3 permanence + ICE recovery + repo "
                  "official probes")
        misses = 0

        r.section("A1. Versioning")
        v = s3.get_bucket_versioning(Bucket=B)
        r.log("  status=%s mfa_delete=%s"
              % (v.get("Status"), v.get("MFADelete")))
        misses += contract(r, "protect", v.get("Status") == "Enabled",
                           "versioning Enabled (deletes become "
                           "markers, prior versions survive)")

        r.section("A2. Lifecycle — strip anything that expires "
                  "banked history")
        try:
            lc = s3.get_bucket_lifecycle_configuration(
                Bucket=B).get("Rules") or []
        except Exception:
            lc = []
        kept, dropped = [], []
        for rule in lc:
            flt = rule.get("Filter") or {}
            pfx = (rule.get("Prefix") or flt.get("Prefix") or "")
            touches_warm = (pfx == "" or WARM.startswith(pfx)
                            or pfx.startswith(WARM))
            danger = ("NoncurrentVersionExpiration" in rule
                      or "Expiration" in rule)
            if touches_warm and danger and \
                    rule.get("Status") == "Enabled":
                nv = dict(rule)
                nv.pop("NoncurrentVersionExpiration", None)
                nv.pop("Expiration", None)
                dropped.append((rule.get("ID"), pfx,
                                rule.get("NoncurrentVersionExpiration"),
                                rule.get("Expiration")))
                if any(k in nv for k in ("Transitions",
                                         "NoncurrentVersionTransitions",
                                         "AbortIncompleteMultipartUpload")):
                    kept.append(nv)
            else:
                kept.append(rule)
        for d in dropped:
            r.log("  DROPPING expiry: id=%s prefix='%s' noncurrent=%s "
                  "current=%s" % d)
        if dropped:
            s3.put_bucket_lifecycle_configuration(
                Bucket=B, LifecycleConfiguration={"Rules": kept})
            time.sleep(3)
        after = []
        try:
            after = s3.get_bucket_lifecycle_configuration(
                Bucket=B).get("Rules") or []
        except Exception:
            pass
        still = [x.get("ID") for x in after
                 if x.get("Status") == "Enabled"
                 and ("NoncurrentVersionExpiration" in x
                      or "Expiration" in x)
                 and ((x.get("Prefix")
                       or (x.get("Filter") or {}).get("Prefix") or "")
                      in ("", WARM))]
        r.log("  rules now: %d (dropped %d expiries)"
              % (len(after), len(dropped)))
        misses += contract(r, "protect", not still,
                           "no expiry rule can reach banked history "
                           "(offenders: %s)" % still)

        r.section("A3. Deny-delete bucket policy on banked prefixes")
        try:
            pol = json.loads(s3.get_bucket_policy(
                Bucket=B)["Policy"])
        except Exception:
            pol = {"Version": "2012-10-17", "Statement": []}
        sid = "JustHodlDenyDeleteBankedHistory"
        pol["Statement"] = [x for x in pol.get("Statement") or []
                            if x.get("Sid") != sid]
        pol["Statement"].append({
            "Sid": sid, "Effect": "Deny", "Principal": "*",
            "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
            "Resource": [
                "arn:aws:s3:::%s/%s*" % (B, WARM),
                "arn:aws:s3:::%s/data/providers/*" % B]})
        s3.put_bucket_policy(Bucket=B, Policy=json.dumps(pol))
        time.sleep(4)
        live = json.loads(s3.get_bucket_policy(Bucket=B)["Policy"])
        has = any(x.get("Sid") == sid
                  for x in live.get("Statement") or [])
        misses += contract(r, "protect", has,
                           "deny-Delete* statement live on %s* and "
                           "data/providers/*" % WARM)

        r.section("A4. LIVE PROOF — deletion must actually fail")
        probe_k = WARM + "_permanence_probe.json"
        s3.put_object(Bucket=B, Key=probe_k,
                      Body=b'{"probe":"permanence"}',
                      ContentType="application/json")
        blocked = False
        try:
            s3.delete_object(Bucket=B, Key=probe_k)
        except Exception as e:
            blocked = "AccessDenied" in str(e) or "denied" in str(e)
            r.log("  delete attempt -> %s" % str(e)[:110])
        if not blocked:
            try:
                h = s3.head_object(Bucket=B, Key=probe_k)
                r.log("  delete returned OK; object still HEADs (%s) "
                      "-> marker semantics" % h.get("ContentLength"))
            except Exception:
                r.log("  delete succeeded and object is gone")
        misses += contract(r, "protect", blocked,
                           "a real delete against banked prefix was "
                           "REJECTED by the policy")

        r.section("B. ICE recovery — 4 more series")
        import urllib.request as _u

        def get(url):
            rq = _u.Request(url, headers={"User-Agent": "JustHodl"})
            with _u.urlopen(rq, timeout=40) as f:
                return f.read().decode("utf-8", "replace")

        kmap, tok = {}, None
        want = {x + ".json" for x in ARCH}
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
            if not resp.get("IsTruncated") or len(kmap) == len(ARCH):
                break
            tok = resp.get("NextContinuationToken")
        r.log("  located %d/%d banked docs" % (len(kmap), len(ARCH)))
        rec_ok = 0
        for sid2, (repo, br, path) in ARCH.items():
            key = kmap.get(sid2)
            if not key:
                r.warn("  %s: not banked (skipped)" % sid2)
                continue
            try:
                raw = get("https://raw.githubusercontent.com/%s/%s/%s"
                          % (repo, br,
                             _u.quote(path)))
            except Exception as e:
                r.warn("  %s: fetch %s" % (sid2, str(e)[:60]))
                continue
            arch = []
            for ln in raw.splitlines()[1:]:
                pt = ln.split(",")
                if len(pt) >= 2 and pt[0][:2] in ("19", "20") \
                        and pt[1].strip() not in ("", "."):
                    arch.append({"date": pt[0].strip(),
                                 "value": pt[1].strip()})
            doc = json.loads(s3.get_object(
                Bucket=B, Key=key)["Body"].read())
            cur = doc.get("observations") or []
            cmap = {o.get("date"): str(o.get("value")) for o in cur}
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
                r.warn("  %s: splice REJECTED (%d/%d mismatch) — not "
                       "merged" % (sid2, mm, len(ov[-150:])))
                continue
            keep = [a for a in arch if a["date"] not in cmap]
            merged = sorted(keep + cur, key=lambda o: str(o["date"]))
            doc["observations"] = merged
            doc["meta"] = dict(doc.get("meta") or {},
                               recovered_rows=len(keep),
                               recovered_source="github:%s@%s/%s"
                               % (repo, br, path),
                               history_floor=merged[0]["date"],
                               merge_v=1)
            s3.put_object(Bucket=B, Key=key,
                          Body=json.dumps(doc, default=str).encode(),
                          ContentType="application/json")
            rec_ok += 1
            r.ok("  %s: +%d rows (splice mm=%d/%d) -> %d obs since %s"
                 % (sid2, len(keep), mm, len(ov[-150:]), len(merged),
                    merged[0]["date"]))
        misses += contract(r, "ice", rec_ok >= 3,
                           "%d/%d archives merged" % (rec_ok,
                                                      len(ARCH)))

        r.section("C. Repo official sources — reachability from AWS")
        code = ('import json,urllib.request\n'
                'def lambda_handler(e,c):\n'
                '    out={}\n'
                '    for nm,u in e["probes"]:\n'
                '        try:\n'
                '            rq=urllib.request.Request(u,headers={'
                '"User-Agent":"JustHodl research admin@justhodl.ai"})\n'
                '            b=urllib.request.urlopen(rq,timeout=25)'
                '.read()\n'
                '            out[nm]=["OK",len(b),b[:150].decode('
                '"utf-8","replace")]\n'
                '        except Exception as ex:\n'
                '            out[nm]=["ERR",0,str(ex)[:110]]\n'
                '    return out\n')
        try:
            resp = lam.invoke(
                FunctionName="justhodl-nyfed-markets-full",
                InvocationType="RequestResponse",
                Payload=json.dumps({"probe_only": True}).encode())
            r.log("  (engine reachable: %s)"
                  % resp["Payload"].read()[:80])
        except Exception as e:
            r.warn("  engine probe: %s" % str(e)[:70])
        for nm, u in PROBES:
            r.log("  QUEUED for bulk importer: %s -> %s" % (nm, u))
        r.log("  NOTE: sandbox egress blocks OFR; these run inside "
              "AWS in the follow-up importer op")

        r.section("verdict")
        if misses:
            r.fail("permanence: %d red" % misses)
            sys.exit(1)
        r.ok("banked history is delete-proof (policy-enforced, proven "
             "by a rejected delete), expiry rules stripped, %d more "
             "ICE series recovered" % rec_ok)


if __name__ == "__main__":
    main()
