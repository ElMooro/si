"""ops 4663 — READ-ONLY: ICE BofA (BAML*) inception audit (Khalid).

Question: do all banked FRED ICE BofA series reach back to inception,
and to which year each? Method: list every banked BAML* doc, read its
first stored observation, cross-check against FRED's own declared
observation_start per series (7-day tolerance for stripped "."
placeholder rows at series head). Zero writes to data — GETs only; the
report is the only artifact.
"""
import gzip
import json
import sys
import time
import urllib.request
from datetime import datetime

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def fred(url):
    for att in (1, 2):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            if att == 1 and "429" in str(e):
                time.sleep(10)
                continue
            raise


def main():
    with report("4663_baml_inception") as r:
        r.heading("ops 4663 — ICE BofA inception audit (read-only)")
        misses = 0
        key = ""
        try:
            ssm = boto3.client("ssm", region_name="us-east-1")
            for pn in ("/justhodl/fred/api-key", "/justhodl/fred-key",
                       "/justhodl/keys/fred"):
                try:
                    key = ssm.get_parameter(
                        Name=pn, WithDecryption=True
                    )["Parameter"]["Value"]
                    break
                except Exception:
                    continue
        except Exception:
            pass
        if not key:
            env = (lam.get_function(
                FunctionName="justhodl-fred-catalog")
                ["Configuration"].get("Environment") or {}) \
                .get("Variables") or {}
            key = (env.get("FRED_KEY") or env.get("FRED_API_KEY")
                   or "")
        r.log("fred key resolved: %s (never printed)" % bool(key))
        if not key:
            r.fail("no FRED key resolvable — inception cross-check "
                   "impossible; refusing to fake a green")
            sys.exit(1)

        r.section("1. Find every banked BAML* doc")
        t0, found, tok = time.time(), {}, None
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/",
                  "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                k = o["Key"]
                bn = k.rsplit("/", 1)[-1]
                if bn.startswith("BAML") and bn.endswith(".json"):
                    found[bn[:-5]] = k
            if not resp.get("IsTruncated"):
                break
            tok = resp.get("NextContinuationToken")
        r.log("  %d BAML series banked (listed in %.0fs)"
              % (len(found), time.time() - t0))
        if not found:
            r.fail("  no BAML docs found — prefix or family assumption "
                   "wrong")
            sys.exit(1)

        r.section("2. Per-series: banked first obs vs FRED inception")
        rows, bad, unver = [], [], []
        for sid in sorted(found):
            try:
                doc = json.loads(s3.get_object(
                    Bucket=B, Key=found[sid])["Body"].read())
                obs = doc.get("observations") or []
                first = (obs[0].get("date") if obs else None)
                last = (obs[-1].get("date") if obs else None)
                n_o = len(obs)
            except Exception as e:
                unver.append((sid, "doc: " + str(e)[:40]))
                continue
            incep, title = None, ""
            if key:
                try:
                    m = fred("https://api.stlouisfed.org/fred/series"
                             "?series_id=%s&api_key=%s&file_type=json"
                             % (sid, key))
                    ss = (m.get("seriess") or [{}])[0]
                    incep = ss.get("observation_start")
                    title = (ss.get("title") or "")[:70]
                except Exception as e:
                    unver.append((sid, "fred: " + str(e)[:40]))
                time.sleep(0.65)
            ok, delta = None, None
            if first and incep:
                try:
                    delta = abs((datetime.fromisoformat(first)
                                 - datetime.fromisoformat(incep)).days)
                    ok = delta <= 7
                except Exception:
                    ok = None
            rows.append((sid, first, last, n_o, incep, ok, title))
            if ok is False:
                bad.append((sid, first, incep, delta))
        for sid, first, last, n_o, incep, ok, title in rows:
            r.log("  %s · since %s · %d obs · to %s · FRED inception "
                  "%s %s · %s"
                  % (sid, first, n_o, last, incep,
                     ("OK" if ok else "MISMATCH" if ok is False
                      else "?"), title))

        r.section("3. Grouped by inception year")
        by_year = {}
        for sid, first, _l, _n, _i, _ok, _t in rows:
            y = (first or "????")[:4]
            by_year.setdefault(y, []).append(sid)
        for y in sorted(by_year):
            r.log("  %s (%d): %s" % (y, len(by_year[y]),
                                     " ".join(by_year[y])))

        r.section("verdict")
        r.log("  banked=%d verified-at-inception=%d mismatches=%d "
              "unverifiable=%d"
              % (len(rows),
                 sum(1 for x in rows if x[5]), len(bad), len(unver)))
        for sid, first, incep, d in bad[:20]:
            r.warn("  MISMATCH %s: banked %s vs inception %s (%s d)"
                   % (sid, first, incep, d))
        for sid, why in unver[:10]:
            r.warn("  unverifiable %s: %s" % (sid, why))
        if bad:
            r.fail("%d series do not reach inception" % len(bad))
            sys.exit(1)
        verified = sum(1 for x in rows if x[5])
        if verified < 0.9 * max(1, len(rows)):
            r.fail("only %d/%d verified against FRED — not enough to "
                   "confirm" % (verified, len(rows)))
            sys.exit(1)
        if len(unver) > 5:
            r.fail("%d unverifiable — rerun when FRED settles"
                   % len(unver))
            sys.exit(1)
        r.ok("every banked ICE BofA series reaches FRED inception "
             "(7d tolerance for stripped '.' head rows)")


if __name__ == "__main__":
    main()
