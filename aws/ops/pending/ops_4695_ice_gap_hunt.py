"""ops 4695 — real gap list (from live state, not memory) + GitHub
+ Wayback hunt for exactly those series.

Tonight's lesson: my own recollection of what was recovered has been
wrong twice already (the FRED start date, the file's actual overlap).
This reads the ACTUAL banked doc for every one of the 192 ICE mnemonics
and classifies deep (first date < 2018) vs shallow (2023-only) live,
before hunting anything.

Hunt method for the gap: (a) GitHub code search per remaining id --
broader than the earlier 15-id sweep, now covering the full 192; (b)
Wayback DIRECT fetch at known-good timestamps (the method that worked
for all 27 core series) -- NOT a CDX query, which throttled (498) on a
broad search earlier. Discovery + depth-validation only; no merge here
-- that stays a separate, auditable pass once candidates are confirmed.
"""
import io
import json
import sys
import time
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))
STAMPS = ["20250601", "20240901", "20240301", "20231001", "20220601"]

PROBE_FN = """
import json, urllib.request
def lambda_handler(event, context):
    out = []
    for nm, u in event["probes"]:
        try:
            rq = urllib.request.Request(u, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; "
                              "x64) AppleWebKit/537.36 (KHTML, like "
                              "Gecko) Chrome/124.0.0.0 Safari/537.36"})
            r = urllib.request.urlopen(rq, timeout=35)
            b = r.read()
            out.append([nm, "OK", len(b),
                        b[:250000].decode("utf-8", "replace")])
        except Exception as e:
            out.append([nm, "ERR", 0, str(e)[:100]])
    return out
"""


def parse_txt(txt):
    rows = []
    for ln in txt.splitlines():
        if ln[:2] not in ("19", "20"):
            continue
        p = ln.replace(",", " ").split()
        if len(p) >= 2 and p[1] not in (".", ""):
            try:
                float(p[1])
            except ValueError:
                continue
            rows.append((p[0], p[1]))
    return rows


def main():
    with report("4695_ice_gap_hunt") as r:
        r.heading("ops 4695 — real gap list + GitHub/Wayback hunt")

        r.section("1. Real gap list from LIVE banked state (not "
                  "memory)")
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
        r.log("  %d BAML docs banked" % len(banked))

        deep, shallow = [], []
        for sid, key in sorted(banked.items()):
            try:
                doc = json.loads(s3.get_object(
                    Bucket=B, Key=key)["Body"].read())
                obs = doc.get("observations") or []
                first = min((o.get("date") for o in obs
                            if o.get("date")), default="9999")
            except Exception:
                first = "9999"
            (deep if first < "2018" else shallow).append((sid, first))
        r.log("  DEEP (pre-2018): %d  |  SHALLOW (2023-only): %d"
              % (len(deep), len(shallow)))
        for sid, f in deep:
            r.log("    deep: %-22s since %s" % (sid, f))
        gap_ids = [sid for sid, _ in shallow]
        r.log("  hunting for %d shallow series" % len(gap_ids))

        r.section("2. GitHub code search across the full gap")
        import urllib.request as _u
        gh_hits = {}
        for sid in gap_ids:
            try:
                rq = _u.Request(
                    "https://api.github.com/search/code?q=filename:"
                    + sid + "+OR+" + sid + "&per_page=5",
                    headers={"Authorization": "",
                             "Accept": "application/vnd.github+json",
                             "User-Agent": "justhodl"})
                d = json.loads(_u.urlopen(rq, timeout=20).read())
                items = [(it["repository"]["full_name"], it["path"],
                         it["repository"].get("default_branch")
                         or "main")
                        for it in d.get("items") or []]
                if items:
                    gh_hits[sid] = items
                    r.log("  GH hit: %-20s %s" % (sid, items[:2]))
            except Exception as e:
                r.log("  GH %-20s search err: %s" % (sid, str(e)[:70]))
            time.sleep(2.2)   # unauthenticated: 10 req/min

        r.section("3. Wayback direct-fetch (the method that worked "
                  "for the 27)")
        fn = "justhodl-gap-hunt-tmp"
        role = lam.get_function(
            FunctionName="justhodl-ofr-stfm")["Configuration"]["Role"]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("lambda_function.py", PROBE_FN)
        try:
            lam.delete_function(FunctionName=fn)
            time.sleep(3)
        except Exception:
            pass
        lam.create_function(
            FunctionName=fn, Runtime="python3.12", Role=role,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()}, Timeout=290,
            MemorySize=1024, Description="ops 4695 gap hunt")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)

        wb_hits = {}
        try:
            for sid in gap_ids:
                probes = [(ts, "https://web.archive.org/web/"
                          + ts + "id_/https://fred.stlouisfed.org/"
                          "data/" + sid + ".txt") for ts in STAMPS]
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps({"probes": probes}).encode())
                got = json.loads(resp["Payload"].read())
                best = None
                for ts, stt, ln, txt in got:
                    if stt != "OK":
                        continue
                    rows = parse_txt(txt)
                    if len(rows) > 500 and (best is None
                                            or rows[0][0]
                                            < best[1][0][0]):
                        best = (ts, rows)
                if best:
                    wb_hits[sid] = best
                    r.log("  WB hit: %-20s %d rows %s -> %s (@%s)"
                         % (sid, len(best[1]), best[1][0][0],
                            best[1][-1][0], best[0]))
        finally:
            try:
                lam.delete_function(FunctionName=fn)
                r.log("  (temp probe deleted)")
            except Exception:
                pass

        r.section("verdict")
        found = set(gh_hits) | set(wb_hits)
        r.log("  GitHub candidates: %d | Wayback candidates: %d | "
              "union: %d of %d gap series"
              % (len(gh_hits), len(wb_hits), len(found),
                 len(gap_ids)))
        doc_out = {
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                   time.gmtime()),
            "gap_total": len(gap_ids), "deep_total": len(deep),
            "github_hits": {k: v[:1] for k, v in gh_hits.items()},
            "wayback_hits": {k: [v[0], len(v[1]), v[1][0][0],
                                 v[1][-1][0]]
                            for k, v in wb_hits.items()},
            "still_unfound": sorted(set(gap_ids) - found)}
        s3.put_object(Bucket=B, Key="data/ice-gap-hunt.json",
                      Body=json.dumps(doc_out, default=str).encode(),
                      ContentType="application/json")
        r.log("  still unfound (%d): %s" % (
            len(doc_out["still_unfound"]),
            doc_out["still_unfound"][:20]))
        if not found:
            r.fail("no candidates found for any gap series")
            sys.exit(1)
        r.ok("%d candidate series found across GitHub+Wayback -- "
             "next pass fetches, splice-validates, and merges only "
             "the ones that pass" % len(found))


if __name__ == "__main__":
    main()
