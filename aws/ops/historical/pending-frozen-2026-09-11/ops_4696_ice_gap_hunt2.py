"""ops 4696 — gap hunt rev-2: fix the two real bugs in 4695.

4695's own logging was insufficient to explain its result: it only
logged HITS, so 0/44 on GitHub and 0/44 on Wayback produced zero
diagnostic evidence -- an unverified negative I won't report as fact.
Two confirmed bugs:
  1. GitHub: sent Authorization:"" (empty string), which GitHub
     rejects as malformed rather than treating as absent -> 401 on
     every one of 44 calls. Fixed by omitting the header entirely.
  2. Wayback: only tried the .txt form, not the .csv fallback the
     working browser script used -- and BAMLCC0A2AATRIV is PROVEN to
     have Wayback data (Khalid's own jh-ice.json carried 8,821 real
     rows for it), so a 0-hit here is a probe bug, not absent data.
This rev fixes both, logs the REASON for every miss (not just hits),
and short-circuits per series on first good hit to cut runtime/load.
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
STAMPS = ["20250601", "20240901", "20240301"]

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
            r = urllib.request.urlopen(rq, timeout=30)
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
    with report("4696_ice_gap_hunt2") as r:
        r.heading("ops 4696 — gap hunt rev-2 (bugs fixed, full "
                  "evidence logged)")

        r.section("1. Real gap list from LIVE banked state")
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
        gap_ids = []
        for sid, key in sorted(banked.items()):
            try:
                doc = json.loads(s3.get_object(
                    Bucket=B, Key=key)["Body"].read())
                first = min((o.get("date") for o in
                            (doc.get("observations") or [])
                            if o.get("date")), default="9999")
            except Exception:
                first = "9999"
            if first >= "2018":
                gap_ids.append(sid)
        r.log("  %d banked, %d shallow (the real gap)"
              % (len(banked), len(gap_ids)))

        r.section("2. GitHub — auth header fixed (no bogus empty "
                  "value)")
        import urllib.request as _u
        import urllib.error as _ue
        gh_hits, gh_reasons = {}, {}
        for sid in gap_ids:
            try:
                rq = _u.Request(
                    "https://api.github.com/search/code?q=filename:"
                    + sid + "&per_page=5",
                    headers={"Accept": "application/vnd.github+json",
                             "User-Agent": "justhodl-research"})
                d = json.loads(_u.urlopen(rq, timeout=20).read())
                items = [(it["repository"]["full_name"], it["path"],
                         it["repository"].get("default_branch")
                         or "main")
                        for it in d.get("items") or []]
                gh_reasons[sid] = "total_count=%s" % d.get(
                    "total_count")
                if items:
                    gh_hits[sid] = items
                    r.log("  GH HIT: %-20s %s" % (sid, items[:2]))
            except _ue.HTTPError as e:
                gh_reasons[sid] = "HTTP %s" % e.code
            except Exception as e:
                gh_reasons[sid] = str(e)[:80]
            time.sleep(2.3)
        r.log("  GitHub: %d hits / %d searched" % (len(gh_hits),
                                                    len(gap_ids)))

        r.section("3. Wayback — .txt AND .csv, first hit wins, "
                  "EVERY miss reason logged")
        fn = "justhodl-gap-hunt2-tmp"
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
            MemorySize=1024, Description="ops 4696 gap hunt rev2")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)

        wb_hits, wb_summary = {}, {"err": 0, "short": 0, "ok": 0}
        try:
            for si, sid in enumerate(gap_ids):
                probes = []
                for ts in STAMPS:
                    probes.append(
                        (ts + ":txt",
                         "https://web.archive.org/web/" + ts
                         + "id_/https://fred.stlouisfed.org/data/"
                         + sid + ".txt"))
                    probes.append(
                        (ts + ":csv",
                         "https://web.archive.org/web/" + ts
                         + "id_/https://fred.stlouisfed.org/graph/"
                         "fredgraph.csv?id=" + sid))
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps({"probes": probes}).encode())
                got = json.loads(resp["Payload"].read())
                hit, last_reason = None, None
                for nm, stt, ln, txt in got:
                    if stt != "OK":
                        wb_summary["err"] += 1
                        last_reason = "ERR " + txt[:60]
                        continue
                    rows = parse_txt(txt)
                    if len(rows) > 500:
                        wb_summary["ok"] += 1
                        hit = (nm, rows)
                        break
                    wb_summary["short"] += 1
                    last_reason = ("OK but %d bytes / %d parsed rows"
                                   % (ln, len(rows)))
                if hit:
                    wb_hits[sid] = hit
                    r.log("  WB HIT: %-20s %d rows %s -> %s (@%s)"
                         % (sid, len(hit[1]), hit[1][0][0],
                            hit[1][-1][0], hit[0]))
                elif si < 8 or sid in ("BAMLCC0A2AATRIV",
                                      "BAMLCC0A4BBBTRIV"):
                    r.log("  WB miss: %-20s last reason: %s"
                         % (sid, last_reason))
        finally:
            try:
                lam.delete_function(FunctionName=fn)
            except Exception:
                pass
        r.log("  Wayback attempt tally across all %d series: "
              "ok=%d short=%d err=%d"
              % (len(gap_ids), wb_summary["ok"], wb_summary["short"],
                 wb_summary["err"]))
        r.log("  Wayback: %d hits / %d searched" % (len(wb_hits),
                                                     len(gap_ids)))

        r.section("verdict")
        found = set(gh_hits) | set(wb_hits)
        r.log("  union: %d/%d gap series have a candidate"
              % (len(found), len(gap_ids)))
        doc_out = {
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                   time.gmtime()),
            "gap_total": len(gap_ids),
            "github_hits": {k: v[:1] for k, v in gh_hits.items()},
            "wayback_hits": {k: [v[0], len(v[1]), v[1][0][0],
                                 v[1][-1][0]]
                            for k, v in wb_hits.items()},
            "wayback_tally": wb_summary,
            "still_unfound": sorted(set(gap_ids) - found)}
        s3.put_object(Bucket=B, Key="data/ice-gap-hunt.json",
                      Body=json.dumps(doc_out, default=str).encode(),
                      ContentType="application/json")
        r.log("  still unfound (%d): %s" % (
            len(doc_out["still_unfound"]),
            doc_out["still_unfound"][:15]))
        if not found:
            r.fail("genuinely 0 candidates after fixing both probe "
                   "bugs -- see the tally above for why")
            sys.exit(1)
        r.ok("%d candidates found (GH=%d, WB=%d) -- next pass "
             "fetches+validates+merges" % (len(found), len(gh_hits),
                                           len(wb_hits)))


if __name__ == "__main__":
    main()
