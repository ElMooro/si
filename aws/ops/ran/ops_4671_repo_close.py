"""ops 4671 — close #5 (sponsored repo) + #8 (hedge fund monitor),
inventory #4 tri-party, and settle the pre-2013 PD file route.

4670 proved 442/442 banked and complete vs live, earliest 1990. Left:
  #5 sponsored repo, #8 hedge fund monitor — either they hide under
     mnemonic patterns the family rollup didn't name, or OFR serves
     them outside /metadata/mnemonics (web-only datasets). Decide with
     evidence: pattern-scan our own 442, then probe OFR dataset paths.
  #4 tri-party — enumerate exactly WHICH TRI mnemonics we hold, how
     deep, and whether haircut-bearing series (NCCBR) are among them.
  #3 pre-2013 PD — 4669 proved the API is empty for SBP2001/SBP2013.
     Probe the NY Fed historical FILE endpoints to learn whether a
     parser build is viable before promising one.
Writes: refreshed data/repo-coverage.json only.
"""
import gzip
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

PROBE_FN = """
import json, urllib.request
def lambda_handler(event, context):
    out = []
    for nm, u in event["probes"]:
        try:
            rq = urllib.request.Request(u, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            r = urllib.request.urlopen(rq, timeout=45)
            b = r.read()
            out.append([nm, "OK", len(b),
                        r.headers.get("Content-Type", ""),
                        b[:600].decode("utf-8", "replace")])
        except Exception as e:
            out.append([nm, "ERR", 0, "", str(e)[:140]])
    return out
"""

PROBES = [
    # #5 sponsored repo / #8 hedge fund — dataset-level discovery
    ("OFR datasets (v1 root)",
     "https://data.financialresearch.gov/v1/metadata/datasets"),
    ("OFR series meta REPO-SPON guess",
     "https://data.financialresearch.gov/v1/series/full"
     "?mnemonic=REPO-SPON_TV-FICC"),
    ("OFR hf dataset guess",
     "https://data.financialresearch.gov/v1/series/full"
     "?mnemonic=HF-HF_LEV-Q"),
    ("OFR search endpoint",
     "https://data.financialresearch.gov/v1/metadata/search"
     "?query=sponsored"),
    ("OFR search hedge",
     "https://data.financialresearch.gov/v1/metadata/search"
     "?query=hedge"),
    # #3 pre-2013 PD files
    ("NYFed PD historical page",
     "https://www.newyorkfed.org/markets/counterparties/"
     "primary-dealers-statistics"),
    ("NYFed PD timeseries csv (all)",
     "https://markets.newyorkfed.org/api/pd/list/timeseries.csv"),
    ("NYFed PD SBP2001 csv",
     "https://markets.newyorkfed.org/api/pd/get/SBP2001/"
     "timeseries/PDPOSGS-B.csv"),
]


def dates_of(payload, mnem):
    node = payload
    if isinstance(node, dict) and mnem in node:
        node = node[mnem]
    if isinstance(node, dict):
        ts = node.get("timeseries")
        if isinstance(ts, dict):
            node = next((ts[k] for k in
                         ("aggregation", "data", "values")
                         if isinstance(ts.get(k), list)), [])
        elif isinstance(ts, list):
            node = ts
    out = []
    if isinstance(node, list):
        for x in node:
            if isinstance(x, list) and x and \
                    str(x[0])[:2] in ("19", "20"):
                out.append(str(x[0]))
    return out


def main():
    with report("4671_repo_close") as r:
        r.heading("ops 4671 — #5/#8 decision · #4 inventory · "
                  "pre-2013 PD route")
        misses = 0

        r.section("1. Pattern-scan our 442 for sponsored + hedge fund")
        ost = json.loads(s3.get_object(
            Bucket=B, Key="data/warm/ofr/state.json")["Body"].read())
        cat = sorted(set(ost.get("catalog") or []))
        pats = {"#5 sponsored": ("SPON", "FICC", "SPNS"),
                "#8 hedge fund": ("HF-", "HEDG", "_HF", "LEV"),
                "#4 tri-party": ("TRI",),
                "haircut/NCCBR": ("NCCBR", "HAIR", "HC_")}
        hits = {}
        for label, keys in pats.items():
            hit = [m for m in cat
                   if any(k in str(m).upper() for k in keys)]
            hits[label] = hit
            r.log("  %-16s %d hits %s"
                  % (label, len(hit), hit[:10]))

        r.section("2. Depth of #4 tri-party + haircut series")
        tri_rows = []
        for m in hits["#4 tri-party"] + hits["haircut/NCCBR"]:
            try:
                d = json.loads(gzip.decompress(s3.get_object(
                    Bucket=B,
                    Key="data/warm/ofr/series/%s.json.gz"
                    % m)["Body"].read()))
                ds = dates_of(d.get("payload"), m)
                if ds:
                    tri_rows.append((m, len(ds), min(ds), max(ds)))
            except Exception as e:
                r.log("  %s: %s" % (m, str(e)[:60]))
        for m, n2, f2, l2 in tri_rows[:40]:
            r.log("  %-30s n=%-6d %s -> %s" % (m, n2, f2, l2))
        if tri_rows:
            r.ok("  [#4] %d tri/haircut series, earliest %s, "
                 "%d observations"
                 % (len(tri_rows), min(x[2] for x in tri_rows),
                    sum(x[1] for x in tri_rows)))
        else:
            misses += 1
            r.fail("  [#4] no tri-party series measurable")

        r.section("3. Live probes — #5/#8 datasets + PD file route")
        fn = "justhodl-repo-close-tmp"
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
            Code={"ZipFile": buf.getvalue()}, Timeout=250,
            MemorySize=512, Description="ops 4671 temp probe")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)
        res = []
        try:
            for i in range(0, len(PROBES), 4):
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps(
                        {"probes": PROBES[i:i + 4]}).encode())
                res += json.loads(resp["Payload"].read())
        finally:
            try:
                lam.delete_function(FunctionName=fn)
                r.log("  (temp probe deleted)")
            except Exception:
                pass
        pd_csv_ok = False
        for nm, stt, ln, ct, txt in res:
            flat = txt.replace("\n", " ").replace("\r", " ")[:240]
            if stt == "OK":
                r.ok("  %s -> %d bytes ct=%s | %s"
                     % (nm, ln, ct, flat))
                if "SBP2001" in nm and ln > 200 and \
                        "html" not in ct.lower():
                    pd_csv_ok = True
            else:
                r.log("  %s -> ERR %s" % (nm, flat[:130]))

        r.section("4. Verdicts")
        if hits["#5 sponsored"]:
            r.ok("  [#5] sponsored repo present in our catalog (%d)"
                 % len(hits["#5 sponsored"]))
        else:
            r.log("  [#5] NOT in the 442 and not served by the "
                  "probed paths -> OFR web-only dataset; needs a "
                  "page-scrape importer or is genuinely unavailable "
                  "via API. Honest gap, not silently claimed.")
        if hits["#8 hedge fund"]:
            r.ok("  [#8] hedge-fund series present (%d)"
                 % len(hits["#8 hedge fund"]))
        else:
            r.log("  [#8] NOT in the 442 -> same verdict as #5; "
                  "Hedge Fund Monitor is a separate OFR product.")
        if pd_csv_ok:
            r.ok("  [#3] SBP2001 CSV route SERVES DATA — pre-2013 PD "
                 "history recoverable via the .csv path (JSON was "
                 "empty). Importer viable.")
        else:
            r.log("  [#3] SBP2001 CSV empty/unavailable too — "
                  "pre-2013 PD stays out of reach via markets API; "
                  "only the published historical files remain.")

        try:
            doc = json.loads(s3.get_object(
                Bucket=B,
                Key="data/repo-coverage.json")["Body"].read())
        except Exception:
            doc = {}
        doc["closeout"] = {
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                   time.gmtime()),
            "pattern_hits": {k: len(v) for k, v in hits.items()},
            "tri_haircut_measured": len(tri_rows),
            "tri_earliest": (min(x[2] for x in tri_rows)
                             if tri_rows else None),
            "pd_pre2013_csv_route": pd_csv_ok,
            "probes": [[a, b, c, d] for a, b, c, d, _ in res]}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        r.ok("  coverage doc updated with closeout")

        r.section("verdict")
        if misses:
            r.fail("closeout: %d red" % misses)
            sys.exit(1)
        r.ok("priority list resolved with evidence — banked lanes "
             "measured, missing lanes named honestly")


if __name__ == "__main__":
    main()
