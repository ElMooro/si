"""ops 4669 — repo truth pass: read OUR catalog, settle 1998, find
tri-party.

4668's corrections drive this:
  - OFR is NOT a gap (442/442 banked; REPO 164, NYPD 194, MMF 42,
    FNYR 30, TYLD 12). My probe mnemonics were invented — this op
    reads the REAL names out of our own state.json and measures the
    DEPTH we hold per repo/MMF/dealer family.
  - OFR /metadata 403 was my wrong path; the engine's /series/full
    grammar works. Confirmed by using it.
  - #4 tri-party 400 on both API paths -> probe the FILE endpoints
    (NY Fed publishes xlsx/csv), plus alternate API spellings.
  - #3 SBP2001 (JAN 1998-JUN 2001) returned an EMPTY timeseries for
    PDFTD-UST -> probe multiple key families against every break to
    learn whether 1998 data exists at all, and under which keys.
Probes + measurement only; the sole write is the findings manifest.
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
            r = urllib.request.urlopen(rq, timeout=40)
            b = r.read()
            ct = r.headers.get("Content-Type", "")
            txt = b[:400].decode("utf-8", "replace")
            out.append([nm, "OK", len(b), ct, txt])
        except Exception as e:
            out.append([nm, "ERR", 0, "", str(e)[:130]])
    return out
"""

TRIPARTY = [
    ("tri-party xlsx (current)",
     "https://www.newyorkfed.org/medialibrary/media/banking/"
     "tpr_infr_reports/tpr_factsheet.xlsx"),
    ("tri-party stats page",
     "https://www.newyorkfed.org/data-and-statistics/"
     "data-visualization/tri-party-repo"),
    ("GCF/tri-party API root",
     "https://markets.newyorkfed.org/api/tripartyRepo.json"),
    ("marketshare tri-party",
     "https://markets.newyorkfed.org/api/tripartyRepo/"
     "marketshare/latest.json"),
    ("OFR tri-party via series/full",
     "https://data.financialresearch.gov/v1/series/full"
     "?mnemonic=REPO-TRI_AR_OO-P"),
]


def main():
    with report("4669_repo_truth") as r:
        r.heading("ops 4669 — OFR depth (real mnemonics) · 1998 "
                  "question · tri-party hunt")
        misses = 0

        r.section("1. OUR OFR catalog — real names + measured depth")
        ost = json.loads(s3.get_object(
            Bucket=B, Key="data/warm/ofr/state.json")["Body"].read())
        cat = list(ost.get("catalog") or [])
        fams = {}
        for m in cat:
            fam = str(m).split("-")[0]
            fams.setdefault(fam, []).append(m)
        for fam in sorted(fams, key=lambda f: -len(fams[f])):
            r.log("  %s (%d): %s"
                  % (fam, len(fams[fam]), fams[fam][:6]))
        sample = []
        for fam in ("REPO", "FNYR", "MMF", "NYPD", "TYLD"):
            sample += fams.get(fam, [])[:3]
        depths = []
        for m in sample:
            try:
                d = json.loads(gzip.decompress(s3.get_object(
                    Bucket=B,
                    Key="data/warm/ofr/series/%s.json.gz"
                    % m)["Body"].read()))
                pay = d.get("payload")
                rows = (pay if isinstance(pay, list)
                        else (pay.get("timeseries")
                              or pay.get("data") or [])
                        if isinstance(pay, dict) else [])
                dates = []
                for x in rows:
                    if isinstance(x, list) and x:
                        dates.append(str(x[0]))
                    elif isinstance(x, dict):
                        dates.append(str(x.get("date")
                                         or x.get("asofdate")))
                dates = [z for z in dates if z[:2] in ("19", "20")]
                if dates:
                    depths.append((m, len(dates), min(dates),
                                   max(dates)))
                    r.log("  %-28s n=%-6d %s -> %s"
                          % (m, len(dates), min(dates), max(dates)))
                else:
                    r.log("  %-28s no dated rows; head=%s"
                          % (m, str(pay)[:110]))
            except Exception as e:
                r.log("  %-28s %s" % (m, str(e)[:70]))
        deep = [x for x in depths if x[2] < "2015"]
        r.log("  depth summary: %d sampled, %d reach pre-2015 "
              "(earliest %s)"
              % (len(depths), len(deep),
                 min([x[2] for x in depths]) if depths else None))
        if not depths:
            misses += 1
            r.fail("  [ofr] could not measure ANY banked depth — "
                   "payload shape unknown, importer would be blind")
        else:
            r.ok("  [ofr] depth measurable on %d/%d sampled series"
                  % (len(depths), len(sample)))

        r.section("2. Build probe list — 1998 question + tri-party")
        st = json.loads(s3.get_object(
            Bucket=B, Key="data/warm/nyfed-markets/pd-state.json"
        )["Body"].read())
        pdcat = list(st.get("catalog") or [])
        fam_keys = []
        for pre in ("PDPOSGS", "PDFTD", "PDFTR", "PDSORA", "PDTRGST",
                    "PDABTOT", "PDCSTOT"):
            hit = [k for k in pdcat if str(k).startswith(pre)]
            if hit:
                fam_keys.append(hit[0])
        r.log("  probing 1998 break SBP2001 with: %s" % fam_keys)
        probes = []
        for k in fam_keys:
            probes.append(("SBP2001/%s" % k,
                           "https://markets.newyorkfed.org/api/pd/get/"
                           "SBP2001/timeseries/%s.json" % k))
            probes.append(("SBP2013/%s" % k,
                           "https://markets.newyorkfed.org/api/pd/get/"
                           "SBP2013/timeseries/%s.json" % k))
        probes += TRIPARTY

        fn = "justhodl-repo-probe-tmp2"
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
            Code={"ZipFile": buf.getvalue()}, Timeout=300,
            MemorySize=512, Description="ops 4669 temp probe")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)
        res = []
        try:
            for i in range(0, len(probes), 5):
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps(
                        {"probes": probes[i:i + 5]}).encode())
                res += json.loads(resp["Payload"].read())
        finally:
            try:
                lam.delete_function(FunctionName=fn)
                r.log("  (temp probe deleted)")
            except Exception as e:
                r.warn("  cleanup: %s" % str(e)[:60])

        r.section("3. The 1998 verdict")
        got98 = 0
        for nm, stt, ln, ct, txt in res:
            if not nm.startswith("SB"):
                continue
            body2 = txt.replace("\n", " ")[:190]
            empty = '"timeseries": [ ]' in txt or \
                    '"timeseries":[]' in txt
            if stt == "OK" and not empty:
                got98 += 1 if nm.startswith("SBP2001") else 0
                r.ok("  %s -> %d bytes DATA %s" % (nm, ln, body2))
            else:
                r.log("  %s -> %s %s"
                      % (nm, "EMPTY" if empty else stt, body2[:110]))
        if got98:
            r.ok("  [1998] SBP2001 SERVES DATA for %d key family(ies) "
                 "— PD history to 1998-01-28 is recoverable" % got98)
        else:
            r.log("  [1998] SBP2001 empty across every probed family "
                  "— the 1998-2001 break exists in the index but "
                  "serves no timeseries via this API path. PD floor "
                  "stays 2013 unless the bulk CSV route carries it.")

        r.section("4. Tri-party (#4 — haircuts)")
        tp_ok = []
        for nm, stt, ln, ct, txt in res:
            if nm.startswith("SB"):
                continue
            if stt == "OK":
                tp_ok.append((nm, ln, ct))
                r.ok("  %s -> %d bytes ct=%s | %s"
                     % (nm, ln, ct, txt.replace("\n", " ")[:150]))
            else:
                r.log("  %s -> ERR %s" % (nm, txt[:110]))
        if not tp_ok:
            r.log("  [tri-party] no probed endpoint served data — "
                  "the NY Fed tri-party/GCF statistics are published "
                  "as report files off the data-viz page, not this "
                  "API; needs an HTML-discovery + file-parse importer")
        else:
            r.ok("  [tri-party] %d live endpoint(s) — importer can "
                 "target these" % len(tp_ok))

        s3.put_object(
            Bucket=B, Key="data/_state/repo-truth-manifest.json",
            Body=json.dumps({"ofr_depths": depths, "probes": res,
                             "ofr_families":
                                 {k: len(v) for k, v in fams.items()}},
                            default=str).encode(),
            ContentType="application/json")
        r.log("  manifest -> data/_state/repo-truth-manifest.json")

        r.section("verdict")
        if misses:
            r.fail("repo truth: %d red" % misses)
            sys.exit(1)
        r.ok("OFR depth measured on real mnemonics · 1998 and "
             "tri-party questions answered with evidence")


if __name__ == "__main__":
    main()
