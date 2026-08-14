"""ops 4668 — repo priority #1-#8: DISCOVERY before importer.

Extend-don't-duplicate: ofr-stfm already banks 442 OFR mnemonics and
nyfed-repo-deep pulls repo-op history. So this op answers, from INSIDE
AWS (unrestricted egress, unlike the sandbox), three questions per
priority item:
  (a) does the authoritative endpoint respond, and with what shape?
  (b) how deep does its history actually go?
  (c) do we ALREADY hold it — and if so, how deep is ours?
Output: an exact build spec for the importer, no guessing. Probes only;
the only writes are the report + a probe manifest.
"""
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))

# Probed via a live engine (has open egress); each entry is
# (priority, label, url)
PROBES = [
    (1, "OFR datasets catalog",
     "https://data.financialresearch.gov/v1/metadata/datasets"),
    (1, "OFR repo dataset series list",
     "https://data.financialresearch.gov/v1/metadata/series"
     "?dataset=repo"),
    (1, "OFR repo full history (DVP vol)",
     "https://data.financialresearch.gov/v1/series/full"
     "?mnemonic=REPO-DVP_TV-FRB"),
    (2, "OFR NYFed ref-rates dataset",
     "https://data.financialresearch.gov/v1/metadata/series"
     "?dataset=nyfrbrates"),
    (2, "OFR SOFR full history",
     "https://data.financialresearch.gov/v1/series/full"
     "?mnemonic=REPO-SOFR_VW-FRB"),
    (3, "NYFed PD earliest break (1998)",
     "https://markets.newyorkfed.org/api/pd/get/SBP2001/"
     "timeseries/PDFTD-UST.json"),
    (3, "NYFed PD seriesbreaks",
     "https://markets.newyorkfed.org/api/pd/list/seriesbreaks.json"),
    (4, "NYFed tri-party latest",
     "https://markets.newyorkfed.org/api/tripartyRepo/get/all/"
     "results/latest.json"),
    (4, "NYFed tri-party alt path",
     "https://markets.newyorkfed.org/api/tripartyRepo/get/"
     "latest.json"),
    (5, "OFR sponsored repo",
     "https://data.financialresearch.gov/v1/metadata/series"
     "?dataset=repo&filter=sponsored"),
    (6, "NYFed RRP full history",
     "https://markets.newyorkfed.org/api/rp/reverserepo/"
     "propositions/search.json?startDate=2013-01-01"),
    (6, "NYFed SRF/repo ops search",
     "https://markets.newyorkfed.org/api/rp/repo/all/results/"
     "lastTwoWeeks.json"),
    (7, "OFR MMF dataset",
     "https://data.financialresearch.gov/v1/metadata/series"
     "?dataset=mmf"),
    (8, "OFR hedge fund monitor",
     "https://data.financialresearch.gov/v1/metadata/series"
     "?dataset=hf"),
]

PROBE_FN = """
import json, urllib.request
def lambda_handler(event, context):
    out = []
    for pr, nm, u in event["probes"]:
        try:
            rq = urllib.request.Request(u, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            b = urllib.request.urlopen(rq, timeout=30).read()
            txt = b.decode("utf-8", "replace")
            shape = ""
            try:
                d = json.loads(txt)
                if isinstance(d, dict):
                    shape = "dict:" + ",".join(list(d)[:6])
                elif isinstance(d, list):
                    shape = "list[%d]" % len(d)
                    if d and isinstance(d[0], (list, dict)):
                        shape += " first=" + str(d[0])[:120]
            except Exception:
                shape = "non-json"
            out.append([pr, nm, "OK", len(b), shape, txt[:260]])
        except Exception as e:
            out.append([pr, nm, "ERR", 0, "", str(e)[:140]])
    return out
"""


def main():
    with report("4668_repo_discovery") as r:
        r.heading("ops 4668 — repo #1-#8 endpoint discovery "
                  "(build spec, not guesses)")
        misses = 0

        r.section("0. What we ALREADY hold (extend-don't-duplicate)")
        try:
            ost = json.loads(s3.get_object(
                Bucket=B,
                Key="data/warm/ofr/state.json")["Body"].read())
            cat = ost.get("catalog") or []
            done = set(ost.get("done") or [])
            fams = {}
            for m in cat:
                fam = str(m).split("-")[0].split("_")[0][:12]
                fams[fam] = fams.get(fam, 0) + 1
            r.log("  ofr-stfm: %d mnemonics, %d banked, %d pending"
                  % (len(cat), len(done), len(set(cat) - done)))
            r.log("  families: %s"
                  % sorted(fams.items(), key=lambda x: -x[1])[:14])
            # depth of one banked repo series
            for probe_m in ("REPO-DVP_TV-FRB", "REPO-SOFR_VW-FRB"):
                try:
                    d = json.loads(gzip.decompress(s3.get_object(
                        Bucket=B,
                        Key="data/warm/ofr/series/%s.json.gz"
                        % probe_m)["Body"].read()))
                    pay = d.get("payload")
                    n = (len(pay) if isinstance(pay, list)
                         else len(pay.get("timeseries") or [])
                         if isinstance(pay, dict) else 0)
                    head = str(pay)[:150]
                    r.log("  banked %s: n=%s head=%s"
                          % (probe_m, n, head))
                except Exception as e:
                    r.log("  banked %s: %s" % (probe_m, str(e)[:70]))
        except Exception as e:
            r.warn("  ofr state: %s" % str(e)[:90])
        try:
            nst = json.loads(s3.get_object(
                Bucket=B, Key="data/warm/nyfed-markets/pd-state.json"
            )["Body"].read())
            dep = nst.get("depth") or {}
            r.log("  nyfed PD: done=%d depth_keys=%s first=%s "
                  "mean=%.0f breaks=%s"
                  % (len(set(nst.get("done") or [])),
                     dep.get("keys"), dep.get("first_min"),
                     dep.get("n_obs_sum", 0)
                     / max(1, dep.get("keys", 0)),
                     nst.get("seriesbreaks")))
        except Exception as e:
            r.warn("  nyfed state: %s" % str(e)[:90])

        r.section("1. Live probes from inside AWS")
        fn = "justhodl-repo-probe-tmp"
        role = None
        try:
            role = lam.get_function(
                FunctionName="justhodl-ofr-stfm"
            )["Configuration"]["Role"]
        except Exception as e:
            r.fail("  cannot read a role: %s" % str(e)[:90])
            sys.exit(1)
        import io
        import zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("lambda_function.py", PROBE_FN)
        code = buf.getvalue()
        try:
            lam.delete_function(FunctionName=fn)
            time.sleep(3)
        except Exception:
            pass
        lam.create_function(
            FunctionName=fn, Runtime="python3.12", Role=role,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": code}, Timeout=300, MemorySize=512,
            Description="ops 4668 temporary probe — deleted at end")
        for _ in range(30):
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("State") == "Active":
                break
            time.sleep(4)
        results = []
        try:
            for i in range(0, len(PROBES), 5):
                chunk = PROBES[i:i + 5]
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps({"probes": chunk}).encode())
                results += json.loads(resp["Payload"].read())
        finally:
            try:
                lam.delete_function(FunctionName=fn)
                r.log("  (temp probe function deleted)")
            except Exception as e:
                r.warn("  temp fn cleanup: %s" % str(e)[:70])

        ok_n = 0
        for pr, nm, st2, ln, shape, body2 in results:
            if st2 == "OK":
                ok_n += 1
                r.ok("  #%d %s -> %d bytes · %s" % (pr, nm, ln, shape))
                r.log("      body: %s" % body2.replace("\n", " ")[:230])
            else:
                r.log("  #%d %s -> ERR %s" % (pr, nm, body2))
        misses += 0 if ok_n >= 7 else 1
        if ok_n < 7:
            r.fail("  only %d/%d endpoints answered — spec would be "
                   "guesswork" % (ok_n, len(PROBES)))

        s3.put_object(
            Bucket=B, Key="data/_state/repo-probe-manifest.json",
            Body=json.dumps({"probes": results}, default=str).encode(),
            ContentType="application/json")
        r.log("  manifest -> data/_state/repo-probe-manifest.json")

        r.section("verdict")
        if misses:
            r.fail("discovery incomplete: %d red" % misses)
            sys.exit(1)
        r.ok("%d/%d endpoints mapped with real shapes — importer can "
             "be written against evidence" % (ok_n, len(PROBES)))


if __name__ == "__main__":
    main()
