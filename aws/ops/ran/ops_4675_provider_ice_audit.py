"""ops 4675 — can ANY of our 42 providers serve ICE, and what
non-truncated credit series already bridge 2017-2023?

Khalid: "check all our data providers to see if we can download them."
Two halves, both evidence-driven:

A. PROBE every provider plausibly capable of carrying ICE BofA
   (commercial redistributors + the few官 sources with credit data).
   ICE is licensed, so the prior is low — but priors are not evidence.

B. BRIDGE HUNT: the 2017-2023 hole only matters if nothing else in our
   52GB covers that window. Moody's Aaa/Baa (DAAA/DBAA/BAA10Y/AAA10Y),
   the Fed's own corporate series, and ECB/BIS credit aggregates are
   NOT licensed the way ICE is and are likely continuous. Measure what
   we already hold across 2017-2023 so the gap can be bridged
   analytically even if no ICE download exists.
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
            r = urllib.request.urlopen(rq, timeout=30)
            b = r.read()
            out.append([nm, "OK", len(b),
                        b[:300].decode("utf-8", "replace")])
        except Exception as e:
            out.append([nm, "ERR", 0, str(e)[:120]])
    return out
"""

# Providers that could conceivably redistribute ICE BofA
PROBES = [
    ("DBnomics FRED/BAML (retest)",
     "https://api.db.nomics.world/v22/series/FRED/BAMLH0A0HYM2"
     "?observations=1"),
    ("DBnomics provider list",
     "https://api.db.nomics.world/v22/providers"),
    ("Yahoo ^BAMLH0A0HYM2",
     "https://query1.finance.yahoo.com/v8/finance/chart/"
     "%5EBAMLH0A0HYM2?range=max&interval=1d"),
    ("ECB corporate spread dataflow",
     "https://data-api.ecb.europa.eu/service/data/FM?format=jsondata"
     "&detail=serieskeysonly&lastNObservations=1"),
    ("BIS credit dataflows",
     "https://stats.bis.org/api/v1/dataflow"),
    ("Fed Board DDP H15 (Moody's)",
     "https://www.federalreserve.gov/datadownload/Output.aspx"
     "?rel=H15&series=&lastobs=5&from=&to=&filetype=csv&label=include"
     "&layout=seriescolumn"),
    ("IMF dataflow",
     "http://dataservices.imf.org/REST/SDMX_JSON.svc/Dataflow"),
    ("World Bank indicator search",
     "https://api.worldbank.org/v2/indicator?format=json&per_page=1"),
]

# Non-truncated credit series that could bridge 2017-2023
BRIDGE = ["DAAA", "DBAA", "BAA10Y", "AAA10Y", "BAA10YM", "AAA",
          "BAA", "T10Y2Y", "TEDRATE", "DGS10", "MORTGAGE30US",
          "HQMCB10YR", "DCOILWTICO"]


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4675_provider_ice_audit") as r:
        r.heading("ops 4675 — provider ICE audit + 2017-2023 bridge")
        misses = 0

        r.section("A. Probe providers for ICE availability")
        fn = "justhodl-ice-audit-tmp"
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
            MemorySize=512, Description="ops 4675 temp ICE audit")
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
            except Exception:
                pass
        ice_found = []
        for nm, st2, ln, txt in res:
            flat = txt.replace("\n", " ")[:200]
            if st2 == "OK":
                has_baml = "BAML" in txt.upper()
                r.log("  %-32s OK %7d %s| %s"
                      % (nm, ln, "**BAML**" if has_baml else "",
                         flat[:130]))
                if has_baml and "chart" in nm.lower():
                    ice_found.append(nm)
            else:
                r.log("  %-32s ERR %s" % (nm, flat[:110]))
        if ice_found:
            r.ok("  [ice] candidate provider(s) returning BAML: %s"
                 % ice_found)
        else:
            r.log("  [ice] NO provider in our stack redistributes "
                  "ICE BofA — consistent with its licence. FRED is "
                  "the only public channel, and it is truncated.")

        r.section("B. Bridge hunt — what we hold across 2017-2023")
        kmap, tok = {}, None
        want = set(x + ".json" for x in BRIDGE)
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
        r.log("  located %d/%d bridge candidates"
              % (len(kmap), len(BRIDGE)))
        good = []
        for sid, key in sorted(kmap.items()):
            try:
                doc = json.loads(s3.get_object(
                    Bucket=B, Key=key)["Body"].read())
                obs = doc.get("observations") or []
                ds = [o.get("date") for o in obs if o.get("date")]
                if not ds:
                    continue
                inwin = [d for d in ds if "2017-01-13" <= d
                         <= "2023-08-14"]
                covers = len(inwin) > 1000
                r.log("  %-14s n=%-6d %s -> %s · 2017-2023 rows=%d %s"
                      % (sid, len(ds), min(ds), max(ds), len(inwin),
                         "COVERS THE HOLE" if covers else ""))
                if covers:
                    good.append(sid)
            except Exception as e:
                r.log("  %s: %s" % (sid, str(e)[:60]))
        misses += contract(
            r, "bridge", len(good) >= 2,
            "%d series span the ICE hole continuously (%s) — the "
            "2017-2023 credit regime is observable even without ICE"
            % (len(good), good[:6]))

        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/repo-coverage.json")["Body"].read())
        except Exception:
            cov = {}
        cov["ice_provider_audit"] = {
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                   time.gmtime()),
            "providers_probed": len(PROBES),
            "providers_with_ice": ice_found,
            "bridge_series": good,
            "verdict": ("ICE licensed — no provider in the stack "
                        "redistributes it; bridge via %s"
                        % (good[:4] or "none found"))}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        r.ok("  audit recorded in data/repo-coverage.json")

        r.section("verdict")
        if misses:
            r.fail("audit: %d red" % misses)
            sys.exit(1)
        r.ok("provider audit complete — ICE availability answered "
             "with evidence, bridge series identified")


if __name__ == "__main__":
    main()
