"""ops/4901 -- expedite ALL remaining imports (Khalid: budget no problem).

Board read: everything COMPLETE except (a) ECB deep 30/48 on a 10-min
drip, (b) OECD 991/1546 "denied at source" -- 64%, which smells like
another fixable negotiation bug (the ECB-406 class), not real 403s,
(c) StatCan 290 denied (previously classified: genuine WDS permission
denials -- accepted), (d) SEC MIDAS: 0 keys because the importer was
NEVER BUILT (registry entry only, no lambda exists).

This op:
  G1 settle ecb-deep v1.2 (self-chain, harness v3 PASS) and kick it
     once -- the chain takes duty to ~100%; 18 giants finish in hours
     instead of days. Verify chaining is actually happening (state
     as_of advancing across multiple runs within the poll window).
  G2 OECD DENIED PROBE MATRIX (playbook doctrine 5: isolate the exact
     failing call): sample 24 denied flows from the walker ledger,
     try 3 request variants each (labels-csv format, SDMX-CSV Accept,
     lastNObservations probe). Classify: UNLOCKABLE (any 2xx) vs
     HARD_403 vs OTHER. Evidence verbatim -> adapter patch queued as
     ops 4902 if unlock rate is material.
  G3 MIDAS ENDPOINT DISCOVERY: probe SEC market-structure download
     candidates from the runner, record exactly what exists (status,
     bytes) so the importer gets built on evidence, not guesses.
Report everything; gates G2/G3 are diagnostic (PASS on completion).
"""
import gzip
import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def zip_has(fn, marker):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    raw = urllib.request.urlopen(loc, timeout=60).read()
    return marker.encode() in zipfile.ZipFile(
        io.BytesIO(raw)).read("lambda_function.py")


def http(url, headers=None, timeout=40):
    try:
        req = urllib.request.Request(url, headers=dict(UA,
                                                       **(headers
                                                          or {})))
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, len(r.read(65536))
    except urllib.error.HTTPError as e:
        return e.code, 0
    except Exception as e:
        return type(e).__name__, 0


def main():
    verdict = {"ops": 4901, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4901 -- expedite remaining imports") as rep:
        rep.heading("ops 4901 — expedite: deep chain · OECD probe "
                    "· MIDAS discovery")

        # ── G1 deep v1.2 settle + kick + chain evidence ─────────────
        ok1 = False
        end = time.time() + 420
        while time.time() < end:
            try:
                if zip_has("justhodl-ecb-deep", "chain_depth"):
                    ok1 = True
                    break
            except Exception:
                pass
            time.sleep(20)
        rep.kv(stage="deep-settle", v12=ok1)
        chain_runs, nc0, nc1 = 0, None, None
        if ok1:
            try:
                nc0 = g("data/_state/ecb-deep.json").get("n_complete")
            except Exception:
                nc0 = None
            lam.invoke(FunctionName="justhodl-ecb-deep",
                       InvocationType="Event")
            seen = set()
            t = time.time()
            while time.time() - t < 900:
                time.sleep(45)
                try:
                    st = g("data/_state/ecb-deep.json")
                except Exception:
                    continue
                if st.get("as_of"):
                    seen.add(st["as_of"])
                nc1 = st.get("n_complete")
                if len(seen) >= 2 and (nc1 or 0) > (nc0 or 0):
                    break
            chain_runs = len(seen)
            rep.kv(stage="deep-chain", runs_observed=chain_runs,
                   n_complete_before=nc0, n_complete_now=nc1,
                   mode=st.get("mode") if "st" in dir() else None)
        verdict["gates"]["deep_chain_live"] = (
            "PASS" if (ok1 and chain_runs >= 2) else
            "PENDING" if ok1 else "FAIL")

        # ── G2 OECD denied probe matrix ─────────────────────────────
        cls = {"UNLOCKABLE": [], "HARD_403": [], "OTHER": []}
        try:
            wo = g("data/_state/sdmx-walk-oecd.json")
            denied = [k for k, v in (wo.get("failures") or {}).items()
                      if "403" in str(v)]
            trip = {}
            try:
                trip = g("data/warm/oecd/flow-triplets.json.gz") or {}
            except Exception:
                pass
            sample = denied[:24]
            rep.kv(stage="oecd-ledger", denied_total=len(denied),
                   sampled=len(sample))
            base = "https://sdmx.oecd.org/public/rest/data/"
            for fid in sample:
                t3 = trip.get(str(fid)) or trip.get(fid)
                ref = (",".join(t3) if isinstance(t3, (list, tuple))
                       else (t3 or str(fid)))
                variants = [
                    ("csvfilewithlabels",
                     f"{base}{ref}/all?format=csvfilewithlabels"
                     "&lastNObservations=1", None),
                    ("sdmx-csv-accept",
                     f"{base}{ref}/all?lastNObservations=1",
                     {"Accept":
                      "application/vnd.sdmx.data+csv;"
                      "file=true;labels=both"}),
                    ("csvfile",
                     f"{base}{ref}/all?format=csvfile"
                     "&lastNObservations=1", None)]
                got = None
                for label, u, h in variants:
                    st_, nb = http(u, h)
                    if st_ == 200 and nb > 40:
                        got = label
                        break
                    time.sleep(0.25)
                if got:
                    cls["UNLOCKABLE"].append(f"{fid}={got}")
                elif st_ == 403:
                    cls["HARD_403"].append(str(fid))
                else:
                    cls["OTHER"].append(f"{fid}:{st_}")
                time.sleep(0.3)
            rep.kv(stage="oecd-probe",
                   unlockable=len(cls["UNLOCKABLE"]),
                   hard_403=len(cls["HARD_403"]),
                   other=len(cls["OTHER"]),
                   unlock_detail=";".join(cls["UNLOCKABLE"][:10]),
                   other_detail=";".join(cls["OTHER"][:6]))
        except Exception as e:
            rep.kv(stage="oecd-probe", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:150]}")
        verdict["gates"]["oecd_probe"] = "PASS"
        verdict["oecd"] = cls

        # ── G3 MIDAS endpoint discovery ─────────────────────────────
        midas = {}
        cands = [
            ("metrics-page",
             "https://www.sec.gov/marketstructure/downloads.html"),
            ("data-page",
             "https://www.sec.gov/marketstructure/datavis.html"),
            ("mstr-q-2026q1", "https://www.sec.gov/files/opa/data/"
             "market-structure/metrics-by-exchange/"
             "q1_2026_exchange.zip"),
            ("mstr-q-2025q4", "https://www.sec.gov/files/opa/data/"
             "market-structure/metrics-by-exchange/"
             "q4_2025_exchange.zip"),
            ("mstr-sec-2025q4", "https://www.sec.gov/files/opa/data/"
             "market-structure/metrics-individual-security/"
             "q4_2025_all.zip"),
            ("mstr-index", "https://www.sec.gov/files/opa/data/"
             "market-structure/marketstructuredata_bydate.xlsx")]
        for label, u in cands:
            st_, nb = http(u, {"User-Agent":
                               "JustHodl Research "
                               "raafouis@gmail.com"})
            midas[label] = {"status": st_, "bytes_head": nb}
            time.sleep(0.4)
        rep.kv(stage="midas-discovery",
               results=json.dumps(midas)[:400])
        verdict["gates"]["midas_discovery"] = "PASS"
        verdict["midas"] = midas

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4901.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4901.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
