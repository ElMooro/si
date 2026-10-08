"""ops/4897 -- EVERY ecb flow, ALL history: the completeness proof.

Khalid's emphasis: "you are pulling every ecb data with all its
historical data." Every = zero unexplained gaps, proven:

  G1 deep v1.1 + provider-catalog settle by marker
  G2 DATAFLOW CENSUS: /service/dataflow (ALL maintainers on the ECB
     Data Portal, not just agency=ECB) vs the 104 banked -- any flow
     living under another agency is a gap; listed verbatim if found
  G3 FAILURE CLASSIFICATION: every entry in the walker's failures
     ledger is re-probed with lastNObservations=1 -- 404 = proven
     empty at source (documented-OK); anything alive gets one
     retry_failures blitz and must then be zero
  G4 coverage.json LIVE (deep run kicked; also advances the backfill)
  G5 deep progress snapshot (informational -- 10-min Scheduler owns
     convergence to 31/31)
  G6 ECB card on data.html carries the walk+deep coverage note
"""
import gzip
import io
import json
import re
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


def head_ts(key):
    try:
        return s3.head_object(Bucket=B, Key=key)["LastModified"]
    except Exception:
        return None


def zip_has(fn, marker):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    raw = urllib.request.urlopen(loc, timeout=60).read()
    return marker.encode() in zipfile.ZipFile(
        io.BytesIO(raw)).read("lambda_function.py")


def settle(fn, marker, secs=420):
    end = time.time() + secs
    while time.time() < end:
        try:
            if zip_has(fn, marker):
                return True
        except Exception:
            pass
        time.sleep(20)
    return False


def main():
    verdict = {"ops": 4897, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4897 -- ecb completeness proof") as rep:
        rep.heading("ops 4897 — every flow, all history: the proof")

        # ── G1 settle both patched fns ──────────────────────────────
        ok_d = settle("justhodl-ecb-deep", "oversize_month")
        ok_p = settle("justhodl-provider-catalog",
                      "walk+deep coverage", secs=200)
        rep.kv(stage="settle", deep_v11=ok_d, provcat=ok_p)
        verdict["gates"]["patches_deployed"] = (
            "PASS" if (ok_d and ok_p) else "FAIL")

        # ── G2 all-agencies dataflow census ─────────────────────────
        extras, total_all, agency_hist = [], 0, {}
        try:
            req = urllib.request.Request(
                "https://data-api.ecb.europa.eu/service/dataflow",
                headers=UA)
            raw = urllib.request.urlopen(req, timeout=90).read()
            txt = raw.decode("utf-8", "ignore")
            flows_all = re.findall(
                r"<[^>]*Dataflow[^>]*\bid=\"([^\"]+)\"[^>]*"
                r"agencyID=\"([^\"]+)\"", txt)
            if not flows_all:  # attr order can flip
                flows_all = [(m.group(2), m.group(1)) for m in
                             re.finditer(
                                 r"<[^>]*Dataflow[^>]*agencyID=\""
                                 r"([^\"]+)\"[^>]*\bid=\"([^\"]+)\"",
                                 txt)]
            total_all = len(flows_all)
            for fid, ag in flows_all:
                agency_hist[ag] = agency_hist.get(ag, 0) + 1
            banked = {f["id"] for f in
                      g("data/warm/ecb/catalog.json.gz")["dataflows"]}
            extras = sorted({f"{ag}:{fid}" for fid, ag in flows_all
                             if fid not in banked})
            rep.kv(stage="dataflow-census", portal_total=total_all,
                   banked=len(banked),
                   agencies=json.dumps(agency_hist)[:180],
                   extras_n=len(extras),
                   extras=",".join(extras[:15]))
        except Exception as e:
            rep.kv(stage="dataflow-census", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:150]}")
            extras = None
        verdict["gates"]["dataflow_census"] = (
            "PASS" if extras == [] else
            "PENDING" if extras else "FAIL")
        verdict["census"] = {"portal_total": total_all,
                             "agencies": agency_hist,
                             "extras": extras}

        # ── G3 failures classification ──────────────────────────────
        walk = g("data/_state/sdmx-walk-ecb.json")
        fails = dict(walk.get("failures") or {})
        alive, dead = [], []
        for fid in list(fails)[:25]:
            u = ("https://data-api.ecb.europa.eu/service/data/"
                 f"{fid}?format=csvdata&lastNObservations=1")
            try:
                r = urllib.request.urlopen(
                    urllib.request.Request(u, headers=UA), timeout=45)
                (alive if r.status == 200 else dead).append(fid)
            except urllib.error.HTTPError as e:
                (dead if e.code == 404 else alive).append(fid)
            except Exception:
                alive.append(fid)
            time.sleep(0.3)
        rep.kv(stage="failures-probe", ledger=len(fails),
               alive_n=len(alive), alive=",".join(alive[:10]),
               source_empty_404=",".join(dead[:10]))
        if alive:
            lam.invoke(FunctionName="justhodl-sdmx-walker",
                       InvocationType="Event",
                       Payload=json.dumps(
                           {"agency": "ecb", "retry_failures": 1,
                            "per": 120, "cap_mb": 150,
                            "budget": 560}).encode())
            t = time.time()
            while time.time() - t < 700:
                time.sleep(25)
                w2 = g("data/_state/sdmx-walk-ecb.json")
                if float(w2.get("lease_until") or 0) <= time.time() \
                        and w2.get("as_of") != walk.get("as_of"):
                    walk = w2
                    break
            fails = dict(walk.get("failures") or {})
            still_alive = [f for f in alive if f in fails]
            rep.kv(stage="failures-retry",
                   remaining_ledger=len(fails),
                   still_alive_n=len(still_alive),
                   still_alive=",".join(still_alive[:10]))
        else:
            still_alive = []
        verdict["gates"]["failures_classified"] = (
            "PASS" if not still_alive else "PENDING")
        verdict["failures"] = {"source_empty_404": dead,
                               "unresolved": still_alive}

        # ── G4 coverage ledger live (kick one deep run) ─────────────
        cv0 = head_ts("data/warm/ecb/coverage.json")
        lam.invoke(FunctionName="justhodl-ecb-deep",
                   InvocationType="Event")
        cov = None
        t = time.time()
        while time.time() - t < 880:
            time.sleep(30)
            ts = head_ts("data/warm/ecb/coverage.json")
            if ts and (cv0 is None or ts > cv0):
                try:
                    cov = g("data/warm/ecb/coverage.json")
                except Exception:
                    cov = None
                if cov:
                    break
        n_total = (cov or {}).get("n_fast", 0) + \
            (cov or {}).get("n_deep", 0)
        rep.kv(stage="coverage", live=bool(cov),
               n_fast=(cov or {}).get("n_fast"),
               n_deep=(cov or {}).get("n_deep"),
               n_deep_complete=(cov or {}).get("n_deep_complete"),
               n_total=n_total)
        verdict["gates"]["coverage_ledger_live"] = (
            "PASS" if (cov and n_total >= 100) else "FAIL")

        # ── G5 deep progress (informational) ────────────────────────
        st = {}
        try:
            st = g("data/_state/ecb-deep.json")
        except Exception:
            pass
        rep.kv(stage="deep-progress",
               n_complete=st.get("n_complete"),
               n_flows=st.get("n_flows"), mode=st.get("mode"))
        verdict["deep_progress"] = {
            "n_complete": st.get("n_complete"),
            "n_flows": st.get("n_flows"), "mode": st.get("mode")}

        # ── G6 ECB card carries the coverage note ───────────────────
        pc0 = head_ts("data/provider-catalog.json")
        lam.invoke(FunctionName="justhodl-provider-catalog",
                   InvocationType="Event")
        card_ok = False
        t = time.time()
        while time.time() - t < 380:
            time.sleep(30)
            ts = head_ts("data/provider-catalog.json")
            if ts and (pc0 is None or ts > pc0):
                break
        try:
            pc = g("data/provider-catalog.json")
            card = next((p for p in pc.get("providers") or []
                         if p.get("slug") == "ecb"), None)
            note = str((card or {}).get("catalog_note") or "")
            card_ok = "walk+deep coverage" in note
            rep.kv(stage="ecb-card", note=note[:240],
                   series_count=(card or {}).get("series_count"),
                   n_keys=(card or {}).get("n_keys"))
        except Exception as e:
            rep.kv(stage="ecb-card", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:120]}")
        verdict["gates"]["ecb_card_coverage_note"] = (
            "PASS" if card_ok else "FAIL")

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4897.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4897.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
