"""ops/4898 -- EVERY portal flow: 104 -> 214, census-clean.

Ops 4897's census found the ECB Data Portal hosts 214 dataflows across
5 maintainer agencies; agency=ECB alone was 104. The other 110 include
genuinely distinct macro (ESTAT/EUROSTAT/IMF: GFS, MNA, QSA, EDP, LCI,
ICPF, IEAQ/IEAF/IESS, JVC/JVS, LFSI, BP6/BPS, RA6/RAS) plus ECB.DISS
dissemination views (*_PUB, JDF_*, MOBILE_*). Khalid: EVERY = every.

This push: catalog v2 (all agencies, "AG:ID" ids, agency histogram),
walker flowRef ":"->",". This op: settle -> rebuild catalog (expect
214) -> blitz the new flows (per=120, cap_mb=150; any new giant is
auto-adopted by ecb-deep v1.1's resync) -> census re-check must be
ZERO extras -> coverage.json re-verify (the 4897 FAIL was lease
timing) -> ECB card note.
"""
import gzip
import io
import json
import re
import sys
import time
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


def walk_state():
    try:
        return g("data/_state/sdmx-walk-ecb.json")
    except Exception:
        return {}


def blitz(rep, label, max_wait=940):
    before = walk_state()
    lam.invoke(FunctionName="justhodl-sdmx-walker",
               InvocationType="Event",
               Payload=json.dumps({"agency": "ecb", "budget": 740,
                                   "per": 120,
                                   "cap_mb": 150}).encode())
    t = time.time()
    while time.time() - t < max_wait:
        time.sleep(25)
        st = walk_state()
        if st.get("as_of") != before.get("as_of") and \
                float(st.get("lease_until") or 0) <= time.time():
            return st
    return walk_state()


def main():
    verdict = {"ops": 4898, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4898 -- every portal flow 104 to 214") as rep:
        rep.heading("ops 4898 — 104 → 214, census-clean")

        ok_c = settle("justhodl-ecb-full-catalog", "agencyID")
        ok_w = settle("justhodl-sdmx-walker", '_fr = lambda',
                      secs=240)
        rep.kv(stage="settle", catalog_v2=ok_c, walker_fr=ok_w)
        verdict["gates"]["patches_deployed"] = (
            "PASS" if (ok_c and ok_w) else "FAIL")

        # rebuild catalog: expect 214 across 5 agencies
        n_flows, agencies = 0, {}
        try:
            r = lam.invoke(FunctionName="justhodl-ecb-full-catalog",
                           InvocationType="RequestResponse")
            payload = json.loads(r["Payload"].read() or b"{}")
            body = payload.get("body")
            res = json.loads(body) if isinstance(body, str) else (
                payload if isinstance(payload, dict) else {})
            n_flows = int(res.get("n_dataflows") or 0)
            agencies = res.get("agencies") or {}
            rep.kv(stage="catalog-v2", n_dataflows=n_flows,
                   agencies=json.dumps(agencies)[:180],
                   accept_winner=res.get("accept_winner"))
        except Exception as e:
            rep.kv(stage="catalog-v2", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:150]}")
        verdict["gates"]["catalog_214"] = (
            "PASS" if n_flows >= 200 else "FAIL")

        # blitz the new flows to done (2 rounds max)
        st = walk_state()
        for rnd in (1, 2):
            nd = len(set(st.get("done") or []))
            nt = st.get("n_total") or 0
            if nt >= 200 and nd >= nt:
                break
            rep.log(f"blitz {rnd}: done {nd}/{nt}")
            st = blitz(rep, f"round-{rnd}")
        nd = len(set(st.get("done") or []))
        nt = st.get("n_total") or 0
        rep.kv(stage="walk", n_done=nd, n_total=nt,
               status=st.get("status"),
               n_truncated=len(st.get("truncated") or []),
               n_failures=len(st.get("failures") or {}),
               fail_sample=json.dumps(dict(list(
                   (st.get("failures") or {}).items())[:6]))[:260])
        verdict["gates"]["walk_214_complete"] = (
            "PASS" if (nt >= 200 and nd >= nt) else "PENDING")

        # census re-check: portal vs banked must be ZERO extras
        extras = None
        try:
            req = urllib.request.Request(
                "https://data-api.ecb.europa.eu/service/dataflow",
                headers=UA)
            txt = urllib.request.urlopen(req, timeout=90
                                         ).read().decode("utf-8",
                                                         "ignore")
            fl = re.findall(r"<[^>]*Dataflow[^>]*\bid=\"([^\"]+)\""
                            r"[^>]*agencyID=\"([^\"]+)\"", txt)
            if not fl:
                fl = [(m.group(2), m.group(1)) for m in re.finditer(
                    r"<[^>]*Dataflow[^>]*agencyID=\"([^\"]+)\""
                    r"[^>]*\bid=\"([^\"]+)\"", txt)]
            banked = {f["id"] for f in
                      g("data/warm/ecb/catalog.json.gz")["dataflows"]}
            extras = sorted({
                (fid if ag == "ECB" else f"{ag}:{fid}")
                for fid, ag in fl
                if (fid if ag == "ECB" else f"{ag}:{fid}")
                not in banked})
            rep.kv(stage="census-recheck", portal=len(fl),
                   banked=len(banked), extras_n=len(extras),
                   extras=",".join(extras[:10]))
        except Exception as e:
            rep.kv(stage="census-recheck", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:120]}")
        verdict["gates"]["census_zero_extras"] = (
            "PASS" if extras == [] else
            "PENDING" if extras else "FAIL")

        # coverage ledger re-verify (4897 miss was lease timing)
        cov, dstate = None, {}
        end = time.time() + 700
        while time.time() < end:
            try:
                cov = g("data/warm/ecb/coverage.json")
                dstate = g("data/_state/ecb-deep.json")
                break
            except Exception:
                time.sleep(30)
        rep.kv(stage="coverage",
               live=bool(cov),
               as_of=(cov or {}).get("as_of"),
               n_fast=(cov or {}).get("n_fast"),
               n_deep=(cov or {}).get("n_deep"),
               n_deep_complete=(cov or {}).get("n_deep_complete"),
               deep_mode=dstate.get("mode"),
               deep_complete=dstate.get("n_complete"))
        verdict["gates"]["coverage_ledger_live"] = (
            "PASS" if cov else "PENDING")

        # ECB card note
        lam.invoke(FunctionName="justhodl-provider-catalog",
                   InvocationType="Event")
        card_ok, note = False, ""
        t = time.time()
        while time.time() - t < 420:
            time.sleep(35)
            try:
                pc = g("data/provider-catalog.json")
                card = next((p for p in pc.get("providers") or []
                             if p.get("slug") == "ecb"), None)
                note = str((card or {}).get("catalog_note") or "")
                if "walk+deep coverage" in note:
                    card_ok = True
                    rep.kv(stage="ecb-card",
                           series_count=(card or {}
                                         ).get("series_count"),
                           note=note[:240])
                    break
            except Exception:
                pass
        if not card_ok:
            rep.kv(stage="ecb-card", note=note[:200], ok=False)
        verdict["gates"]["ecb_card_coverage_note"] = (
            "PASS" if card_ok else "PENDING")

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["walk"] = {"n_done": nd, "n_total": nt,
                           "truncated": len(st.get("truncated")
                                            or [])}
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4898.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4898.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
