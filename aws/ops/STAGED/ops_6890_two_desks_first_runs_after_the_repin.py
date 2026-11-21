"""ops 6890 - ACTING: the bond desk and the sector fusion, first runs after their pins were brought in step with the
deployed ETF compilers.

Dry run first: ops 6889 (read-only, 19:33 UTC) ran both consumers' own code against the live ETF publication with the
five digests of the files that are actually deployed - the bond desk's cohort compiler accepted it and its independent
check passed (11 cohorts, 33 windows, 138 members), the sector fusion restored the issuer packet (11 sectors).  With
the old four pins both had refused every publication since 2026-11-11.

  1. The three deployed packages are downloaded and their pins (and the fusion's copy of the issuer parser) compared
     byte for byte with this checkout; if any differs the script stops and invokes nothing.
  2. One run of justhodl-bond-desk (its schedule is weekdays 15:15 UTC - today's only run), then the published packet:
     flow_status must leave "withheld_ValueError".
  3. One run of justhodl-sector-flow-state (weekdays 21:15 UTC), then its publication: a new generated_at.
     justhodl-sector-capital-fusion is not invoked: it reads the flow state on its own schedule; its state is reported.
The only writes are the two engines' own daily publications.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
PACKAGES = {"justhodl-bond-desk": {"bond_flow_qualification.py": "aws/lambdas/justhodl-bond-desk/source/bond_flow_qualification.py"},
            "justhodl-sector-flow-state": {"sector_fusion_pins.py": "aws/shared/sector_fusion_pins.py", "sector_issuer_native.py": "aws/shared/sector_issuer_native.py"},
            "justhodl-sector-capital-fusion": {"sector_fusion_pins.py": "aws/shared/sector_fusion_pins.py", "sector_issuer_native.py": "aws/shared/sector_issuer_native.py"}}


def flows_of(doc):
    return (((doc.get("regions") or {}).get("us") or {}).get("flows") or {}) if isinstance(doc, dict) else {}


def selftest():
    assert flows_of({"regions": {"us": {"flows": {"flow_status": "x"}}}}) == {"flow_status": "x"} and flows_of({}) == {} and flows_of(None) == {}
    assert set(PACKAGES) == {"justhodl-bond-desk", "justhodl-sector-flow-state", "justhodl-sector-capital-fusion"}
    for files in PACKAGES.values():
        for path in files.values():
            assert (ROOT / path).is_file(), path


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=330))
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))

    def jget(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]

    with report("ops_6890_two_desks_first_runs_after_the_repin") as r:
        r.heading("ops 6890 - bond desk and sector fusion: first runs after the re-pin, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. What is deployed")
        ready = True
        for fn, files in PACKAGES.items():
            g = lam.get_function(FunctionName=fn)
            with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            same = {}
            for name, path in files.items():
                hits = [n for n in z.namelist() if n == name or n.endswith("/" + name)]
                same[name] = bool(hits) and z.read(hits[0]) == (ROOT / path).read_bytes()
            ready = ready and all(same.values())
            r.log("%-32s updated %s | %s" % (fn, (g["Configuration"].get("LastModified") or "")[:19], json.dumps(same)))
        if not ready:
            r.fail("a deployed package does not carry this checkout's pins yet; nothing was invoked")
            sys.exit(1)
        r.ok("the three packages carry this checkout's pins and parser copy")

        r.section("2. Bond desk")
        before, when = jget("data/bond-desk.json")
        r.log("before: written %s, version %s, flow_status %s" % (when.strftime("%Y-%m-%d %H:%M"), before.get("version"), flows_of(before).get("flow_status")))
        t0 = time.time()
        res = lam.invoke(FunctionName="justhodl-bond-desk", InvocationType="RequestResponse", Payload=b"{}")
        body = res["Payload"].read()[:600].decode("utf-8", "replace")
        r.log("invoke: status %s%s in %.0f s | %s" % (res.get("StatusCode"), " FunctionError %s" % res.get("FunctionError") if res.get("FunctionError") else "", time.time() - t0, body[:300]))
        after, when2 = jget("data/bond-desk.json")
        fl = flows_of(after)
        research = fl.get("flow_research") or {}
        r.log("after: written %s, generated_at %s, flow_status %s, cohort contract %s" % (when2.strftime("%Y-%m-%d %H:%M"), after.get("generated_at"), fl.get("flow_status"), research.get("contract")))
        cohorts = research.get("cohorts") or {}
        for name, c in list(cohorts.items())[:12]:
            w = ((c.get("windows") or {}).get("5_observations") or {})
            r.log("  cohort %-22s %s | funds included %s | complete total %s | coverage subtotal %s" % (name, w.get("status"), w.get("included_count"), w.get("complete_cohort_decimal"), w.get("coverage_subtotal_decimal")))
        bond_ok = fl.get("flow_status") == "dated_issuer_cohorts" and when2 > when
        if bond_ok:
            r.ok("the bond desk publishes its dated issuer cohorts again (was withheld_ValueError since 2026-11-11)")
        else:
            r.fail("bond desk flow_status is %s" % fl.get("flow_status"))

        r.section("3. Sector fusion")
        try:
            sbefore, swhen = jget("data/sector-flow-state.json")
        except Exception as e:  # noqa: BLE001
            sbefore, swhen = {}, None
            r.log("sector-flow-state before: %s" % type(e).__name__)
        r.log("before: written %s, generated_at %s, contract %s" % (swhen.strftime("%Y-%m-%d %H:%M") if swhen else "-", sbefore.get("generated_at"), sbefore.get("contract")))
        t0 = time.time()
        res = lam.invoke(FunctionName="justhodl-sector-flow-state", InvocationType="RequestResponse", Payload=b"{}")
        body = res["Payload"].read()[:900].decode("utf-8", "replace")
        r.log("invoke: status %s%s in %.0f s | %s" % (res.get("StatusCode"), " FunctionError %s" % res.get("FunctionError") if res.get("FunctionError") else "", time.time() - t0, body[:600]))
        safter, swhen2 = jget("data/sector-flow-state.json")
        r.log("after: written %s, generated_at %s, contract %s, sectors %s" % (swhen2.strftime("%Y-%m-%d %H:%M"), safter.get("generated_at"), safter.get("contract"), len(safter.get("sectors") or safter.get("rows") or [])))
        sector_ok = bool(swhen) and swhen2 > swhen and safter.get("generated_at") != sbefore.get("generated_at")
        if sector_ok:
            r.ok("the sector flow state is published again (last publication before this was %s)" % sbefore.get("generated_at"))
        else:
            r.warn("the sector flow state was not republished by this run - the invoke answer above says why")
        try:
            cap, cwhen = jget("data/sector-capital-fusion.json")
            r.log("sector-capital-fusion (not invoked): written %s, generated_at %s" % (cwhen.strftime("%Y-%m-%d %H:%M"), cap.get("generated_at")))
        except Exception as e:  # noqa: BLE001
            r.log("sector-capital-fusion: %s" % type(e).__name__)
        r.kv(bond_desk_flows_published=bond_ok, sector_flow_state_published=sector_ok)
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
