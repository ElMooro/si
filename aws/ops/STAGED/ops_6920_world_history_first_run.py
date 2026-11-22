"""ops 6920 - ACTING: the World tab's histories, compiled for the first time by the deployed engine.

justhodl-ciss-stress now also publishes data/ciss-sovereign-world.json after its daily run (07:10 UTC).  Today's run is
done, so the World tab's chart would stay empty until tomorrow.  This invokes the engine ONCE outside its schedule
with {"world_only": true}: no request goes to the ECB, the CISS warehouse and the desk document are not compiled
again; the engine reads the site's own stores (the desk's monthly yields, the CDS desk's measured prints, the world
table and its chain of past publications) and asks the site's series resolver for the OECD monthly yields, one at a
time.  It writes two objects: its day-by-day record (data/ciss-research/sovereign/world-state.json) and the document.
Disclosed to Khalid.

  1. Guard: the deployed package must hold this checkout's world_history.py, sovereign_desk.py and handler.
  2. One synchronous invoke.
  3. Read back: contract, the four sets, every series against its measure, the table's figures of the day at the end
     of the reported series, the warehouse and the desk document untouched.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-ciss-stress" / "source")]
LIVE, FN = "justhodl-dashboard-live", "justhodl-ciss-stress"
SOURCE = ROOT / "aws" / "lambdas" / FN / "source"
UNTOUCHED = ("data/ciss-stress.json", "data/ciss-sovereign.json", "data/ciss-sovereign-history.json", "data/global-sovereign.json", "data/cds-desk-history.json")
NEED = {"yield": 28, "cds": 20, "yield_reported": 40, "cds_reported": 34}


def consistent(doc):
    """Problems between the document's measures and its series; [] when they agree."""
    out, M, S = [], doc["measures"], doc["series"]
    if set(M) != set(S):
        out.append("measures and series are not the same ids")
    for k in sorted(set(M) & set(S)):
        m, s = M[k], S[k]
        if not (len(s["d"]) == len(s["v"]) == m["n"] and s["d"] == sorted(set(s["d"])) and s["d"][0] == m["start"] and s["d"][-1] == m["date"] and s["v"][-1] == m["value"]):
            out.append(k)
    for c in doc["countries"]:
        for sid, mid in c["ids"].items():
            if mid not in M or M[mid]["set"] != sid or M[mid]["country"] != c["country"]:
                out.append("%s.%s" % (c["country"], sid))
    return out


def selftest():
    good = {"measures": {"A": {"n": 2, "start": "2026-01-01", "date": "2026-01-02", "value": 2.0, "set": "yield", "country": "X"}},
            "series": {"A": {"d": ["2026-01-01", "2026-01-02"], "v": [1.0, 2.0]}}, "countries": [{"country": "X", "ids": {"yield": "A"}}]}
    assert consistent(good) == []
    bad = json.loads(json.dumps(good))
    bad["series"]["A"]["v"][-1] = 3.0
    assert consistent(bad) == ["A"]
    bad = json.loads(json.dumps(good))
    bad["countries"][0]["ids"]["cds"] = "A"
    assert consistent(bad) == ["X.cds"]


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import world_history as world
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=600, retries={"max_attempts": 0}))
    s3 = boto3.client("s3", region_name="us-east-1")
    fails = []

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return o["Body"].read(), o["LastModified"]

    with report("ops_6920_world_history_first_run") as r:
        r.heading("ops 6920 - the World tab's histories, first compile by the deployed engine, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard")
        g = lam.get_function(FunctionName=FN)
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in ("world_history.py", "sovereign_desk.py", "lambda_function.py"):
            hits = [x for x in z.namelist() if x == name or x.endswith("/" + name)]
            same[name] = bool(hits) and z.read(hits[0]) == (SOURCE / name).read_bytes()
        r.log("%s updated %s | as in this checkout: %s | timeout %s s" % (FN, (g["Configuration"].get("LastModified") or "")[:19], same, g["Configuration"].get("Timeout")))
        if not all(same.values()):
            r.fail("the deployed package is not this checkout's; nothing was invoked")
            sys.exit(1)
        before = {}
        for key in UNTOUCHED:
            raw, when = get(key)
            before[key] = (hashlib.sha256(raw).hexdigest(), when)
        for key in (world.CURRENT, world.STATE):
            try:
                r.log("before: %s written %s" % (key, get(key)[1].strftime("%Y-%m-%d %H:%M")))
            except Exception:  # noqa: BLE001
                r.log("before: %s does not exist yet" % key)

        r.section("2. One invoke, the world histories only")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"world_only": True}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        took = time.time() - t0
        body = json.loads(payload.get("body") or "{}") if isinstance(payload, dict) else {}
        r.log("invoke: HTTP %s, function error %s, %.0fs | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), took, json.dumps(body)[:600]))
        result = body.get("world") or {}
        if resp.get("FunctionError") or not result.get("published") or set(body) != {"world"}:
            r.fail("the engine did not publish the world histories (or did more than asked): %s" % json.dumps(payload)[:400])
            sys.exit(1)

        r.section("3. Read back")
        raw, when = get(world.CURRENT)
        state_raw, state_when = get(world.STATE)
        doc, state = json.loads(raw), json.loads(state_raw)
        M = doc["measures"]
        r.log("document %d bytes written %s | record %d bytes written %s" % (len(raw), when.strftime("%H:%M:%S"), len(state_raw), state_when.strftime("%H:%M:%S")))
        r.log("contract %s %s | generated %s | coverage %s | errors %s" % (doc.get("contract"), doc.get("version"), doc.get("generated_at"), json.dumps(doc.get("coverage")), json.dumps(doc.get("errors"))[:300]))
        if doc.get("contract") != world.CONTRACT or doc.get("version") != world.VERSION or doc.get("generated_at") != result.get("generated_at"):
            fails.append("contract, version or clock: %s %s %s" % (doc.get("contract"), doc.get("version"), doc.get("generated_at")))
        if doc.get("errors"):
            fails.append("the document lists errors: %s" % json.dumps(doc["errors"])[:300])
        body_only = dict(doc)
        sha = body_only.pop("sha256", None)
        import ciss_source_model as model
        r.log("the document carries its own digest: %s" % (sha == model.digest(body_only)))
        if sha != model.digest(body_only):
            fails.append("the document's digest does not match its bytes")
        sets = {s["id"]: s["n"] for s in doc["sets"]}
        r.log("sets: %s | sources %s" % (json.dumps(sets), json.dumps(doc["coverage"]["by_source"], sort_keys=True)))
        for sid, need in NEED.items():
            if sets.get(sid, 0) < need:
                fails.append("set %s has %d countries, fewer than %d" % (sid, sets.get(sid, 0), need))
            r.log("  %-15s not held for: %s" % (sid, ", ".join(doc["missing"][sid]) or "nobody"))
        problems = consistent(doc)
        r.log("every series against its measure: %d measures, %d disagree %s" % (len(M), len(problems), problems[:6]))
        if problems:
            fails.append("series and measures disagree: %s" % problems[:6])
        table = json.loads(get("data/global-sovereign.json")[0])
        if [c["country"] for c in doc["countries"]] != [c["country"] for c in table["countries"]] or doc["table"]["generated_at"] != table["generated_at"]:
            fails.append("the document does not stand on today's world table")
        off, checked = [], 0
        for c, row in zip(doc["countries"], table["countries"]):
            for sid, field in (("yield_reported", "yield_10y_pct"), ("cds_reported", "cds_bp")):
                mid = c["ids"].get(sid)
                if mid and isinstance(row.get(field), (int, float)) and M[mid]["date"] == world.provider_day(row.get("as_of")):
                    checked += 1
                    if abs(M[mid]["value"] - row[field]) > 1e-9:
                        off.append((c["country"], sid, M[mid]["value"], row[field]))
        r.log("the table's figures of the day at the end of the reported series: %d checked, %d differ %s" % (checked, len(off), off[:3]))
        if off or checked < 60:
            fails.append("reported series do not end on the table's figures (%d checked): %s" % (checked, off[:3]))
        if state.get("contract") != world.STATE_CONTRACT or state.get("last_publication") != table["generated_at"]:
            fails.append("the day-by-day record is not at today's publication")
        for name in ("WY_UNITED_STATES", "WY_GERMANY", "WY_FRANCE", "WC_TURKEY", "WC_BRAZIL", "RY_TURKEY", "RC_TURKEY", "RY_VIETNAM"):
            m = M.get(name)
            r.log("  %-17s %s" % (name, ("%s | %s .. %s | %d points | last %s | %s" % (m["source"], m["start"], m["date"], m["n"], m["value"], m["status"])) if m else "absent"))
            if not m:
                fails.append("%s is absent" % name)
        touched = [key for key in UNTOUCHED if hashlib.sha256(get(key)[0]).hexdigest() != before[key][0]]
        r.log("the warehouse, the desk document, the world table and the CDS desk's file are as before: %s" % (not touched))
        if touched:
            fails.append("changed during the run (another engine may have written it; check the times): %s" % touched)
        r.kv(failed_checks=len(fails), countries=len(doc["countries"]), measures=len(M), seconds=round(took), publications_folded=result.get("publications_folded"), series_asked=result.get("series_asked"), **sets)
        if fails:
            for f in fails:
                r.fail(f)
            sys.exit(1)
        r.ok("the deployed engine published the World tab's histories; they agree with the table and with themselves, and nothing else was touched")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
