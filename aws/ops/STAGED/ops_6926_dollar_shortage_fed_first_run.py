"""ops 6926 - ACTING: the dollar-shortage document with the Federal Reserve's series, compiled for the first time by the deployed engine.

Khalid 2026-11-22 16:20 ET: "... verify again and add whatever dollar shortage metrics such as dxy or other".

justhodl-ciss-stress (dollar_shortage.py 1.1.0) now adds twelve Federal Reserve measures to the three ECB ones in
data/ciss-dollar-shortage.json.  Its next scheduled run is tomorrow 07:10 UTC, so until then the Sovereign desk's
"Dollar shortage" tab would have only the ECB's three.  This invokes the engine ONCE outside its schedule with
{"dollar_only": true}: no request goes to the ECB; the engine reads the desk document already published and the three
ECB responses it names, asks the site's own series resolver for 19 series one at a time (the same requests ops 6925
made in its dry run), and writes two objects - the series it read and the document.  Disclosed to Khalid.

  1. Guard: the deployed package must hold this checkout's dollar_shortage.py, world_history.py, sovereign_desk.py and
     handler.
  2. One synchronous invoke.
  3. Read back: contract, digest, the three ECB measures against the desk document and its weekly history, each of the
     twelve Federal Reserve measures against its series and against the dry run's document (ops 6925), the kept
     series, the worker handing the document to the site's pages, and that nothing else was written.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
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
WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"
UNTOUCHED = ("data/ciss-stress.json", "data/ciss-sovereign.json", "data/ciss-sovereign-history.json", "data/ciss-sovereign-world.json")
SAME = ("value", "date", "n", "start", "high", "low", "change", "pctile", "peaks", "tops", "key", "evidence", "source_row", "unit", "freq")


def problems(doc, desk_doc, hist):
    """What does not hold between the dollar-shortage document, the desk document and the desk's history; [] when all does."""
    out = []
    for mid in doc.get("ecb") or []:
        m, s, d = doc["measures"].get(mid), doc["series"].get(mid), desk_doc["measures"].get(mid)
        if not m or not s or not d:
            out.append("%s is missing somewhere" % mid)
            continue
        out += ["%s.%s differs from the desk document" % (mid, k) for k in SAME if m.get(k) != d.get(k)]
        if not (len(s["d"]) == len(s["v"]) == m["n"] and s["d"] == sorted(set(s["d"])) and s["d"][0] == m["start"] and s["d"][-1] == m["date"] and s["v"][-1] == m["value"]):
            out.append("%s: series and measure disagree" % mid)
        full, weekly = dict(zip(s["d"], s["v"])), (hist.get("series") or {}).get(mid) or {"d": [], "v": []}
        if not weekly["d"] or any(full.get(day) != value for day, value in zip(weekly["d"], weekly["v"])):
            out.append("%s: the desk's weekly history is not inside the complete series" % mid)
    return out


def against_dry_run(doc, dry):
    """Where the deployed engine's Federal Reserve measures differ from the dry run's; [] when none does."""
    out = []
    for mid, m in sorted(doc["measures"].items()):
        if m.get("source") != "fred":
            continue
        other = (dry.get("measures") or {}).get(mid)
        if not other:
            out.append("%s was not in the dry run" % mid)
            continue
        out += ["%s.%s: %s here, %s in the dry run" % (mid, k, m.get(k), other.get(k)) for k in ("value", "date", "n", "start", "high", "low", "unit", "freq", "status", "part", "worse") if m.get(k) != other.get(k)]
        if doc["series"][mid] != dry["series"].get(mid):
            out.append("%s: the series differs from the dry run's" % mid)
    return out


def selftest():
    f = {"value": 1.0, "date": "2026-01-02", "n": 2, "source": "fred"}
    one = {"measures": {"F": dict(f), "E": {"source": "ecb"}}, "series": {"F": {"d": ["2026-01-01", "2026-01-02"], "v": [0.5, 1.0]}}}
    assert against_dry_run(one, one) == []
    assert against_dry_run(one, {"measures": {"F": {**f, "value": 2.0}}, "series": one["series"]}) == ["F.value: 1.0 here, 2.0 in the dry run"]
    assert against_dry_run(one, {"measures": {}, "series": {}}) == ["F was not in the dry run"]
    m = {"value": 2.0, "date": "2026-01-02", "n": 2, "start": "2026-01-01", "high": [2.0, "2026-01-02"]}
    doc = {"ecb": ["A"], "measures": {"A": dict(m)}, "series": {"A": {"d": ["2026-01-01", "2026-01-02"], "v": [1.0, 2.0]}}}
    desk_doc, hist = {"measures": {"A": dict(m)}}, {"series": {"A": {"d": ["2026-01-02"], "v": [2.0]}}}
    assert problems(doc, desk_doc, hist) == []
    assert problems(doc, {"measures": {"A": {**m, "value": 3.0}}}, hist) == ["A.value differs from the desk document"]
    assert problems(doc, desk_doc, {"series": {"A": {"d": ["2026-01-02"], "v": [9.0]}}}) == ["A: the desk's weekly history is not inside the complete series"]


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import ciss_source_model as model
    import dollar_shortage as dollar
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
    s3 = boto3.client("s3", region_name="us-east-1")
    fails = []

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return o["Body"].read(), o["LastModified"]

    with report("ops_6926_dollar_shortage_fed_first_run") as r:
        r.heading("ops 6926 - the dollar-shortage document with the Federal Reserve's series, first compile by the deployed engine, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard")
        g = lam.get_function(FunctionName=FN)
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in ("dollar_shortage.py", "world_history.py", "sovereign_desk.py", "lambda_function.py"):
            hits = [x for x in z.namelist() if x == name or x.endswith("/" + name)]
            same[name] = bool(hits) and z.read(hits[0]) == (SOURCE / name).read_bytes()
        r.log("%s updated %s | as in this checkout: %s" % (FN, (g["Configuration"].get("LastModified") or "")[:19], same))
        if not all(same.values()):
            r.fail("the deployed package is not this checkout's; nothing was invoked")
            sys.exit(1)
        before = {key: hashlib.sha256(get(key)[0]).hexdigest() for key in UNTOUCHED}
        try:
            was = json.loads(get(dollar.CURRENT)[0])
            r.log("before: %s version %s, %d measures, generated %s" % (dollar.CURRENT, was.get("version"), len(was.get("measures") or {}), was.get("generated_at")))
        except Exception:  # noqa: BLE001
            r.log("before: %s does not exist yet" % dollar.CURRENT)
        try:
            get(dollar.STATE)
            r.log("before: the kept series exist already (%s)" % dollar.STATE)
        except Exception:  # noqa: BLE001
            r.log("before: no series are kept yet (%s)" % dollar.STATE)

        r.section("2. One invoke, the dollar-shortage document only")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"dollar_only": True}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        took = time.time() - t0
        body = json.loads(payload.get("body") or "{}") if isinstance(payload, dict) else {}
        r.log("invoke: HTTP %s, function error %s, %.1fs | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), took, json.dumps(body)[:600]))
        result = body.get("dollar") or {}
        if resp.get("FunctionError") or not result.get("published") or set(body) != {"dollar"}:
            r.fail("the engine did not publish the dollar-shortage document (or did more than asked): %s" % json.dumps(payload)[:400])
            sys.exit(1)

        r.section("3. Read back")
        raw, when = get(dollar.CURRENT)
        doc = json.loads(raw)
        desk_doc, hist = json.loads(get("data/ciss-sovereign.json")[0]), json.loads(get("data/ciss-sovereign-history.json")[0])
        r.log("document %d bytes written %s | contract %s %s | generated %s | stands on the desk document of %s" % (
            len(raw), when.strftime("%H:%M:%S"), doc.get("contract"), doc.get("version"), doc.get("generated_at"), doc["desk"]["generated_at"][:16]))
        if doc.get("contract") != dollar.CONTRACT or doc.get("version") != dollar.VERSION or doc.get("generated_at") != result.get("generated_at") or doc["desk"]["generated_at"] != desk_doc["generated_at"]:
            fails.append("contract, version, clock or the desk document it stands on")
        body_only = dict(doc)
        if body_only.pop("sha256", None) != model.digest(body_only):
            fails.append("the document's digest does not match its bytes")
        found = problems(doc, desk_doc, hist)
        for part in doc.get("parts") or []:
            r.log("%s" % part["name"])
            for mid in part["ids"]:
                m, s = doc["measures"][mid], doc["series"][mid]
                r.log("  %-16s %s %s on %s (%s) | %d observations %s .. %s | high %s | low %s | %s" % (mid, m["value"], m["unit"], m["date"], m["status"], len(s["d"]), s["d"][0], s["d"][-1], m["high"], m["low"], m["short"]))
                if not (len(s["d"]) == len(s["v"]) == m["n"] and s["d"] == sorted(set(s["d"])) and s["d"][0] == m["start"] and s["d"][-1] == m["date"] and s["v"][-1] == m["value"]):
                    fails.append("%s: series and measure disagree" % mid)
        r.log("the ECB's three against the desk document and its weekly history: %d problems %s" % (len(found), found[:4]))
        fails += found
        if doc.get("ecb") != ["AUX_EURUSD", "AUX_FX_EA", "AUX_FX_NONEA"] or len(doc["series"]["AUX_EURUSD"]["d"]) < 6500:
            fails.append("the three ECB measures, with the reference rate for every day")
        if len(doc["measures"]) != 15 or doc.get("errors") != {} or doc["coverage"].get("status") != "measured" or sorted(doc["order"]) != sorted(doc["measures"]) or result.get("series_asked") != 19:
            fails.append("fifteen measures without an error, from 19 series asked for: %d measures, errors %s, asked %s" % (len(doc["measures"]), json.dumps(doc.get("errors"))[:300], result.get("series_asked")))
        dry = json.loads(gzip.decompress(base64.b64decode((ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6925_fixture.md").read_text())))
        differs = against_dry_run(doc, dry)
        r.log("the twelve Federal Reserve measures against the dry run of %s: %d differences %s" % (dry["generated_at"][:16], len(differs), differs[:4]))
        fails += differs
        state = json.loads(get(dollar.STATE)[0])
        r.log("kept series: %d, read at %s" % (len(state.get("fred") or {}), sorted({v["read_at"][:16] for v in state["fred"].values()})))
        if state.get("contract") != dollar.STATE_CONTRACT or sorted(state["fred"]) != sorted(dollar.FRED_IDS):
            fails.append("the kept series are not the 19 the document stands on")
        try:
            req = urllib.request.Request(WORKER + "/" + dollar.CURRENT + "?t=%d" % time.time(), headers={"Origin": "https://justhodl.ai", "User-Agent": "jh-ops-probe"})
            with urllib.request.urlopen(req, timeout=60) as resp2:
                served = json.loads(resp2.read())
                allow = resp2.headers.get("access-control-allow-origin")
            r.log("through the worker the Dollar desk reads: HTTP 200, allow-origin %s, generated %s, same document: %s" % (allow, served.get("generated_at"), served.get("sha256") == doc.get("sha256")))
            if served.get("sha256") != doc.get("sha256") or allow not in ("*", "https://justhodl.ai"):
                fails.append("the worker does not hand this document to the site's pages")
        except Exception as e:  # noqa: BLE001
            fails.append("the worker refused the document: %s: %s" % (type(e).__name__, str(e)[:120]))
        touched = [key for key in UNTOUCHED if hashlib.sha256(get(key)[0]).hexdigest() != before[key]]
        r.log("the warehouse, the desk document, its history and the world histories are as before: %s" % (not touched))
        if touched:
            fails.append("changed during the run: %s" % touched)
        r.kv(failed_checks=len(fails), measures=len(doc["measures"]), observations=doc["coverage"]["observations"], bytes=len(raw), seconds=round(took, 1))
        if fails:
            for f in fails:
                r.fail(f)
            sys.exit(1)
        r.ok("the deployed engine published the dollar-shortage document with 3 ECB and 12 Federal Reserve measures; the ECB's equal the desk document, the rest equal the dry run, and nothing else was touched")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
