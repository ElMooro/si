"""ops 6914 - ACTING: the sovereign-stress desk document, compiled for the first time by the deployed engine.

justhodl-ciss-stress now compiles data/ciss-sovereign.json (and its history) after its warehouse run, daily 07:10 UTC.
Today's warehouse run is done, so the document does not exist yet and the desk page would open empty until
tomorrow.  This invokes the engine ONCE outside its schedule with {"sovereign_only": true}: the warehouse is not
collected again (its 88 ECB requests are not repeated, its receipt and its 20 consumers are untouched); only the
desk document is compiled from the warehouse already published, plus seven requests to the ECB.  Disclosed to Khalid.

  1. Guard: the deployed package must hold this checkout's sovereign_desk.py and handler; otherwise no invoke.
  2. One synchronous invoke.
  3. Read back both documents: contract, coverage, the receipt (and that the document is the one the receipt names),
     every measure against the warehouse's own row, the warehouse untouched.
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
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared")]
LIVE, FN = "justhodl-dashboard-live", "justhodl-ciss-stress"
SOURCE = ROOT / "aws" / "lambdas" / FN / "source"
DESK, HISTORY, WAREHOUSE = "data/ciss-sovereign.json", "data/ciss-sovereign-history.json", "data/ciss-stress.json"
NEED = {"sovciss": 11, "euro_area": 6, "contribution": 6, "ciss": 10, "clifs": 20, "curve": 4, "yield": 20, "spread": 19, "target": 15, "debt": 20, "auxiliary": 3}


def differences(measures, rows):
    """Measures that do not equal the warehouse's own fresh row for the same ECB key."""
    out, checked = [], 0
    for x in measures.values():
        row = rows.get(x["key"] or "")
        if not row or row["quality"]["status"] != "fresh":
            continue
        checked += 1
        if x["period"] != row["latest_date"] or abs(x["value"] - row["latest"]) > 6e-6 or (row.get("pctile") is not None and abs(x["pctile"] - row["pctile"]) > 0.06):
            out.append("%s %s for %s against %s for %s" % (x["id"], x["value"], x["period"], row["latest"], row["latest_date"]))
    return checked, out


def selftest():
    rows = {"K": {"quality": {"status": "fresh"}, "latest_date": "2026-11-19", "latest": 0.5, "pctile": 50.0}, "S": {"quality": {"status": "stale"}}}
    same = {"a": {"id": "A", "key": "K", "period": "2026-11-19", "value": 0.5, "pctile": 50.0}, "b": {"id": "B", "key": "S"}, "c": {"id": "C", "key": None}}
    assert differences(same, rows) == (1, [])
    assert differences({"a": {**same["a"], "value": 0.6}}, rows)[1] and differences({"a": {**same["a"], "period": "2026-11-18"}}, rows)[1]


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import ciss_source_model as model
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=900))
    s3 = boto3.client("s3", region_name="us-east-1")
    fails = []

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return o["Body"].read(), o["LastModified"]

    with report("ops_6914_sovereign_desk_first_run") as r:
        r.heading("ops 6914 - the sovereign-stress desk document, first compile by the deployed engine, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard")
        g = lam.get_function(FunctionName=FN)
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in ("sovereign_desk.py", "lambda_function.py"):
            hits = [x for x in z.namelist() if x == name or x.endswith("/" + name)]
            same[name] = bool(hits) and z.read(hits[0]) == (SOURCE / name).read_bytes()
        r.log("%s updated %s | as in this checkout: %s | timeout %s s, memory %s MB" % (FN, (g["Configuration"].get("LastModified") or "")[:19], same, g["Configuration"].get("Timeout"), g["Configuration"].get("MemorySize")))
        if not all(same.values()) or g["Configuration"].get("Timeout", 0) < 600:
            r.fail("the deployed package is not this checkout's; nothing was invoked")
            sys.exit(1)
        ware_raw, ware_when = get(WAREHOUSE)
        ware_sha = hashlib.sha256(ware_raw).hexdigest()
        try:
            _, before = get(DESK)
            r.log("before: a desk document written %s exists" % before.strftime("%Y-%m-%d %H:%M"))
        except Exception:  # noqa: BLE001
            r.log("before: no desk document yet")

        r.section("2. One invoke, desk only")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"sovereign_only": True}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        took = time.time() - t0
        body = json.loads(payload.get("body") or "{}") if isinstance(payload, dict) else {}
        r.log("invoke: HTTP %s, function error %s, %.0fs | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), took, json.dumps(body)[:500]))
        result = body.get("sovereign") or {}
        if resp.get("FunctionError") or not result.get("published"):
            r.fail("the engine did not publish the desk document: %s" % json.dumps(payload)[:400])
            sys.exit(1)

        r.section("3. Read back")
        desk_raw, desk_when = get(DESK)
        hist_raw, hist_when = get(HISTORY)
        doc, hist = json.loads(desk_raw), json.loads(hist_raw)
        m = doc["measures"]
        r.log("desk %d bytes written %s | history %d bytes written %s" % (len(desk_raw), desk_when.strftime("%H:%M:%S"), len(hist_raw), hist_when.strftime("%H:%M:%S")))
        r.log("contract %s %s | generated %s | coverage %s | errors %s" % (doc.get("contract"), doc.get("version"), doc.get("generated_at"), json.dumps(doc.get("coverage")), json.dumps(doc.get("errors"))))
        if doc.get("contract") != "ciss-sovereign.v1" or doc.get("errors"):
            fails.append("contract or errors: %s %s" % (doc.get("contract"), json.dumps(doc.get("errors"))[:200]))
        manifest = json.loads(get(doc["replay"]["manifest_key"])[0])
        named = model.digest({k: v for k, v in doc.items() if k not in ("replay", "history")})
        r.log("receipt %s | names this document: %s | names this history: %s | compiler stored: %s" % (
            doc["replay"]["manifest_key"].split("/")[-1][:16], named == manifest["output_sha256"], model.digest(hist) == manifest["history_sha256"],
            get(manifest["compiler"]["key"])[0] == (SOURCE / "sovereign_desk.py").read_bytes()))
        if named != manifest["output_sha256"] or model.digest(hist) != manifest["history_sha256"] or doc["history"]["sha256"] != manifest["history_sha256"]:
            fails.append("the published documents are not the ones the receipt names")
        if doc["replay"]["manifest_key"] != "data/ciss-research/sovereign/runs/" + model.digest(manifest) + ".json":
            fails.append("the receipt is not filed under its own digest")
        groups = {}
        for x in m.values():
            groups[x["group"]] = groups.get(x["group"], 0) + 1
        r.log("measures by group: %s" % json.dumps(groups, sort_keys=True))
        for name, need in NEED.items():
            if groups.get(name, 0) < need:
                fails.append("group %s has %d measures, at least %d expected" % (name, groups.get(name, 0), need))
        missing = [k for k in m if not (hist["series"].get(k) or {}).get("d")]
        if missing:
            fails.append("measures without a history: %s" % missing[:6])
        ware = json.loads(ware_raw)
        checked, differ = differences(m, {x["key"]: x for x in ware["series"]})
        r.log("%d measures checked against their warehouse rows, %d differ %s" % (checked, len(differ), differ[:4]))
        if differ or checked < 40:
            fails.append("%d of %d measures differ from the warehouse" % (len(differ), checked))
        if doc["warehouse"]["manifest_key"] != ware["replay"]["manifest_key"]:
            fails.append("the document does not stand on the current warehouse run")
        after_raw, after_when = get(WAREHOUSE)
        untouched = hashlib.sha256(after_raw).hexdigest() == ware_sha and after_when == ware_when
        r.log("warehouse untouched: %s (written %s)" % (untouched, after_when.strftime("%Y-%m-%d %H:%M:%S")))
        if not untouched:
            fails.append("the warehouse changed during a desk-only run")
        for s in doc["sources"]:
            r.log("  %-16s %s series, %s rows, newest %s%s" % (s["name"], s.get("series"), s.get("rows", "-"), s.get("newest", "-"), " | CARRIED" if s.get("carried") else ""))
        ea, cross = m["EA_SOV_GDP"], doc["cross_section"]
        r.log("euro-area SovCISS %s on %s (lower than %s%% of its history) | %s | all issuers minus AAA, 10y: %s bp on %s | TARGET owed %s bn (%s)" % (
            ea["value"], ea["date"], round(100 - ea["pctile"], 1), " ".join("%s %s" % (c["area"], c["value"]) for c in cross["ranking"]),
            m["EA_CURVE_10Y"]["value"], m["EA_CURVE_10Y"]["date"], m["EA_TGT_DEBT"]["value"], m["EA_TGT_DEBT"]["date"]))
        r.kv(seconds=round(took), measures=len(m), countries=len(doc["countries"]), checked_against_warehouse=checked, differ=len(differ), failed_checks=len(fails))
        if fails:
            r.fail(" | ".join(fails))
            sys.exit(1)
        r.ok("the deployed engine compiled and published the sovereign-stress desk document; it replays, agrees with the warehouse, and the warehouse was not touched")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
