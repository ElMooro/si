"""ops 6903 - ACTING: one run of justhodl-nyfed-pd 4.1.0, then its published snapshot read back and reproduced.

The engine has published nothing since 2026-11-06 (ops 6899: its one request for all 70 series answers 422).  4.1.0
asks in five parts; ops 6902 ran the whole engine from the checkout with every write kept in memory and it completed.
Its own schedule is Thursday and Friday 21:36 UTC, so without this run the document would stay at the week of
2026-10-28 until 2026-11-26.  One invoke, outside that schedule (disclosed to Khalid).

  1. Guard: the deployed package must hold this checkout's dealer_original.py; otherwise nothing is invoked.
  2. One synchronous invoke.
  3. Read back: version, week, the series' newest dates, Treasury financing both sides, which original responses the
     snapshot holds - and the snapshot is reproduced here from its retained original responses (the engine's replay).
"""
from collections import Counter
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
sys.path[:0] = [str(ROOT / "aws" / "lambdas" / "justhodl-nyfed-pd" / "source"), str(ROOT / "aws" / "shared")]
LIVE, FN = "justhodl-dashboard-live", "justhodl-nyfed-pd"


def last_published(history):
    """The newest row of a group's history that carries a number: [date, usd_mn]."""
    for row in reversed(history or []):
        if row[1] is not None:
            return row[:2]
    return None


def selftest():
    assert last_published([["a", 1, "p", True], ["b", None, "p", False]]) == ["a", 1] and last_published([]) is None


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import dealer_original as native
    import dealer_research as model
    import dealer_research_store as store
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=450))
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))
    logs = boto3.client("logs", region_name="us-east-1")

    def current():
        o = s3.get_object(Bucket=LIVE, Key=model.CURRENT)
        return json.loads(o["Body"].read()), o["LastModified"]

    with report("ops_6903_dealer_engine_first_run") as r:
        r.heading("ops 6903 - justhodl-nyfed-pd 4.1.0: one run and its snapshot reproduced, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. What is deployed")
        g = lam.get_function(FunctionName=FN)
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in ("dealer_original.py", "dealer_research.py", "dealer_research_store.py"):
            hits = [x for x in z.namelist() if x == name or x.endswith("/" + name)]
            same[name] = bool(hits) and z.read(hits[0]) == (ROOT / "aws/lambdas/justhodl-nyfed-pd/source" / name).read_bytes()
        r.log("%s updated %s | modules as in this checkout: %s | timeout %s s" % (FN, (g["Configuration"].get("LastModified") or "")[:19], same, g["Configuration"].get("Timeout")))
        if not all(same.values()):
            r.fail("the deployed package is not 4.1.0; nothing was invoked")
            sys.exit(1)

        r.section("2. One run")
        before, when0 = current()
        r.log("before: written %s | version %s | as_of %s | quality %s" % (when0.strftime("%Y-%m-%d %H:%M"), before.get("version"), before.get("as_of"), json.dumps((before.get("quality") or {}).get("counts"))))
        t0 = time.time()
        res = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = res["Payload"].read().decode("utf-8", "replace")
        took = time.time() - t0
        try:
            outer = json.loads(raw)
            body = json.loads(outer.get("body") or "{}")
        except Exception:  # noqa: BLE001
            outer, body = {}, {}
        r.log("invoke: HTTP %s%s, engine status %s, %.0f s | %s" % (res.get("StatusCode"), " FunctionError %s" % res.get("FunctionError") if res.get("FunctionError") else "", outer.get("statusCode"), took, json.dumps(body)[:600] if body else raw[:400]))
        if outer.get("statusCode") != 200 or body.get("published") is not True:
            try:
                st = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime", descending=True, limit=1)["logStreams"][0]
                ev = logs.get_log_events(logGroupName="/aws/lambda/" + FN, logStreamName=st["logStreamName"], limit=30, startFromHead=False)["events"]
                r.log("log: %s" % " || ".join(e["message"].strip()[:200] for e in ev if not e["message"].startswith(("START", "END", "INIT_START"))))
            except Exception as e:  # noqa: BLE001
                r.log("log unreadable: %s" % type(e).__name__)
            r.fail("the run did not publish")
            sys.exit(1)

        r.section("3. Read back")
        after, when = current()
        ser, groups, fin = after.get("native_series") or {}, after.get("groups") or {}, after.get("financing") or {}
        t = fin.get("treasury") or {}
        newest = Counter(v.get("as_of") for v in ser.values())
        r.log("after: written %s | version %s | generated_at %s | source_generated_at %s | as_of %s | quality %s" % (
            when.strftime("%Y-%m-%d %H:%M"), after.get("version"), after.get("generated_at"), after.get("source_generated_at"), after.get("as_of"), json.dumps(after.get("quality"))[:240]))
        r.log("%d series | newest report dates %s | original responses %s | sources %s" % (
            len(ser), dict(newest), dict(Counter(v.get("original_response") for v in ser.values())), sorted(k for k in (after.get("sources") or {}) if k.startswith("observations"))))
        r.log("Treasury financing %s: repo out %s | reverse repo in %s | two-sided %s" % (t.get("as_of"), t.get("repo_out_b"), t.get("reverse_repo_in_b"), t.get("gross_two_sided_b")))
        rows = {}
        for gid in ("treasury_repo", "treasury_reverse_repo"):
            grp = groups.get(gid) or {}
            rows[gid] = last_published(grp.get("history"))
            r.log("  %-22s %s: %s USD mn, %s (missing %s) | last published week %s | parts %s" % (
                gid, grp.get("as_of"), grp.get("usd_mn"), (grp.get("quality") or {}).get("status"), (grp.get("quality") or {}).get("missing"), rows[gid],
                {k: "%s %s" % (ser[k].get("source_status"), ser[k].get("reported_value")) for k in grp.get("series_ids") or []}))
        withheld = sorted(k for k, v in ser.items() if v.get("source_status") != "observed")
        r.log("series whose latest week the New York Fed did not publish: %d %s" % (len(withheld), withheld))
        rev = after.get("revisions") or {}
        r.log("values the source changed since the previous snapshot (2026-11-06): %s" % rev.get("known_changed_values"))
        corp = after.get("corporate") or {}
        r.log("corporate bonds %s: net %s (%s)" % (corp.get("as_of"), corp.get("net_bonds_b"), (corp.get("quality") or {}).get("status")))
        read = store.raw_reader(s3, LIVE)
        reproduced = False
        try:
            manifest = native.strict_json(read(after["replay"]["manifest_key"]))
            output = store.replay(manifest, read)
            reproduced = output == {k: v for k, v in after.items() if k != "replay"} and native.digest(output) == after["replay"]["output_sha256"]
            r.log("replay from the %d retained original responses: reproduced %s (output sha256 %s)" % (len(native.URLS), reproduced, after["replay"]["output_sha256"][:16]))
        except Exception as e:  # noqa: BLE001
            r.log("replay: %s %s" % (type(e).__name__, str(e)[:200]))
        repo = groups.get("treasury_repo") or {}
        ok = (after.get("version") == "4.1.0" and (after.get("as_of") or "") > (before.get("as_of") or "") and len(ser) == 70 and set(newest) == {after.get("as_of")}
              and (repo.get("quality") or {}).get("status") == "fresh" and t.get("repo_out_b") is not None and rows["treasury_reverse_repo"] is not None and reproduced)
        r.kv(version=after.get("version"), as_of_before=before.get("as_of"), as_of_after=after.get("as_of"), treasury_repo_out_b=t.get("repo_out_b"),
             reverse_repo_last_published="%s %s" % tuple(rows["treasury_reverse_repo"] or ("-", "-")), withheld_series=len(withheld), reproduced=reproduced, run_seconds=round(took))
        if ok:
            r.ok("published: the week of %s, reproduced from its original responses" % after.get("as_of"))
        else:
            r.fail("the published document is not as expected")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
