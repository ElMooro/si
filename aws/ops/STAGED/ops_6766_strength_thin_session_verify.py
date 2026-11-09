"""ops 6766 - verify justhodl-strength 1.0.12 (ops 6765 finding: the bank's manifest named Saturday 2026-11-07 the last
session, so the 03:40 runs of Nov 8 and Nov 9 ranked 18 of 852 names) and republish today's desk.

  G0  holds (CAP / ANOM lists, stage) and the three-rung deploy ladder: the artifact zip carries VERSION 1.0.12 and the
      marker ('effective_session'), State Active, LastUpdateStatus Successful;
  1.  the desk as published now (header) and the Friday artifact the Saturday 03:40 run kept
      (data/strength/history/2026-11-06.json.gz: ranked count, top ten) - read BEFORE the invoke, the engine rewrites
      its session's history key on every run;
  2.  invoke justhodl-strength (Event) and poll data/strength.json until it is younger than this run: version, as_of,
      bank.manifest_last_session / thin_days, rows, ranked, stale rows, the rows' own as-of dates, benchmark as-of,
      top ten against the Friday artifact;
  3.  the served copy: https://justhodl.ai/data/strength.json (cache-busted) carries the same version / as_of / ranked.
The engine is not held (checked first; held -> stop). Releases nothing; writes nothing but this report."""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
GOVERNOR = "justhodl-spend-governor"
ENGINE = "justhodl-strength"
OUT = "data/strength.json"
FRIDAY = "2026-11-06"
FRIDAY_ARTIFACT = "data/strength/history/%s.json.gz" % FRIDAY
WANT_VERSION = "1.0.12"
MARKER = "effective_session"
EDGE = "https://justhodl.ai/data/strength.json"
MIN_RANKED = 500


def ranked_rows(doc):
    return [r for r in doc.get("rows") or [] if r.get("strength") is not None and not r.get("stale")]


def top(doc, n=10):
    return [r.get("ticker") for r in sorted(ranked_rows(doc), key=lambda r: -r["strength"])[:n]]


def header(doc):
    uni, bank = doc.get("universe") or {}, doc.get("bank") or {}
    return {"version": doc.get("version"), "generated_at": doc.get("generated_at"), "as_of": doc.get("as_of"),
            "rows": len(doc.get("rows") or []), "ranked": uni.get("n_ranked"), "bank_last_session": bank.get("last_session"),
            "manifest_last_session": bank.get("manifest_last_session"), "thin_days": bank.get("thin_days"),
            "benchmark_as_of": (doc.get("benchmark") or {}).get("as_of"), "errors": doc.get("n_errors")}


def is_session_day(day):
    try:
        return datetime.strptime(str(day), "%Y-%m-%d").weekday() < 5
    except ValueError:
        return False


def selftest():
    doc = {"version": "1.0.12", "as_of": "2026-11-06", "universe": {"n_ranked": 2}, "bank": {"last_session": "2026-11-06", "manifest_last_session": "2026-11-07",
                                                                                      "thin_days": [{"day": "2026-11-07", "tickers": 18, "of": 852, "benchmark": False}]},
           "rows": [{"ticker": "A", "strength": 90.0, "stale": False}, {"ticker": "B", "strength": 99.0, "stale": False}, {"ticker": "C", "strength": 99.5, "stale": True},
                    {"ticker": "D", "strength": None, "stale": False}]}
    assert top(doc) == ["B", "A"] and len(ranked_rows(doc)) == 2
    h = header(doc)
    assert h["ranked"] == 2 and h["rows"] == 4 and h["manifest_last_session"] == "2026-11-07" and h["thin_days"][0]["tickers"] == 18
    assert is_session_day("2026-11-06") and not is_session_day("2026-11-07") and not is_session_day("2026-11-08") and not is_session_day(None)
    print("selftest ok")


def ready(lam, fn, r, wait_s=240):
    t0 = time.time()
    while True:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            return c
        if time.time() - t0 > wait_s:
            r.fail("%s not ready: State %s LastUpdateStatus %s" % (fn, c.get("State"), c.get("LastUpdateStatus")))
            sys.exit(1)
        time.sleep(10)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6766_strength_thin_session_verify") as r:
        r.heading("ops 6766 - justhodl-strength 1.0.12 verify + republish today's desk (%s)" % now.date())

        r.section("G0. holds and the three-rung ladder")
        st = {}
        for attempt in range(4):
            try:
                st = json.loads(lam.invoke(FunctionName=GOVERNOR, Payload=b'{"action":"status"}')["Payload"].read() or b"{}")
                break
            except lam.exceptions.TooManyRequestsException:
                time.sleep(30)
        held = set(st.get("capped") or []) | {fn for fn, _ in (st.get("anomaly_held") or [])}
        r.log("governor stage %s | capped %s | anomaly %s" % (st.get("stage"), st.get("capped"), st.get("anomaly_held")))
        if not st or ENGINE in held or st.get("stage") != "NORMAL":
            r.fail("the governor did not answer, the engine is held or the stage is not NORMAL - nothing invoked")
            sys.exit(1)
        c = ready(lam, ENGINE, r)
        r.log("%s: State %s / %s, last modified %s, timeout %ss" % (ENGINE, c["State"], c["LastUpdateStatus"], c["LastModified"], c["Timeout"]))
        code = lam.get_function(FunctionName=ENGINE)["Code"]["Location"]
        blob = urllib.request.urlopen(code, timeout=120).read()
        src = zipfile.ZipFile(io.BytesIO(blob)).read("lambda_function.py").decode("utf-8", "replace")
        ver = re.search(r'^VERSION = "([^"]+)"', src, re.M)
        r.log("artifact: %d B, lambda_function.py VERSION %s, marker '%s' %s" % (len(blob), ver.group(1) if ver else "?", MARKER, "present" if MARKER in src else "ABSENT"))
        facts["artifact_version"] = ver.group(1) if ver else None
        if MARKER not in src or not ver or ver.group(1) != WANT_VERSION:
            r.fail("the deployed artifact is not %s - deploy-lambdas has not landed; re-run this op after it does" % WANT_VERSION)
            sys.exit(1)

        r.section("1. before: the published desk and the Friday artifact (kept by the Saturday 03:40 run)")
        obj = s3.get_object(Bucket=LIVE, Key=OUT)
        before_mod = obj["LastModified"]
        doc0 = json.loads(obj["Body"].read() or b"{}")
        r.log("published (last modified %s): %s" % (before_mod.isoformat(timespec="seconds"), json.dumps(header(doc0), default=str)))
        facts.update(before_as_of=doc0.get("as_of"), before_ranked=(doc0.get("universe") or {}).get("n_ranked"))
        fri = {}
        try:
            fobj = s3.get_object(Bucket=LIVE, Key=FRIDAY_ARTIFACT)
            fri = json.loads(gzip.decompress(fobj["Body"].read()))
            r.log("Friday artifact %s (last modified %s): %s" % (FRIDAY_ARTIFACT, fobj["LastModified"].isoformat(timespec="seconds"), json.dumps(header(fri), default=str)))
            r.log("Friday artifact top ten: %s" % ", ".join(top(fri)))
            facts["friday_ranked"] = (fri.get("universe") or {}).get("n_ranked")
        except Exception as exc:  # noqa: BLE001 - the comparison is a bonus, the gate is the ranked count
            r.warn("Friday artifact unreadable: %s: %s" % (type(exc).__name__, str(exc)[:140]))

        r.section("2. invoke (Event) and poll data/strength.json")
        res = lam.invoke(FunctionName=ENGINE, InvocationType="Event", Payload=json.dumps({"trigger": "ops-6766"}).encode())
        r.log("invoke status %s" % res.get("StatusCode"))
        t_inv, doc = time.time(), None
        while time.time() - t_inv < 420:
            time.sleep(15)
            h = s3.head_object(Bucket=LIVE, Key=OUT)
            if h["LastModified"] > before_mod:
                doc = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
                break
        if not doc:
            r.fail("data/strength.json did not refresh within 7 min (the engine raised or timed out; read its log)")
            sys.exit(1)
        hd = header(doc)
        r.log("after (%.0f s): %s" % (time.time() - t_inv, json.dumps(hd, default=str)))
        rows = doc.get("rows") or []
        n_stale = sum(1 for x in rows if x.get("stale"))
        dates = Counter(str(x.get("as_of")) for x in rows)
        r.log("rows %d | ranked %s | stale %d | rows' own as-of: %s | trigger %s" % (
            len(rows), hd["ranked"], n_stale, json.dumps(dates.most_common(5)), json.dumps(doc.get("trigger"))))
        t_new = top(doc)
        r.log("top ten now: %s" % ", ".join(t_new))
        if fri:
            t_old = top(fri)
            r.log("top ten shared with the Friday artifact: %d of 10 | ranked now %s vs Friday %s" % (
                len(set(t_new) & set(t_old)), hd["ranked"], (fri.get("universe") or {}).get("n_ranked")))
            facts["top10_shared_with_friday"] = len(set(t_new) & set(t_old))
        facts.update(version=hd["version"], as_of=hd["as_of"], ranked=hd["ranked"], rows=len(rows), stale=n_stale,
                     manifest_last_session=hd["manifest_last_session"], thin_days=json.dumps(hd["thin_days"]), benchmark_as_of=hd["benchmark_as_of"],
                     errors=hd["errors"])
        if hd["version"] != WANT_VERSION:
            r.fail("the published desk is version %s, not %s" % (hd["version"], WANT_VERSION))
            sys.exit(1)
        if not is_session_day(hd["as_of"]):
            r.fail("the desk is still as of %s - not a session day" % hd["as_of"])
            sys.exit(1)
        if not isinstance(hd["ranked"], int) or hd["ranked"] < MIN_RANKED:
            r.fail("only %s names ranked (Sunday and Monday 03:40: 18; a whole desk ranks ~800)" % hd["ranked"])
            sys.exit(1)
        if hd["as_of"] != FRIDAY:
            r.warn("as of %s (expected %s - a newer complete session is fine)" % (hd["as_of"], FRIDAY))

        r.section("3. the served copy (%s)" % EDGE)
        try:
            served = None
            for attempt in range(4):
                req = urllib.request.Request("%s?v=%d%d" % (EDGE, int(time.time()), attempt), headers={
                    "Cache-Control": "no-cache", "Pragma": "no-cache", "User-Agent": "justhodl-ops/6766"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                    if resp.headers.get("Content-Encoding") == "gzip":
                        raw = gzip.decompress(raw)
                served = json.loads(raw)
                if served.get("generated_at") == doc.get("generated_at"):
                    break
                time.sleep(20)
            sh = header(served)
            r.log("served: %s" % json.dumps(sh, default=str))
            facts.update(served_version=sh["version"], served_as_of=sh["as_of"], served_ranked=sh["ranked"])
            if served.get("generated_at") != doc.get("generated_at"):
                r.warn("the edge still serves generated_at %s (bucket %s) - the object's max-age is 300 s" % (served.get("generated_at"), doc.get("generated_at")))
        except Exception as exc:  # noqa: BLE001 - the bucket read above is the gate; the edge read is the proof a visitor sees it
            r.warn("served copy unreadable from the runner: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("strength 1.0.12 live; the desk stands on %s with %s names ranked" % (hd["as_of"], hd["ranked"]))
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
