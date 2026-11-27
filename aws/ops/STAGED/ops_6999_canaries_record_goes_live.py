"""ops 6999 - the canaries' record goes live: one run each of the macro grid, the record engine and the war room, read back.

Khalid, 2026-11-27 11:06 ET: "next improve the engine : canaries ... test it against historical data to calibrate it".
Dry run ops 6998 was clean. This op:
  0. proves the three deployed packages are the new code (the package itself is read, not a timestamp);
  1. runs justhodl-canary-grid once, so the eight credit canaries read against their full window today;
  2. runs justhodl-warroom-weights once (the monthly study that 1 November's freeze swallowed, and the record);
  3. runs justhodl-canary-warroom once, so the board carries the calibrated reading today;
  4. reads everything back from the bucket and checks it.

Each run is a disclosed one-off outside the engine's own schedule (grid 15:30 UTC daily, weights the 1st of the month,
war room 20:50 UTC daily); none is repeated while its document is already newer than what it depends on, so the op is
safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6999_canaries_record_goes_live"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
GRID, WEIGHTS, WARROOM = "justhodl-canary-grid", "justhodl-warroom-weights", "justhodl-canary-warroom"
CREDIT = ["hy_credit_oas", "euro_hy_oas", "em_corp_oas", "em_hy_oas", "us_hy_ytw", "em_hy_ytw", "bbb_aaa"]
MARKS = {GRID: ("lambda_function.py", "def merge_older(rows, older, limit):"), WEIGHTS: ("record_io.py", 'OUT_KEY = "data/canary-record.json"'),
         WARROOM: ("lambda_function.py", 'VERSION = "2.0.0"')}
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***"), (re.compile(r"\b[0-9a-f]{32}\b"), "***")]


def clean(text):
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=900, connect_timeout=20, retries={"max_attempts": 0}))
    now = datetime.now(timezone.utc)
    failed, ran = [], []

    def head(key):
        try:
            return s3.head_object(Bucket=LIVE, Key=key)["LastModified"]
        except Exception:  # noqa: BLE001
            return None

    def kept(key):
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())

    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        def check(ok, text):
            r.log(("PASS  " if ok else "FAIL  ") + clean(text))
            if not ok:
                failed.append(text)

        def invoke(fn, payload):
            t0 = time.time()
            out = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            body = out["Payload"].read().decode("utf-8", "replace")
            ran.append("%s at %s UTC (%.0f s)" % (fn, datetime.now(timezone.utc).strftime("%H:%M"), time.time() - t0))
            say("    invoked %s with %s: %.0f s, status %s%s" % (fn, json.dumps(payload), time.time() - t0, out.get("StatusCode"), (", function error " + str(out.get("FunctionError"))) if out.get("FunctionError") else ""))
            try:
                return json.loads(body), out.get("FunctionError")
            except Exception:  # noqa: BLE001
                return {"raw": body[:300]}, out.get("FunctionError")

        r.heading("6999 - the canaries' record goes live, %s" % now.isoformat(timespec="minutes"))

        r.section("0. The deployed packages are the new code")
        deployed = {}
        for fn, (member, mark) in MARKS.items():
            try:
                info = lam.get_function(FunctionName=fn)
                deployed[fn] = datetime.fromisoformat(info["Configuration"]["LastModified"].replace("+0000", "+00:00"))
                with urllib.request.urlopen(info["Code"]["Location"], timeout=60) as resp:
                    z = zipfile.ZipFile(io.BytesIO(resp.read()))
                names = z.namelist()
                src = z.read(member).decode("utf-8", "replace") if member in names else ""
                same = None
                local = ROOT / "aws/lambdas" / fn / "source" / member
                if local.exists():
                    same = src == local.read_text(encoding="utf-8")
                check(mark in src and same is not False, "%s: its package holds %s with the new code (modified %s, %s MB / %s s, identical to the repository's file: %s)" % (
                    fn, member, info["Configuration"]["LastModified"][:19], info["Configuration"]["MemorySize"], info["Configuration"]["Timeout"], same))
                if fn in (WEIGHTS, WARROOM):
                    cr = z.read("canary_record.py") if "canary_record.py" in names else b""
                    check(cr == (ROOT / "aws/lambdas" / WEIGHTS / "source/canary_record.py").read_bytes(), "%s carries canary_record.py as in the repository" % fn)
            except Exception as e:  # noqa: BLE001
                check(False, "%s: its package could not be read (%s)" % (fn, type(e).__name__))
        if failed:
            r.fail("the new code is not deployed; nothing was run: " + "; ".join(failed)[:600])
            sys.exit(1)

        before = kept("data/canary-warroom.json")
        grid_before = dict((s.get("key"), s.get("stress")) for s in (kept("data/canary-grid.json").get("signals") or []))

        r.section("1. The macro grid, once")
        g_at = head("data/canary-grid.json")
        if g_at and g_at > deployed[GRID]:
            say("its document (%s) is already newer than its deploy (%s): not run again" % (g_at.isoformat(timespec="minutes"), deployed[GRID].isoformat(timespec="minutes")))
        else:
            out, err = invoke(GRID, {})
            say("    answer: %s" % json.dumps(out)[:300])
            check(not err, "the grid ran without a function error")
        grid = kept("data/canary-grid.json")
        sig = dict((s.get("key"), s) for s in grid.get("signals") or [])
        say("grid now: written %s, early-warning level %s (%s), %s of %s signals available; rates and credit %s" % (
            head("data/canary-grid.json").isoformat(timespec="minutes"), grid.get("early_warning_level"), grid.get("band"), grid.get("n_available"), grid.get("n_total"),
            json.dumps((grid.get("sub_grids") or {}).get("rates_credit"))))
        for k in CREDIT:
            say("    %-14s before %s -> now %s (z %s, as of %s)" % (k, grid_before.get(k), (sig.get(k) or {}).get("stress"), (sig.get(k) or {}).get("zscore"), (sig.get(k) or {}).get("as_of")))
        check(all((sig.get(k) or {}).get("available") for k in CREDIT), "all seven credit canaries are still available")
        check(len([s for s in grid.get("signals") or [] if s.get("available")]) >= 55, "the grid still has its signals (%s available)" % grid.get("n_available"))

        r.section("2. The record engine, once")
        rec_at = head("data/canary-record.json")
        g_at = head("data/canary-grid.json")
        if rec_at and rec_at > deployed[WEIGHTS] and rec_at > g_at:
            say("the record (%s) is already newer than the deploy and the grid's document: not run again" % rec_at.isoformat(timespec="minutes"))
        else:
            out, err = invoke(WEIGHTS, {})
            say("    answer: %s" % json.dumps(out)[:900])
            check(not err and out.get("ok") and (out.get("record") or {}).get("written"), "the record was published (version %s)" % out.get("version"))
            ps = out.get("proxy_study") or {}
            say("    the proxy study beside it: %s" % json.dumps(ps)[:400])
        rec = kept("data/canary-record.json")
        say("record: generated %s, cut %s, %s of %s canaries, verdicts %s, %d barometer months, built in %s s" % (
            rec.get("generated_at"), rec.get("cut_day"), rec.get("tested"), rec.get("defined"), json.dumps(rec.get("verdicts")), len(rec["barometer"]["months"]), rec.get("duration_s")))
        t3 = rec["barometer"]["turns"]["3"]
        lvl = rec["barometer"]["views"]["per_mechanism"]
        check(rec.get("document") == "canary-record.v1" and rec.get("tested", 0) >= 150 and len(rec["barometer"]["months"]) >= 600, "the record is whole")
        check(0.45 <= lvl["auc_early"] <= 0.56 and t3["earned"]["auc_early"] - 1.64 * t3["earned"]["auc_early_se"] > 0.5,
              "it says what the dry run said: level early %s, turn early %s +- %s" % (lvl["auc_early"], t3["earned"]["auc_early"], t3["earned"]["auc_early_se"]))
        lc = rec.get("live_check") or {}
        say("against the engines' own readings: %s" % json.dumps(lc)[:700])
        still = [k for k in CREDIT if "macro_grid:" + k in (lc.get("flagged") or [])]
        check(not still, "after the grid's run no credit canary is far from its rebuild (still flagged: %s)" % still)
        by = dict((c["id"], c) for c in rec["canaries"])
        for k in CREDIT:
            c = by["macro_grid:" + k]
            say("    %-14s rebuilt %s | engine's own %s | gap %s | %s, early score %s" % (k, c["last_stress"], c["live_stress"], c["gap_to_live"], c["verdict"], c["auc_early"]))
        try:
            wj = kept("data/warroom-weights.json")
            say("the proxy study's weights: generated %s, %s learned" % (wj.get("generated_at"), wj.get("n_learned")))
        except Exception as e:  # noqa: BLE001
            say("weights document not read: %s" % type(e).__name__)

        r.section("3. The war room, once")
        w_at, rec_at = head("data/canary-warroom.json"), head("data/canary-record.json")
        if before.get("version") == "2.0.0" and w_at and w_at > rec_at and w_at > deployed[WARROOM]:
            say("the board (%s) is already 2.0.0 and newer than the record: not run again" % w_at.isoformat(timespec="minutes"))
        else:
            out, err = invoke(WARROOM, {})
            say("    answer: %s" % json.dumps(out)[:400])
            check(not err and out.get("ok") and out.get("calibrated") is True, "the war room ran and calibrated")
        doc = kept("data/canary-warroom.json")
        missing = sorted(set(before) - set(doc))
        check(not missing, "every key the board published before is still there (missing %s); now also %s" % (missing, sorted(set(doc) - set(before))))
        check(doc.get("version") == "2.0.0" and bool(doc.get("calibrated")), "the board is 2.0.0 and carries the calibrated reading")
        cal = doc.get("calibrated") or {}
        check((cal.get("record") or {}).get("generated_at") == rec.get("generated_at"), "it read the record just published")
        L, T = cal.get("level") or {}, cal.get("turn") or {}
        say("headline: %s" % doc["master"]["headline"])
        say("existing views: per canary %s, per mechanism %s, earned %s, early-warning %s" % (doc["barometer"]["views"]["per_canary"]["score"], doc["barometer"]["views"]["per_mechanism"]["score"],
                                                                                           doc["barometer"]["views"]["earned"]["score"], doc["master"]["early_warning_0_100"]))
        say("LEVEL %s = %s percentile, %s; after it %s of %s months fell (%s), near a high %s of %s (%s)" % (
            L.get("score"), L.get("percentile"), L.get("band"), (L.get("after") or {}).get("fell"), (L.get("after") or {}).get("months"), (L.get("after") or {}).get("fall_share"),
            (L.get("after") or {}).get("early_fell"), (L.get("after") or {}).get("early_months"), (L.get("after") or {}).get("early_fall_share")))
        say("TURN %s against %s (%s days back, %s) = %s percentile, %s; after it %s of %s fell (%s), near a high %s of %s (%s); by mechanism %s; weights %s" % (
            T.get("change"), T.get("against_day"), T.get("days_back"), T.get("against_from"), T.get("percentile"), T.get("band"), (T.get("after") or {}).get("fell"), (T.get("after") or {}).get("months"),
            (T.get("after") or {}).get("fall_share"), (T.get("after") or {}).get("early_fell"), (T.get("after") or {}).get("early_months"), (T.get("after") or {}).get("early_fall_share"),
            json.dumps(T.get("by_mechanism")), json.dumps(T.get("weights"))))
        say("cell: %s | counted: %s" % (json.dumps(cal.get("cell")), json.dumps(cal.get("what_is_counted"))))
        n_rec = sum(1 for c in doc["all_canaries"] if (c.get("record") or {}).get("verdict") not in (None, "no record"))
        check(n_rec == cal.get("canaries") and n_rec >= 100 and all(c.get("record") for c in doc["all_canaries"]), "%d of %d canaries on the board carry a record, the rest say why not" % (n_rec, len(doc["all_canaries"])))
        hist = kept("data/warroom-history.json")
        check(isinstance(hist[-1].get("cal"), dict) and hist[-1]["cal"].get("l") == L.get("score"), "today's history entry keeps the calibrated reading (%s)" % json.dumps(hist[-1].get("cal"))[:200])
        text = json.dumps(doc)
        check("api_key" not in text and not re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", text), "the board carries no key and no address")

        r.kv(failed_checks=len(failed), one_off_runs="; ".join(ran) or "none this time", level="%s (%s pct, %s)" % (L.get("score"), L.get("percentile"), L.get("band")),
             turn="%s (%s pct, %s)" % (T.get("change"), T.get("percentile"), T.get("band")))
        if failed:
            r.fail("%d checks failed: %s" % (len(failed), "; ".join(failed)[:900]))
            sys.exit(1)
        r.ok("LIVE: the record is published, the grid reads its credit canaries against their full window, and the board carries the calibrated reading")


if __name__ == "__main__":
    main()
