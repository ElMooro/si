"""ops 6758 - justhodl-strength 1.0.7 first light (re-runnable): invoke the deployed engine once and read the new blocks back -
asset classes (counts / ranked / the leveraged funds kept out), boards.by_class, the thesis walk-forward (window, dump
days, the base rate, last_session), the history snapshot key. No bank gate: whatever session the bank holds is the
session tested (before the 03:10 UTC fold that is 2026-11-04; ops 6757 is the 2026-11-05 dump-day test proper).
Invokes one engine (a reader). Writes nothing but the engine's own artifacts and this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE, FN, OUT = "us-east-1", "justhodl-dashboard-live", "justhodl-strength", "data/strength.json"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=330)


def pct(v):
    return "-" if v is None else "%+.2f%%" % v


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6758_strength_107_first_light") as r:
        r.heading("ops 6758 - strength 1.0.7 first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        cf = lam.get_function_configuration(FunctionName=FN)
        r.log("%s: modified %s, sha %s, MAX_UNIVERSE %s" % (FN, (cf.get("LastModified") or "")[:19], (cf.get("CodeSha256") or "")[:12], ((cf.get("Environment") or {}).get("Variables") or {}).get("MAX_UNIVERSE")))
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6758_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        r.log("invoke: status %s error %s %.0fs payload %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(payload)[:400]))
        if resp.get("FunctionError"):
            r.fail("the engine raised: %s" % json.dumps(payload)[:900])
            raise SystemExit(1)
        obj = s3.get_object(Bucket=LIVE, Key=OUT)
        doc = json.loads(obj["Body"].read())
        r.log("%s: %d bytes | version %s | as_of %s | rows %d ranked %s | run %ss | errors %s %s" % (OUT, obj["ContentLength"], doc.get("version"), doc.get("as_of"), len(doc.get("rows") or []),
              (doc.get("boards") or {}).get("n_ranked"), doc.get("run_seconds"), doc.get("n_errors"), json.dumps((doc.get("errors") or [])[:5])[:400]))
        if doc.get("version") != "1.0.7":
            r.fail("live artifact is %s, not 1.0.7" % doc.get("version"))
            raise SystemExit(1)
        u = doc.get("universe") or {}
        r.log("universe: %s | unbanked %s | skipped %s" % (json.dumps({k: u.get(k) for k in ("n", "n_rows", "n_ranked", "scopes", "max")}), u.get("unbanked"), u.get("skipped_for_time")))
        cls = doc.get("classes") or {}
        r.log("classes counted %s" % json.dumps(cls.get("counts")))
        r.log("classes ranked  %s" % json.dumps(cls.get("ranked")))
        unclassed = [x["ticker"] for x in (doc.get("rows") or []) if not x.get("asset_class")]
        r.log("rows without a class: %d %s" % (len(unclassed), unclassed[:40]))
        b = doc.get("boards") or {}
        ex = (b.get("excluded") or {}).get("leveraged") or []
        r.log("leveraged kept out (%d): %s" % (len(ex), ", ".join("%s %s" % (x["ticker"], pct(x.get("h_d"))) for x in ex)))
        r.log("leaders: %s" % ", ".join("%s %s %s" % (x["ticker"], x["strength"], x.get("asset_class")) for x in (b.get("leaders") or [])[:25]))
        r.log("laggards: %s" % ", ".join("%s %s %s" % (x["ticker"], x["strength"], x.get("asset_class")) for x in (b.get("laggards") or [])[:25]))
        for c in b.get("class_order") or []:
            x = (b.get("by_class") or {}).get(c) or {}
            r.log("  %-20s n %4s median rank %5s | vs SPY today %7s 1W %7s 1M %7s 3M %7s | beat SPY 1M %s%% 3M %s%% | best %s | worst %s" % (
                x.get("label"), x.get("n"), x.get("median_strength"), pct(x.get("median_vs_d")), pct(x.get("median_vs_w")), pct(x.get("median_vs_m")), pct(x.get("median_vs_q")), x.get("pct_above_spy_m"), x.get("pct_above_spy_q"),
                " ".join(y["ticker"] for y in x.get("best") or []), " ".join(y["ticker"] for y in x.get("worst") or [])))
        # a few named checks of the classification on real rows
        by = {x["ticker"]: x for x in doc.get("rows") or []}
        for t in ("HYG", "JNK", "TLT", "LQD", "GLD", "SLV", "SIVR", "PPLT", "CPER", "GDX", "USO", "UUP", "IBIT", "VXX", "EWJ", "XLE", "QQQ", "AAPL", "TQQQ", "SQQQ", "SOXS", "BIL"):
            x = by.get(t)
            r.log("  %-5s %s/%s strength %s in-class %s sleeve %s excluded %s" % (t, (x or {}).get("asset_class"), (x or {}).get("class_sub"), (x or {}).get("strength"), (x or {}).get("strength_in_class"), (x or {}).get("sleeve"), (x or {}).get("excluded")) if x else "  %-5s not in the universe" % t)
        th = doc.get("thesis") or {}
        sm = th.get("summary") or {}
        r.log("thesis: window %s | %ss | error %s" % (json.dumps(th.get("window")), th.get("seconds"), th.get("error")))
        r.log("VERDICT: %s" % th.get("verdict"))
        r.log("dump days %s deep %s | all: %s" % (sm.get("n_dump_days"), sm.get("n_deep_dump_days"), json.dumps((sm.get("dump") or {}).get("all"))[:900]))
        r.log("stocks: %s" % json.dumps((sm.get("dump") or {}).get("stocks"))[:700])
        r.log("by class on dump days: %s" % json.dumps((sm.get("dump") or {}).get("by_class"))[:1200])
        r.log("all days: %s" % json.dumps(sm.get("all_days")))
        ls = th.get("last_session") or {}
        r.log("last session %s (ranked as of %s) SPY %s dump %s | top %s | bottom %s" % (ls.get("date"), ls.get("ranked_as_of"), pct(ls.get("spy_ret_pct")), ls.get("dump"), json.dumps((ls.get("all") or {}).get("top")), json.dumps((ls.get("all") or {}).get("bottom"))))
        for d in (th.get("dump_days") or [])[:30]:
            a = d["all"]["top"]
            r.log("  %s SPY %s | top excess %s pp held %s%% beat %s%% | bottom excess %s held %s%% | stocks top excess %s" % (d["date"], pct(d["spy_ret_pct"]), a.get("mean_excess_pp"), a.get("held_up_pct"), a.get("hit_pct"),
                  d["all"]["bottom"].get("mean_excess_pp"), d["all"]["bottom"].get("held_up_pct"), ((d.get("stocks") or {}).get("top") or {}).get("mean_excess_pp")))
        hk = "data/strength/history/%s.json.gz" % doc.get("as_of")
        try:
            h = s3.head_object(Bucket=LIVE, Key=hk)
            r.log("history %s: %s bytes, modified %s" % (hk, h["ContentLength"], h["LastModified"].isoformat(timespec="seconds")))
        except Exception as e:  # noqa: BLE001
            r.warn("history %s missing: %s" % (hk, type(e).__name__))
        r.kv(version=str(doc.get("version")), as_of=str(doc.get("as_of")), rows=str(len(doc.get("rows") or [])), ranked=str((doc.get("boards") or {}).get("n_ranked")), dump_days=str(sm.get("n_dump_days")), unclassed=str(len(unclassed)), bytes=str(obj["ContentLength"]))
        r.ok("ops 6758 complete: strength 1.0.7 live as of %s" % doc.get("as_of"))


if __name__ == "__main__":
    main()
    sys.exit(0)
