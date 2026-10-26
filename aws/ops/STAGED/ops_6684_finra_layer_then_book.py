"""ops 6684 - the FINRA chain end to end, once: justhodl-short-interest (600 s budget, lane SI) -> its per-ticker consumer
layer data/short-interest-tickers.json (never published before: the layer needed a 120 s reserve the 360 s budget could
not leave) -> justhodl-bank-factors v1.0.6 (FINRA short interest joined beside Polygon's) -> three documents read back.

Each engine is invoked once asynchronously and followed through its REPORT line; the probe then reads the tickers artifact
(HEAD, contract, n_tickers, settlement_date), re-runs the book join with force=True, and reads the manifest's
coverage.finra_short / sources.finra_short and the derived finra_* fields of AAPL, GME, TSLA.  Writes nothing except what
the engines write.  No key involved.
"""
from pathlib import Path
import gzip
import json
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
BOOK = "data/warm/factor-book/"
TICKERS = "data/short-interest-tickers.json"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=60)
WAIT_SI = int(os.environ.get("WAIT_SI") or "660")
WAIT_BOOK = int(os.environ.get("WAIT_BOOK") or "780")


def jget(s3, key):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        try:
            raw = gzip.decompress(raw)
        except Exception:  # noqa: BLE001
            pass
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {"_error": type(e).__name__}


def follow(logs, lam, fn, payload, wait_s, r):
    since_ms = int(time.time() * 1000) - 5000
    resp = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=json.dumps(payload).encode())
    rid = resp.get("ResponseMetadata", {}).get("RequestId")
    r.log("%s: accepted %s (request %s)" % (fn, resp.get("StatusCode"), rid))
    t0, line = time.time(), None
    while time.time() - t0 < wait_s and not line:
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, filterPattern='"REPORT RequestId: %s"' % rid, limit=3).get("events", [])
        except Exception:  # noqa: BLE001
            ev = []
        if ev:
            m = ev[-1].get("message", "")
            line = " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:", "Init Duration")))
        else:
            time.sleep(20)
    r.log("%s: REPORT %s" % (fn, line or "(not within %d s)" % wait_s))
    try:
        ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, limit=40,
                                    filterPattern='?"[tickers-layer]" ?"finra" ?Traceback ?"[ERROR]" ?"Task timed out" ?"[bank-factors] float"').get("events", [])
        for e in ev[-10:]:
            r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:300]))
    except Exception as e:  # noqa: BLE001
        r.warn("   logs: %s" % type(e).__name__)
    return line


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6684_finra_layer_then_book") as r:
        r.heading("ops 6684 - FINRA chain: short-interest layer -> factor book v1.0.6 (one run each)")
        c = lam.get_function_configuration(FunctionName="justhodl-short-interest")
        r.log("justhodl-short-interest deployed: %s MB / %s s, modified %s" % (c.get("MemorySize"), c.get("Timeout"), (c.get("LastModified") or "")[:19]))
        if int(c.get("Timeout") or 0) < 600:
            r.warn("the 600 s budget is not live yet (lane SI pending) - the layer will skip again; stopping here")
            raise SystemExit(0)
        r.section("1. justhodl-short-interest")
        follow(logs, lam, "justhodl-short-interest", {}, WAIT_SI, r)
        try:
            h = s3.head_object(Bucket=LIVE, Key=TICKERS)
            doc = jget(s3, TICKERS)
            r.log("%s: %s bytes, modified %s | contract %s | n_tickers %s | settlement %s | generated %s" % (
                TICKERS, h.get("ContentLength"), h.get("LastModified").isoformat()[:19] if h.get("LastModified") else None, doc.get("contract"),
                (doc.get("meta") or {}).get("n_tickers"), doc.get("settlement_date"), doc.get("generated_at")))
            by = doc.get("by_ticker") or {}
            for sym in ("AAPL", "GME", "TSLA"):
                if sym in by:
                    r.log("   %s -> %s" % (sym, json.dumps({k: by[sym].get(k) for k in ("short_interest", "change_pct", "days_to_cover", "dtc_effective", "settlement_date", "latest")})))
        except Exception as e:  # noqa: BLE001
            r.warn("%s: %s - the layer did not publish; the book join below will again find 0 FINRA tickers" % (TICKERS, type(e).__name__))
        r.section("2. justhodl-bank-factors v1.0.6 join")
        follow(logs, lam, "justhodl-bank-factors", {"force": True}, WAIT_BOOK, r)
        man = jget(s3, BOOK + "manifest.json")
        r.log("manifest: version %s updated %s | coverage.finra_short %s (polygon short %s) | sources.finra_short %s | as_of.finra-short-interest %s" % (
            man.get("version"), man.get("updated_at"), (man.get("coverage") or {}).get("finra_short"), (man.get("coverage") or {}).get("short"),
            json.dumps((man.get("sources") or {}).get("finra_short")), (man.get("as_of") or {}).get("finra-short-interest")))
        for sym in ("AAPL", "GME", "TSLA", "SOFI"):
            d = (jget(s3, BOOK + "symbols/%s.json.gz" % sym)).get("derived") or {}
            r.log("%s: short_source %s short_pct_float %s | finra SI %s dtc %s change%% %s vs-polygon%% %s settlement %s" % (
                sym, d.get("short_source"), d.get("short_pct_float"), d.get("finra_short_interest"), d.get("finra_days_to_cover"),
                d.get("finra_si_change_pct"), d.get("finra_vs_polygon_si_pct"), d.get("finra_settlement_date")))
        r.ok("ops 6684 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
