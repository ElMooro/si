"""ops 6682 - read-only: why did the factor book's FINRA join (v1.0.6, ops 6681) find 0 tickers?

Reads data/short-interest-tickers.json the way the book does (HEAD, size, encoding, first bytes, the contract / schema /
measurement_contract fields, by_ticker count and three sample rows), the book manifest's notes (the loader writes one when
the artifact is unavailable or off-contract), and justhodl-short-interest's last 48 h of REPORT / tickers-layer lines (does
the layer publish at 07:15 UTC?).  Nothing is written.  No key involved.
"""
from pathlib import Path
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
KEY = "data/short-interest-tickers.json"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=60)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6682_finra_tickers_probe") as r:
        r.heading("ops 6682 - the FINRA tickers artifact as the factor book reads it (read-only)")
        try:
            h = s3.head_object(Bucket=LIVE, Key=KEY)
            r.log("HEAD %s: %s bytes, ContentType %s, ContentEncoding %s, LastModified %s, CacheControl %s" % (
                KEY, h.get("ContentLength"), h.get("ContentType"), h.get("ContentEncoding"), (h.get("LastModified") or "").isoformat()[:19] if h.get("LastModified") else None, h.get("CacheControl")))
            raw = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read()
            r.log("first 160 bytes: %r" % raw[:160])
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
                r.log("gzip member -> %d bytes" % len(raw))
            doc = json.loads(raw.decode("utf-8", "replace"))
            r.log("top-level keys: %s" % sorted(doc.keys())[:40])
            r.log("contract %r | schema_version %r | measurement_contract %r | settlement_date %r | generated_at %r" % (
                doc.get("contract"), doc.get("schema_version"), doc.get("measurement_contract"), doc.get("settlement_date"), doc.get("generated_at")))
            by = doc.get("by_ticker")
            r.log("by_ticker: %s with %d entries" % (type(by).__name__, len(by) if isinstance(by, dict) else -1))
            if isinstance(by, dict):
                for sym in list(by)[:3] + [s for s in ("AAPL", "GME", "TSLA") if s in by]:
                    r.log("   %s -> %s" % (sym, json.dumps(by[sym], default=str)[:400]))
            meta = doc.get("meta") or {}
            r.log("meta: %s" % json.dumps({k: meta.get(k) for k in ("n_tickers", "n_latest_settlement", "settlement_date", "generated_at")}))
        except Exception as e:  # noqa: BLE001
            r.warn("%s: %s %s" % (KEY, type(e).__name__, str(e)[:200]))
        r.section("the book manifest's notes")
        try:
            man = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key="data/warm/factor-book/manifest.json")["Body"].read()))
        except Exception:  # noqa: BLE001
            try:
                man = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/factor-book/manifest.json")["Body"].read())
            except Exception as e:  # noqa: BLE001
                man = {"_error": type(e).__name__}
        r.log("manifest keys: %s" % sorted(man.keys())[:30])
        for n in (man.get("notes") or [])[:12]:
            r.log("   note: %s" % str(n)[:300])
        r.log("sources.finra_short %s | coverage.finra_short %s | version %s" % ((man.get("sources") or {}).get("finra_short"), (man.get("coverage") or {}).get("finra_short"), man.get("version")))
        r.section("justhodl-short-interest: 48 h of REPORT and tickers-layer lines")
        since = int((time.time() - 48 * 3600) * 1000)
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-short-interest", startTime=since, limit=60,
                                        filterPattern='?"REPORT RequestId" ?"[tickers-layer]" ?Traceback ?"[ERROR]"').get("events", [])
            for e in ev[-20:]:
                m = e.get("message", "").strip().replace("\n", " ⏎ ")
                if "REPORT RequestId" in m:
                    m = " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:")))
                r.log("   %s | %s" % (time.strftime("%m-%d %H:%M:%S", time.gmtime(e["timestamp"] / 1000)), m[:300]))
        except Exception as e:  # noqa: BLE001
            r.warn("logs: %s" % type(e).__name__)
        r.ok("ops 6682 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
