"""ops 6666 - read-only: why did the Polygon EOD bank answer nothing in the wave-2 first light?

pre-pump-detector read 119 histories, all from FMP (`history sources {"fmp": 119}`), smart-beta 141 x "fmp live", although
both ask the Polygon EOD bank first (aws/shared/polygon_bankfirst.answer / the eod files directly).  This probe runs the
SAME readers on the runner for a handful of tickers and prints every reason: the eod file's first / last session and row
count, the eod manifest (updated_at, gaps, sessions), polygon_bankfirst.answer() for a 210-day window ending today plus
STATS and the control state, bars()/closes() (complete_only on and off), and the walker bank's price file last date for
the same names.  Nothing is written.  No key involved.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
TICKERS = ["AAPL", "MSFT", "GME", "SOFI", "PLTR", "NVDA", "F", "CCL"]


def jgz(s3, key):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        try:
            raw = gzip.decompress(raw)
        except Exception:  # noqa: BLE001
            pass
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {"_error": type(e).__name__}


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    import polygon_bankfirst as pb
    import fmp_bank
    with report("ops_6666_polygon_eod_bank_probe") as r:
        r.heading("ops 6666 - Polygon EOD bank: why no answers in the wave-2 light (read-only)")
        now = datetime.now(timezone.utc)
        r.log("now %s; polygon_bankfirst last completed session = %s; in session window = %s" % (now.isoformat(), pb._last_completed_session(now), pb._in_session_window(now)))
        r.section("1. the eod manifest")
        man = jgz(s3, pb.MANIFEST)
        r.log("manifest key %s -> %s" % (pb.MANIFEST, json.dumps({k: (v if not isinstance(v, list) else (len(v), v[-5:])) for k, v in (man or {}).items()}, default=str)[:900]))
        r.section("2. per-ticker eod files + readers")
        for t in TICKERS:
            key = pb.ROOT + t + ".json.gz"
            doc = jgz(s3, key)
            if not isinstance(doc, dict) or "_error" in doc:
                r.warn("%s: eod file %s -> %s" % (t, key, doc))
                continue
            rows = doc.get("rows") or []
            last_ms = rows[-1][0] if rows and isinstance(rows[-1], list) else None
            r.log("%s: eod file rows %d first %s last %s (last row date %s) fields %s" % (t, len(rows), doc.get("first"), doc.get("last"), pb._row_date(last_ms).isoformat() if last_ms else None, doc.get("fields")))
            end = now.date()
            start = end - timedelta(days=int(210 * 1.55) + 10)
            for k in pb.STATS:
                pb.STATS[k] = 0
            pb._reset_ctl()
            url = "https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=asc&limit=5000" % (t, start.isoformat(), end.isoformat())
            t0 = time.time()
            ans = pb.answer(url)
            n = len((json.loads(ans) or {}).get("results") or []) if ans else 0
            r.log("   answer(range %s..%s) -> %s rows in %.2fs | STATS %s | ctl off=%s reason=%s" % (start, end, n, time.time() - t0, json.dumps(pb.STATS), pb._ctl.get("off"), pb._ctl.get("reason")))
            url2 = "https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=asc&limit=5000" % (t, start.isoformat(), pb._last_completed_session(now).isoformat())
            ans2 = pb.answer(url2)
            r.log("   answer(range ..last completed session) -> %s rows | STATS %s" % (len((json.loads(ans2) or {}).get("results") or []) if ans2 else 0, json.dumps(pb.STATS)))
            b1 = pb.bars(t, sessions=5)
            b2 = pb.bars(t, sessions=5, complete_only=False)
            r.log("   bars(complete_only) -> %d; bars(any) -> %d, last %s" % (len(b1), len(b2), (b2[-1]["date"], b2[-1]["c"]) if b2 else None))
            wp = fmp_bank.walker("historical-price-eod-full", t)
            if isinstance(wp, dict):
                hist = wp.get("historical") or wp.get("rows") or []
            else:
                hist = wp or []
            last = None
            try:
                last = max((x.get("date") for x in hist if isinstance(x, dict) and x.get("date")), default=None)
            except Exception:  # noqa: BLE001
                pass
            r.log("   walker historical-price-eod-full: %s rows, last %s" % (len(hist) if hist else 0, last))
        r.section("3. session calendar check")
        r.log("eod bank BANK_FIRST_SESSION %s; today weekday %d" % (pb.BANK_FIRST_SESSION, now.weekday()))
        r.ok("ops 6666 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
