"""ops 6657 - read-only: is the FMP key being rate-limited right now, and how hard?

ops 6656 (01:38-01:50 UTC, 2026-10-26): the walker's dedicated price-lane run at 18 req/s got 756 answers, ALL http 429 -
after the 23:27 run at 28 req/s had drawn 503 x 429.  Either the key is inside a lockout window, a quota the plan does
not advertise is spent, or the limit is far below the 3,000/min the plan says.  This op makes a handful of spaced
/stable calls with the fleet's key (SSM /justhodl/fmp/api-key, never printed), records each HTTP status and every
rate-limit-looking response header, then waits 65 s and tries again, then probes a slow steady cadence (12 calls at
1 call/s).  Also reads the walker's and fmp-bank's last-run http verdicts and the schedule-manifest to list every FMP
caller firing in the 22:00-02:00 UTC window.  Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
HEAD_KEYS = ("x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset", "retry-after", "x-rate-limit-limit", "x-rate-limit-remaining", "ratelimit-limit", "ratelimit-remaining", "ratelimit-reset", "cf-ray", "server", "date")


def probe(key, path, params):
    q = dict(params)
    q["apikey"] = key
    url = "https://financialmodelingprep.com/stable/%s?%s" % (path, urllib.parse.urlencode(q))
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "JustHodl ops probe"}), timeout=30) as r:
            body = r.read(4000)
            hdr = {k.lower(): v for k, v in r.headers.items()}
            n = None
            try:
                d = json.loads(body)
                n = len(d) if isinstance(d, list) else len(d.keys())
            except Exception:  # noqa: BLE001
                pass
            return {"status": r.status, "ms": int((time.time() - t0) * 1000), "rows": n, "headers": {k: hdr[k] for k in HEAD_KEYS if k in hdr}}
    except urllib.error.HTTPError as e:
        hdr = {k.lower(): v for k, v in (e.headers.items() if e.headers else [])}
        body = b""
        try:
            body = e.read(600)
        except Exception:  # noqa: BLE001
            pass
        return {"status": e.code, "ms": int((time.time() - t0) * 1000), "headers": {k: hdr[k] for k in HEAD_KEYS if k in hdr}, "body": body.decode("utf-8", "replace").replace(key, "<key>")[:300]}
    except Exception as e:  # noqa: BLE001
        return {"status": "err:%s" % type(e).__name__, "ms": int((time.time() - t0) * 1000)}


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    ssm = boto3.client("ssm", region_name=REGION, config=CFG)
    key = ssm.get_parameter(Name="/justhodl/fmp/api-key", WithDecryption=True)["Parameter"]["Value"]
    with report("ops_6657_fmp_key_health_probe") as r:
        r.heading("ops 6657 - FMP key health: are we rate-limited right now? (read-only)")
        r.log("key: SSM /justhodl/fmp/api-key, %d chars, sha256 prefix %s (value never printed)" % (len(key), __import__("hashlib").sha256(key.encode()).hexdigest()[:8]))
        r.section("1. three spaced calls now")
        for path, params in (("profile", {"symbol": "AAPL"}), ("quote", {"symbol": "AAPL"}), ("historical-price-eod/full", {"symbol": "AAPL", "from": "2026-10-01"})):
            res = probe(key, path, params)
            r.log("%s %s -> %s" % (datetime.now(timezone.utc).strftime("%H:%M:%S"), path, json.dumps(res)[:400]))
            time.sleep(3)
        r.section("2. after 65 s (a per-minute window must have rolled)")
        time.sleep(65)
        res = probe(key, "profile", {"symbol": "MSFT"})
        r.log("%s profile MSFT -> %s" % (datetime.now(timezone.utc).strftime("%H:%M:%S"), json.dumps(res)[:400]))
        r.section("3. slow steady cadence: 12 calls at 1 call/s")
        codes = []
        for sym in ("NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM", "UNH", "V", "PG", "HD", "KO"):
            res = probe(key, "profile", {"symbol": sym})
            codes.append(res.get("status"))
            time.sleep(1)
        r.log("statuses: %s" % codes)
        r.kv(step="fmp key health", ok=sum(1 for c in codes if c == 200), http429=sum(1 for c in codes if c == 429), other=sum(1 for c in codes if c not in (200, 429)))
        r.section("4. who else calls FMP in the 22:00-02:00 UTC window (schedule manifest, by expression)")
        man = json.loads(s3.get_object(Bucket=LIVE, Key="config/schedule-manifest.json")["Body"].read())
        import re
        hits = []
        for sec in ("rules", "schedules"):
            for e in man.get(sec, []):
                m = re.match(r"cron\((\d+) (\d+) ", e.get("expr") or "")
                if not m or e.get("state") != "ENABLED":
                    continue
                h = int(m.group(2))
                if h >= 22 or h < 2:
                    fn = (e.get("targets") or [{}])[0].get("arn", "").split(":function:")[-1]
                    hits.append("%02d:%02d %s" % (h, int(m.group(1)), fn))
        r.log("%d enabled triggers between 22:00 and 02:00 UTC: %s" % (len(hits), ", ".join(sorted(hits))[:3000]))
        for fn, st_key in (("justhodl-fmp-walker", "data/warm/fmp/_state/state.json"), ("justhodl-fmp-bank", "data/warm/fmp-bank/_state/state.json")):
            try:
                st = json.loads(s3.get_object(Bucket=LIVE, Key=st_key)["Body"].read())
                lr = st.get("last_run") or {}
                r.log("%s last run: started %s calls %s http %s" % (fn, lr.get("started"), lr.get("calls"), json.dumps(lr.get("http"))))
            except Exception as e:  # noqa: BLE001
                r.warn("%s state: %s" % (fn, type(e).__name__))
        r.ok("ops 6657 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
