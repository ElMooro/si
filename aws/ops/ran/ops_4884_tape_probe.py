"""ops/4884 -- tape-truth source probe (report-only).
Four candidate feeds, bind only what prints:
 (a) Polygon minute aggs SPY (donor key sweep) -- CVD raw
     material; also options snapshot entitlement check.
 (b) FINRA regsho daily short-volume file (keyless CDN).
 (c) CBOE delayed-quotes chain CDN for SPY + _SPX -- full
     chain w/ greeks would unlock dealer GEX keylessly.
 (d) existing S3 banks: etf-true-flows + 13F dollar-flows key
     shapes (fields for the fusion join).
"""
import gzip
import json
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"


def poly_key():
    for fn in ("justhodl-spx-beaters", "justhodl-spx-ma",
               "justhodl-intraday", "justhodl-symbol-resolver"):
        try:
            env = lam.get_function_configuration(
                FunctionName=fn)["Environment"]["Variables"]
            for k, v in env.items():
                if "POLYGON" in k.upper():
                    return fn, k, v
        except Exception:  # noqa: BLE001
            pass
    return None, None, None


def hit(rep, tag, url, headers=None, maxb=400):
    try:
        req = urllib.request.Request(
            url, headers=headers or {"User-Agent": "ops-4884"})
        with urllib.request.urlopen(req, timeout=50) as r:
            raw = r.read()
        rep.log("[%s] 200 bytes=%d" % (tag, len(raw)))
        return raw
    except Exception as e:  # noqa: BLE001
        rep.log("[%s] DIED %s" % (tag, str(e)[:90]))
        return None


def main():
    with report("ops 4884 -- tape probe") as rep:
        fn, kn, kv = poly_key()
        if kv:
            rep.ok("polygon donor %s env %s" % (fn, kn))
            d = date.today() - timedelta(days=1)
            while d.weekday() >= 5:
                d -= timedelta(days=1)
            raw = hit(rep, "poly-min",
                      "https://api.polygon.io/v2/aggs/ticker/"
                      "SPY/range/1/minute/%s/%s?limit=50000"
                      "&apiKey=%s" % (d, d, kv))
            if raw:
                j = json.loads(raw)
                rs = j.get("results") or []
                rep.log("  minute bars n=%d sample=%s"
                        % (len(rs), json.dumps(
                            rs[390] if len(rs) > 390
                            else (rs[0] if rs else {}))[:220]))
            raw = hit(rep, "poly-opt-snap",
                      "https://api.polygon.io/v3/snapshot/"
                      "options/SPY?limit=5&apiKey=%s" % kv)
            if raw:
                j = json.loads(raw)
                rep.log("  opt results n=%s status=%s"
                        % (len(j.get("results") or []),
                           j.get("status")))
        else:
            rep.warn("no polygon donor found")
        d = date.today()
        got_finra = False
        for back in range(1, 6):
            dd = d - timedelta(days=back)
            if dd.weekday() >= 5:
                continue
            raw = hit(rep, "finra-%s" % dd,
                      "https://cdn.finra.org/equity/regsho/"
                      "daily/CNMSshvol%s.txt"
                      % dd.strftime("%Y%m%d"))
            if raw:
                lines = raw.decode("utf-8",
                                   "replace").splitlines()
                rep.log("  lines=%d hdr=%r row1=%r"
                        % (len(lines), lines[0][:120],
                           lines[1][:120] if len(lines) > 1
                           else ""))
                got_finra = True
                break
        if not got_finra:
            rep.warn("finra regsho unreachable")
        for sym in ("SPY", "_SPX"):
            raw = hit(rep, "cboe-%s" % sym,
                      "https://cdn.cboe.com/api/global/"
                      "delayed_quotes/options/%s.json" % sym,
                      headers={"User-Agent": "Mozilla/5.0",
                               "Referer":
                               "https://www.cboe.com/"})
            if raw:
                try:
                    j = json.loads(raw)
                    opts = ((j.get("data") or {})
                            .get("options") or [])
                    rep.log("  %s options n=%d keys=%s"
                            % (sym, len(opts),
                               list(opts[0])[:14]
                               if opts else []))
                    if opts:
                        rep.log("  sample=%s" % json.dumps(
                            opts[len(opts) // 2])[:340])
                    rep.log("  data keys=%s close=%s"
                            % (list(j.get("data") or {})[:12],
                               (j.get("data") or {})
                               .get("close")))
                except ValueError:
                    rep.log("  non-json head=%r" % raw[:120])
        for key in ("data/etf-true-flows.json",
                    "data/13f-dollar-flows.json",
                    "data/short-squeeze.json"):
            try:
                raw = s3.get_object(Bucket=B,
                                    Key=key)["Body"].read()
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                j = json.loads(raw)
                rep.log("[s3 %s] top keys=%s" % (key,
                                                 list(j)[:10]))
            except Exception as e:  # noqa: BLE001
                rep.log("[s3 %s] absent (%s)" % (key,
                                                 str(e)[:40]))
        if not (kv or got_finra):
            rep.fail("nothing usable proved")
            sys.exit(1)
        rep.ok("probe complete -- engine binds only the "
               "above")


if __name__ == "__main__":
    main()
