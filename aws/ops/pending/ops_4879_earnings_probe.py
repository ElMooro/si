"""ops/4879 -- earnings-engine probe (report-only).
 (a) FMP donor sweep: find which fleet function carries an FMP
     key (env name printed, value never).
 (b) Endpoint shape dumps with that key, tiny samples:
     - earnings calendar (last 7d window)
     - earnings surprises (AAPL)
     - earnings call transcript (AAPL latest) -- size + first
       300 chars
     - stable bulk variants if alive
 The engine binds ONLY what prints here.
"""
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1")
CANDIDATES = ["justhodl-estimate-revisions",
              "justhodl-deal-scanner",
              "justhodl-universe-builder",
              "justhodl-stock-buying",
              "justhodl-fundamental-census",
              "justhodl-comeback-screener",
              "justhodl-catalyst"]


def get_key(rep):
    for fn in CANDIDATES:
        try:
            env = lam.get_function_configuration(
                FunctionName=fn)["Environment"]["Variables"]
        except Exception:  # noqa: BLE001
            continue
        for k in env:
            if "FMP" in k.upper():
                rep.ok("donor %s env %s (len=%d)"
                       % (fn, k, len(env[k])))
                return env[k]
    return None


def hit(rep, tag, url):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "ops-4879"})
        with urllib.request.urlopen(req, timeout=45) as r:
            raw = r.read()
        rep.log("[%s] HTTP %d bytes=%d" % (tag, r.status,
                                           len(raw)))
        try:
            j = json.loads(raw)
        except ValueError:
            rep.log("  non-json: %r" % raw[:160])
            return None
        if isinstance(j, list):
            rep.log("  list n=%d" % len(j))
            if j:
                rep.log("  item0: %s"
                        % json.dumps(j[0],
                                     ensure_ascii=False)[:420])
        else:
            rep.log("  dict keys=%s" % list(j)[:12])
            rep.log("  %s" % json.dumps(
                j, ensure_ascii=False)[:420])
        return j
    except Exception as e:  # noqa: BLE001
        rep.log("[%s] DIED %s" % (tag, str(e)[:90]))
        return None


def main():
    with report("ops 4879 -- earnings probe") as rep:
        key = get_key(rep)
        if not key:
            rep.fail("no FMP donor found")
            sys.exit(1)
        from datetime import date, timedelta
        d1 = date.today()
        d0 = d1 - timedelta(days=7)
        base3 = "https://financialmodelingprep.com/api/v3"
        bases = "https://financialmodelingprep.com/stable"
        hit(rep, "cal-v3", "%s/earning_calendar?from=%s&to=%s"
            "&apikey=%s" % (base3, d0, d1, key))
        hit(rep, "cal-stable",
            "%s/earnings-calendar?from=%s&to=%s&apikey=%s"
            % (bases, d0, d1, key))
        hit(rep, "surp-v3",
            "%s/earnings-surprises/AAPL?apikey=%s"
            % (base3, key))
        hit(rep, "surp-stable",
            "%s/earnings-surprises?symbol=AAPL&apikey=%s"
            % (bases, key))
        tr = hit(rep, "transcript-v3",
                 "%s/earning_call_transcript/AAPL?apikey=%s"
                 % (base3, key))
        if isinstance(tr, list) and tr and \
                isinstance(tr[0], dict):
            c = str(tr[0].get("content", ""))
            rep.log("  transcript0 q=%s y=%s date=%s "
                    "chars=%d head=%r"
                    % (tr[0].get("quarter"),
                       tr[0].get("year"),
                       tr[0].get("date"), len(c), c[:300]))
        hit(rep, "transcript-stable",
            "%s/earning-call-transcript?symbol=AAPL&apikey=%s"
            % (bases, key))
        rep.ok("shapes on record")


if __name__ == "__main__":
    main()
