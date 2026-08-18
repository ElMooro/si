"""ops/4880 -- stable transcript param variants (report-only).
400 on bare symbol => endpoint exists.  Try the documented-ish
shapes; bind whichever returns content.
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


def key():
    for fn in ("justhodl-estimate-revisions",
               "justhodl-deal-scanner",
               "justhodl-universe-builder",
               "justhodl-stock-buying",
               "justhodl-fundamental-census"):
        try:
            env = lam.get_function_configuration(
                FunctionName=fn)["Environment"]["Variables"]
            for k, v in env.items():
                if "FMP" in k.upper():
                    return fn, k, v
        except Exception:  # noqa: BLE001
            pass
    return None, None, None


def main():
    with report("ops 4880 -- transcript variants") as rep:
        fn, kn, kv = key()
        if not kv:
            rep.fail("no donor")
            sys.exit(1)
        rep.ok("donor %s env %s" % (fn, kn))
        S = "https://financialmodelingprep.com/stable"
        tries = [
            ("yq", "%s/earning-call-transcript?symbol=AAPL"
             "&year=2026&quarter=2&apikey=%s" % (S, kv)),
            ("yq3", "%s/earning-call-transcript?symbol=AAPL"
             "&year=2026&quarter=3&apikey=%s" % (S, kv)),
            ("latest", "%s/earning-call-transcript-latest"
             "?symbol=AAPL&apikey=%s" % (S, kv)),
            ("dates", "%s/earning-call-transcript-dates"
             "?symbol=AAPL&apikey=%s" % (S, kv)),
        ]
        hitc = 0
        for tag, url in tries:
            try:
                with urllib.request.urlopen(
                        urllib.request.Request(
                            url, headers={"User-Agent":
                                          "ops-4880"}),
                        timeout=45) as r:
                    raw = r.read()
                j = json.loads(raw)
                rep.log("[%s] 200 bytes=%d type=%s n=%s"
                        % (tag, len(raw), type(j).__name__,
                           len(j) if isinstance(j, list)
                           else "-"))
                it = j[0] if isinstance(j, list) and j else j
                if isinstance(it, dict):
                    rep.log("  keys=%s" % list(it)[:10])
                    c = str(it.get("content", ""))
                    if c:
                        rep.log("  content chars=%d head=%r"
                                % (len(c), c[:220]))
                        hitc += 1
                    else:
                        rep.log("  item: %s" % json.dumps(
                            it, ensure_ascii=False)[:300])
            except Exception as e:  # noqa: BLE001
                rep.log("[%s] DIED %s" % (tag, str(e)[:80]))
        if hitc:
            rep.ok("%d variant(s) with content -- bindable"
                   % hitc)
        else:
            rep.warn("no content variant -- transcript desk "
                     "will ship rules-on-metadata or defer "
                     "honestly")


if __name__ == "__main__":
    main()
