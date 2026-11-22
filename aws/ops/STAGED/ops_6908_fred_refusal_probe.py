"""ops 6908 - READ-ONLY: why FRED refused the board at 06:10 UTC, and what the bank holds to fall back on.

ops 6907: in the 06:10:18 run the ninth dollar request answered an HTTP error at 06:10:23, the FRED discovery failed
at 06:10:49 and no FRED-group series reached the board (766 series instead of 1,800).

Read here, nothing written, the key never printed:
  1. the ICE BofA group's newest dates on the board - the block between those two moments asks FRED once for every
     ICE series older than 21 days, as fast as it can;
  2. FRED now, with the engine's own key: the calls the run makes (one dollar series, the tag inventory, the SOFR
     search, one series title), each with its HTTP status and FRED's own error text;
  3. the bank the group could be served from when FRED refuses: data/warm/fred-scoped/FRED_Repo_Complex/.
"""
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE, FN = "justhodl-dashboard-live", "justhodl-repo"
FRED = "https://api.stlouisfed.org/fred/"


def ask(path, params, key):
    """(status, short text) - never the URL, which carries the key."""
    q = dict(params, api_key=key, file_type="json")
    try:
        with urllib.request.urlopen(urllib.request.Request(FRED + path + "?" + urllib.parse.urlencode(q), headers={"User-Agent": "JustHodl research"}), timeout=45) as resp:
            doc = json.loads(resp.read())
            n = len(doc.get("seriess") or doc.get("observations") or [])
            return resp.status, "%d rows, count %s" % (n, doc.get("count"))
    except urllib.error.HTTPError as e:
        body = b""
        try:
            body = e.read()
        except Exception:  # noqa: BLE001
            pass
        text = body[:200].decode("utf-8", "replace").replace(key, "<key>")
        return e.code, "%s | retry-after %s" % (text, e.headers.get("Retry-After") if e.headers else None)
    except Exception as e:  # noqa: BLE001
        return type(e).__name__, ""


def selftest():
    assert FRED.endswith("/fred/")


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1")
    with report("ops_6908_fred_refusal_probe") as r:
        r.heading("ops 6908 - FRED's refusal at 06:10 UTC and the bank behind the group (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        board = json.loads(s3.get_object(Bucket=LIVE, Key="data/repo.json")["Body"].read())
        today = datetime.now(timezone.utc).date()
        r.section("1. The ICE BofA group")
        for g in board.get("groups") or []:
            if "ICE BofA" in g["name"]:
                lasts = Counter(s.get("last") for s in g["series"])
                old = sum(n for d, n in lasts.items() if d and (today - date.fromisoformat(d)).days > 21)
                r.log("%s: %d series | older than 21 days (asked of FRED on every run): %d | newest dates %s" % (g["name"], len(g["series"]), old, dict(lasts.most_common(6))))
        r.section("2. FRED now, with the engine's key")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        key = env.get("FRED_API_KEY") or ""
        r.log("the function has a FRED key: %s (%d characters)" % (bool(key), len(key)))
        if key:
            for label, path, params in (("one dollar series, TWEXEMEGSMTH", "series/observations", {"series_id": "TWEXEMEGSMTH", "observation_start": "2026-06-01"}),
                                        ("a dollar series that answered, DTWEXBGS", "series/observations", {"series_id": "DTWEXBGS", "observation_start": "2026-11-01"}),
                                        ("tag inventory, first page", "tags/series", {"tag_names": "repurchase agreements", "limit": 500, "offset": 0}),
                                        ("tag inventory, second page", "tags/series", {"tag_names": "repurchase agreements", "limit": 500, "offset": 500}),
                                        ("SOFR search", "series/search", {"search_text": "SOFR", "limit": 60}),
                                        ("one title, IORB", "series", {"series_id": "IORB"}),
                                        ("one ICE series, BAMLH0A0HYM2", "series/observations", {"series_id": "BAMLH0A0HYM2", "observation_start": "2026-11-01"})):
                code, text = ask(path, params, key)
                r.log("  %-42s %s | %s" % (label, code, text[:230]))
                time.sleep(0.6)
            t0, codes = time.time(), Counter()
            for i in range(40):                                  # the pace the ICE block asks at: no pause
                code, _ = ask("series", {"series_id": "IORB"}, key)
                codes[code] += 1
            r.log("  40 title requests without a pause: %.1f s (%.0f a minute) -> %s" % (time.time() - t0, 40 / (time.time() - t0) * 60, dict(codes)))
        r.section("3. The bank behind the group")
        n, size, sample, token = 0, 0, [], None
        while True:
            kw = {"Bucket": LIVE, "Prefix": "data/warm/fred-scoped/FRED_Repo_Complex/"}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            for o in page.get("Contents", []):
                n += 1
                size += o["Size"]
                if len(sample) < 4:
                    sample.append(o["Key"].split("/")[-1])
            token = page.get("NextContinuationToken")
            if not token:
                break
        one = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/fred-scoped/FRED_Repo_Complex/IORB.json")["Body"].read())
        r.log("banked documents: %d, %.1f MB | first %s | IORB document keys %s, title %r, banked_at %s, %d observations to %s" % (
            n, size / 1e6, sample, sorted(one), one.get("title"), one.get("banked_at"), len(one.get("observations") or []), (one.get("observations") or [{}])[-1].get("date")))
        r.kv(banked_documents=n)
        r.ok("read-only")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
