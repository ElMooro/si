"""ops_4020 — verify the TV workbench end to end."""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
MARK = "tv-workbench v1.0 ops4019"
PAGE = "https://justhodl.ai/tv-workbench.html"
MARKS = ["v1-ops4019", "pulled from", "watchlist", "TV Workbench"]


def read_out():
    try:
        return json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-workbench.json")["Body"].read())
    except Exception:
        return None


def main():
    with report("4020_workbench_verify") as rep:
        rep.heading("ops 4020 — TV workbench verify")
        checks = []
        doc = None
        for i in range(14):
            doc = read_out()
            if doc and doc.get("marker") == MARK:
                rep.ok(f"  artifact after ~{i*20}s")
                break
            time.sleep(20)
        t = (doc or {}).get("totals") or {}
        rep.kv(marker=(doc or {}).get("marker"), **t)
        checks += [
            ("artifact is v1.0", (doc or {}).get("marker") == MARK),
            (">=150 watchlists", (t.get("watchlists") or 0) >= 150),
            (">=800 unique indicators", (t.get("unique_symbols") or 0) >= 800),
            (">=300 indicators carry notes",
             (t.get("symbols_with_notes") or 0) >= 300),
        ]
        wls = (doc or {}).get("watchlists") or []
        bs = next((w for w in wls if "swan" in w.get("name", "").lower()), None)
        rep.kv(blackswan=(bs or {}).get("name"), bs_n=(bs or {}).get("n"))
        checks.append(("Black Swan Event mirrored >=400 indicators",
                       bool(bs) and (bs.get("n") or 0) >= 400))
        syms = (doc or {}).get("symbols") or {}
        jplg = next((v for k, v in syms.items()
                     if v.get("bare") == "JPLG" or k.endswith("JPLG")), None)
        rep.kv(jplg=json.dumps(jplg)[:220] if jplg else None)
        checks.append(("JPLG carries a verbatim note",
                       bool(jplg) and (jplg.get("n_notes") or 0) >= 1
                       and bool((jplg.get("notes") or [{}])[0].get("t"))))
        rep.kv(sources_available=((doc or {}).get("inputs") or {})
               .get("sources_available"),
               note="False until the v1.5.0 Upload — honest, not a gap")
        got, htm = 0, ""
        for i in range(8):
            try:
                r = urllib.request.Request(PAGE + f"?cb={int(time.time())}",
                                           headers={"User-Agent": "Mozilla/5.0",
                                                    "Cache-Control": "no-cache"})
                htm = urllib.request.urlopen(r, timeout=25).read().decode(
                    "utf8", "ignore")
                got = sum(1 for m in MARKS if m in htm)
                if got == len(MARKS):
                    break
            except Exception:
                pass
            time.sleep(15)
        rep.kv(page_bytes=len(htm), markers=f"{got}/{len(MARKS)}")
        checks.append(("page live at edge", got == len(MARKS)))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {t.get('watchlists')} watchlists / "
               f"{t.get('unique_symbols')} indicators / "
               f"{t.get('symbols_with_notes')} noted; Black Swan "
               f"{bs.get('n')}; page {got}/{len(MARKS)}")


if __name__ == "__main__":
    main()
