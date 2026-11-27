"""ops 6997 - read-only: the CISS histories for the canaries' record, and how much history FRED still hands the macro grid.

Two things ops 6996 left open:
  1. its CISS cut failed on one unreadable date; this cuts the CISS engine's own document again, skipping such rows;
  2. eight of the grid's credit canaries read far hotter live than their own rule gives on the warehouse's history
     (high-yield spread: 94 live, 46 by a ten-year window). The grid asks FRED for 1,300 or 2,600 observations; this
     counts how many FRED returns for those series today, against what the warehouse holds. The key is read from the
     parameter store, sent to FRED, and never printed.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6997_canaries_ciss_cut_and_fred_reach"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
RESOLVER = "https://justhodl-data-proxy.raafouis.workers.dev/series?id="
ASK = [("BAMLH0A0HYM2", 2600), ("BAMLHE00EHYIOAS", 1300), ("BAMLEMCBPIOAS", 1300), ("BAMLEMHBHYCRPIOAS", 1300), ("BAMLH0A0HYM2SYTW", 1300),
       ("BAMLEMHBHYCRPISYTW", 1300), ("BAMLC0A4CBBB", 1300), ("BAMLC0A1CAAA", 10400), ("T10Y2Y", 2600), ("DGS10", 1300), ("IPG3344S", 160), ("VIXCLS", 1300)]
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***"), (re.compile(r"\b[0-9a-f]{32}\b"), "***")]


def clean(text):
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def ymd(text):
    t = str(text)[:10]
    if re.match(r"^\d{4}-\d{2}$", t):                      # a monthly series: the month, dated its 28th
        return int(t[:4]) * 10000 + int(t[5:7]) * 100 + 28
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", t):
        return None
    return int(t[:4]) * 10000 + int(t[5:7]) * 100 + int(t[8:10])


def main():
    import boto3
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION)
    now = datetime.now(timezone.utc)

    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        r.heading("6997 - the CISS cut, and FRED's reach today (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. What FRED returns to the macro grid's own request, against the warehouse")
        key = None
        try:
            ssm = boto3.client("ssm", region_name=REGION)
            key = ssm.get_parameter(Name="/justhodl/fred/api-key", WithDecryption=True)["Parameter"]["Value"]
            say("key read from the parameter store (not shown)")
        except Exception as e:  # noqa: BLE001
            say("key not read: %s" % type(e).__name__)
        for sid, limit in ASK:
            got = first = last = None
            if key:
                try:
                    url = ("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=%d" % (sid, key, limit))
                    with urllib.request.urlopen(url, timeout=30) as resp:
                        obs = json.loads(resp.read()).get("observations", [])
                    vals = [o for o in obs if o.get("value") not in (".", "", None)]
                    got, first, last = len(obs), (vals[-1]["date"] if vals else None), (vals[0]["date"] if vals else None)
                except Exception as e:  # noqa: BLE001
                    say("  %-22s FRED did not answer: %s" % (sid, type(e).__name__))
                time.sleep(0.4)
            wn = wfirst = wlast = None
            try:
                req = urllib.request.Request(RESOLVER + urllib.parse.quote("fred:" + sid, safe=""), headers={"User-Agent": "JustHodl-Ops/6997"})
                with urllib.request.urlopen(req, timeout=45) as resp:
                    p = json.loads(resp.read().decode("utf-8", "replace"))
                o2 = p.get("obs") or []
                wn = len(o2)
                wfirst = str(o2[0][0] if isinstance(o2[0], list) else o2[0].get("date"))[:10] if o2 else None
                wlast = str(o2[-1][0] if isinstance(o2[-1], list) else o2[-1].get("date"))[:10] if o2 else None
            except Exception as e:  # noqa: BLE001
                say("  %-22s warehouse did not answer: %s" % (sid, str(e)[:80]))
            say("  %-22s asked %5d | FRED returned %s (%s to %s)%s | warehouse holds %s (%s to %s)" % (
                sid, limit, got, first, last, "  <- SHORT of the request" if got is not None and got < limit * 0.9 else "", wn, wfirst, wlast))
            time.sleep(0.25)
        key = None

        r.section("2. The CISS series, from the CISS engine's own document")
        cut = {}
        try:
            d = json.loads(s3.get_object(Bucket=LIVE, Key="data/ciss-stress.json")["Body"].read())
            ser = d.get("series") or []
            items = list(ser.values()) if isinstance(ser, dict) else ser
            skipped = 0
            for i, s in enumerate(items):
                if not isinstance(s, dict):
                    continue
                pts = []
                for row in s.get("chart_points") or []:
                    try:
                        a, b = row[0], row[1]
                        day = ymd(a)
                        if day is None or b is None:
                            skipped += 1
                            continue
                        pts.append([day, float("%.6g" % float(b))])
                    except Exception:  # noqa: BLE001
                        skipped += 1
                st = s.get("stats") or s
                cut[str(i).zfill(3)] = {"label": str(s.get("label") or s.get("name") or s.get("key"))[:140], "key": s.get("key"), "area": s.get("area"),
                                        "category": s.get("category"), "pctile": st.get("pctile"), "latest": st.get("latest"), "freq": s.get("freq"),
                                        "raw_first": str((s.get("chart_points") or [[None]])[0][0])[:12], "p": pts}
            say("%d series; rows skipped as unreadable %d; engine's own fields on the first: %s" % (len(cut), skipped, sorted(items[0])[:45] if items else None))
            for k in list(cut)[:6] + list(cut)[-3:]:
                v = cut[k]
                say("  %s %-70s %5d weeks %s to %s | engine percentile %s" % (k, v["label"][:70], len(v["p"]), v["p"][0][0] if v["p"] else None, v["p"][-1][0] if v["p"] else None, v["pctile"]))
            say("first forty in the document's order with a percentile (the war room's own selection): %s" % json.dumps([k for k, v in cut.items() if v["pctile"] is not None][:40]))
        except Exception as e:  # noqa: BLE001
            say("CISS not read: %s" % str(e)[:200])
        raw = json.dumps(cut, separators=(",", ":")).encode()
        b64 = base64.b64encode(gzip.compress(raw, 9)).decode()
        r.log("<!-- FIXTURE ciss-cut %d bytes gzip+base64" % len(raw))
        for i in range(0, len(b64), 3000):
            r.log(b64[i:i + 3000])
        r.log("FIXTURE-END -->")
        r.ok("read; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
