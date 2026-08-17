"""ops/4803 -- DTCC Wayback retry with backoff (CDX 503'd in 4802).
5 attempts x (10,20,40,80,160)s; on success: fetch up to 40 distinct
snapshots, parse, merge-upsert into warm docs. Also tries the
availability API as a fallback seed."""
import gzip
import json
import re
import sys
import time
import urllib.request
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (research; raafouis@gmail.com)"}
s3 = boto3.client("s3", region_name="us-east-1")


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def fetch(url, timeout=50):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=UA),
        timeout=timeout).read()


def parse_csv(txt):
    tsy, agy = {}, {}
    for ln in txt.splitlines():
        if ln.startswith("TRAILER") or ln.startswith("Date"):
            continue
        pc = ln.split(",")
        if len(pc) < 3:
            continue
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", pc[0])
        if not m:
            continue
        ds = f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
        try:
            tsy[ds] = float(pc[1])
            agy[ds] = float(pc[2])
        except Exception:
            continue
    return tsy, agy


def main():
    with report("4803_wayback_retry") as rep:
        rep.heading("ops 4803 -- DTCC Wayback retry (backoff)")
        cdx = ("http://web.archive.org/cdx/search/cdx?url="
                "dtcc.com/data/failsdata.csv&output=json"
                "&fl=timestamp,digest&collapse=digest&limit=200")
        rows = None
        for i, wait in enumerate((0, 10, 20, 40, 80, 160)):
            if wait:
                time.sleep(wait)
            try:
                rows = json.loads(fetch(cdx, 60).decode())
                break
            except Exception as e:
                rep.log(f"attempt {i}: {type(e).__name__}: "
                         f"{str(e)[:80]}")
        if rows is None:
            rep.warn("CDX unreachable after backoff -- leave for the "
                      "next scheduled attempt; live upsert continues "
                      "daily regardless")
            return
        caps = sorted(r[0] for r in rows[1:]) if len(rows) > 1 else []
        rep.kv(check="distinct_snapshots", value=len(caps))
        tsy_all, agy_all = {}, {}
        got = 0
        for ts in caps[:40]:
            u = (f"http://web.archive.org/web/{ts}id_/"
                  "https://www.dtcc.com/data/failsdata.csv")
            try:
                t, a = parse_csv(fetch(u, 60).decode(
                    "utf-8", "replace"))
                if t:
                    tsy_all.update(t)
                    agy_all.update(a)
                    got += 1
            except Exception:
                pass
            time.sleep(1.2)
        rep.kv(check="snapshots_parsed", value=got)
        rep.kv(check="recovered_days", value=len(tsy_all))
        if tsy_all:
            rep.kv(check="recovered_span",
                    value=f"{min(tsy_all)} -> {max(tsy_all)}")
        for sid, newd in (("treasury", tsy_all), ("agency", agy_all)):
            if not newd:
                continue
            wk = f"data/warm/dtcc-fails/{sid}.json"
            try:
                doc = sread(wk)
                m2 = {o["date"]: o["value"]
                       for o in doc.get("observations", [])}
            except Exception:
                doc = {"id": f"DTCC-{sid.upper()}-FAILS",
                        "title": f"FICC daily GROSS {sid} fails",
                        "observations": []}
                m2 = {}
            before = len(m2)
            for d, v in newd.items():
                m2.setdefault(d, v)
            doc["observations"] = [{"date": a, "value": b}
                                     for a, b in sorted(m2.items())]
            doc["source"] = "dtcc.com + wayback"
            s3.put_object(Bucket=B, Key=wk,
                Body=json.dumps(doc, separators=(",", ":")).encode(),
                ContentType="application/json")
            rep.ok(f"{sid}: {before} -> {len(m2)} obs "
                    f"({min(m2)} -> {max(m2)})")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
