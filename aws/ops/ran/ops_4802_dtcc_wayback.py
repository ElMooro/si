"""ops/4802 -- DTCC fails HISTORY recovery via Wayback. failsdata.csv
is a ~1yr rolling window; each archived snapshot holds another year.
CDX-query all snapshots (collapse by digest), fetch up to 40 distinct
captures oldest-first, parse (strip TRAILER, MM/DD/YYYY), merge into
the warm docs the engine upserts daily. Report depth achieved."""
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


def fetch(url, timeout=45):
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
    with report("4802_dtcc_wayback") as rep:
        rep.heading("ops 4802 -- DTCC fails Wayback recovery")
        cdx = ("http://web.archive.org/cdx/search/cdx?url="
                "dtcc.com/data/failsdata.csv&output=json"
                "&fl=timestamp,digest&collapse=digest&limit=200")
        try:
            rows = json.loads(fetch(cdx, 60).decode())
        except Exception as e:
            rep.warn(f"CDX: {type(e).__name__}: {str(e)[:100]}")
            return
        caps = [r[0] for r in rows[1:]] if len(rows) > 1 else []
        rep.kv(check="distinct_snapshots", value=len(caps))
        caps = sorted(caps)[:40]
        tsy_all, agy_all = {}, {}
        got = 0
        for ts in caps:
            u = (f"http://web.archive.org/web/{ts}id_/"
                  "https://www.dtcc.com/data/failsdata.csv")
            try:
                txt = fetch(u, 60).decode("utf-8", "replace")
                t, a = parse_csv(txt)
                if t:
                    tsy_all.update(t)
                    agy_all.update(a)
                    got += 1
            except Exception:
                pass
            time.sleep(1)
        rep.kv(check="snapshots_parsed", value=got)
        rep.kv(check="recovered_days", value=len(tsy_all))
        if tsy_all:
            rep.kv(check="recovered_span",
                    value=f"{min(tsy_all)} -> {max(tsy_all)}")
        merged = {}
        for sid, newd in (("treasury", tsy_all), ("agency", agy_all)):
            wk = f"data/warm/dtcc-fails/{sid}.json"
            try:
                doc = sread(wk)
                m2 = {o["date"]: o["value"]
                       for o in doc.get("observations", [])}
            except Exception:
                doc = {"id": f"DTCC-{sid.upper()}-FAILS",
                        "title": f"FICC daily GROSS {sid} fails",
                        "source": "dtcc.com + wayback",
                        "observations": []}
                m2 = {}
            before = len(m2)
            m2.update({d: v for d, v in newd.items()
                        if d not in m2})
            doc["observations"] = [{"date": a, "value": b}
                                     for a, b in sorted(m2.items())]
            s3.put_object(Bucket=B, Key=wk,
                Body=json.dumps(doc, separators=(",", ":")).encode(),
                ContentType="application/json")
            merged[sid] = (before, len(m2))
            rep.ok(f"{sid}: {before} -> {len(m2)} obs "
                    f"({min(m2) if m2 else '-'} -> "
                    f"{max(m2) if m2 else '-'})")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
