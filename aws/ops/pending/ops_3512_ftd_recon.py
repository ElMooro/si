"""ops 3512 — equity-FTD family recon (read-only): prove the SEC CNS
file format end-to-end before the graded-family build. Reuses
ignition's URL scheme. Prints header verbatim, price-column presence,
settlement-date span, top-10 by shares and by $, sample tickers.
"""
import io, json, sys, urllib.request, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}

with report("3512_ftd_recon") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    now = datetime.now(timezone.utc)
    got = None
    for k in range(4):
        d = now.replace(day=15) - timedelta(days=31 * k)
        for half in ("b", "a"):
            tag = d.strftime("%Y%m") + half
            url = ("https://www.sec.gov/files/data/fails-deliver-data/"
                   f"cnsfails{tag}.zip")
            try:
                raw = urllib.request.urlopen(
                    urllib.request.Request(url, headers=UA), timeout=50).read()
                got = (tag, raw); break
            except Exception:
                continue
        if got: break
    if not got:
        gate("Y1_fetch", False, "no cnsfails file reachable in 4 months")
    else:
        tag, raw = got
        zf = zipfile.ZipFile(io.BytesIO(raw))
        txt = zf.read(zf.namelist()[0]).decode("utf-8", "replace")
        lines = txt.split("\n")
        hdr = lines[0].strip()
        rows = [l.split("|") for l in lines[1:] if l.count("|") >= 4]
        dates = sorted({r[0].strip() for r in rows if r[0].strip().isdigit()})
        has_price = sum(1 for r in rows[:2000]
                        if len(r) >= 6 and r[5].strip().replace(".", "")
                        .isdigit()) > 1000
        agg_q, agg_d = {}, {}
        for r in rows:
            sym = r[2].strip()
            if not sym:
                continue
            try:
                q = int(r[3])
            except ValueError:
                continue
            agg_q[sym] = agg_q.get(sym, 0) + q
            if has_price and len(r) >= 6:
                try:
                    agg_d[sym] = agg_d.get(sym, 0) + q * float(r[5])
                except ValueError:
                    pass
        topq = sorted(agg_q.items(), key=lambda x: -x[1])[:10]
        topd = sorted(agg_d.items(), key=lambda x: -x[1])[:10]
        gate("Y1_format", len(rows) > 50000 and len(dates) >= 8,
             {"tag": tag, "zip_kb": len(raw)//1024, "header": hdr,
              "n_rows": len(rows), "n_symbols": len(agg_q),
              "settle_dates": [dates[0], dates[-1], len(dates)],
              "has_price_col": has_price})
        gate("Y2_content", topq[0][1] > 1e5,
             {"top10_shares": topq,
              "top10_dollars": [(s, round(v/1e6, 1)) for s, v in topd],
              "samples": {s: agg_q.get(s) for s in
                          ("AAPL", "TSLA", "GME", "AMC", "NVDA")}})
    (REPO/"aws/ops/reports/3512.json").write_text(
        json.dumps({"ops": 3512, "fails": fails}))
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
sys.exit(0)
