"""ops 6746 - READ-ONLY probe: can a leader's bars be stitched to its previous ticker? (Khalid 2026-11-04: "build a new
engine called leaders ...").

ops 6745 found seven leaders whose 600 banked sessions cross a calendar gap - the ticker belonged to another security,
or the company traded under another symbol in between (BK -> BNY, GOLD -> B, FI -> FISV, DOOO -> DOO ...) - and one
separation (CTVA, x0.16 overnight) that no split file carries. Cutting the history at the break is safe but leaves a
bank with 115 sessions and no 200-session average. This probe reads what the warehouse knows about ticker changes
so the engine can prepend the predecessor's bars when - and only when - the record and the tape agree:

  1. FMP's symbol-change list (the walker's data/warm/fmp/symbol-change/latest.json.gz): shape, freshness, the rows
     whose new symbol is a leader or a catalog fund;
  2. for every leader with a break or fewer than 600 bars: the symbol-change row, the predecessor's bars in the
     Polygon EOD bank, and the continuity of the hand-over (last old close against first new close);
  3. the Polygon bank's ticker-events and ticker-details files for the same tickers;
  4. the separation: Polygon dividends, FMP splits / dividends and the new ticker around CTVA's 2026-10-01 print.

Nothing is written to S3.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-leaders" / "source"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=48)
GAP_DAYS = 10


def days(a, b):
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)

    def get(key):
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
        except Exception as exc:  # noqa: BLE001
            return None, "%s" % type(exc).__name__, None
        raw = obj["Body"].read()
        if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        try:
            return json.loads(raw), None, obj
        except Exception as exc:  # noqa: BLE001
            return None, "json %s" % type(exc).__name__, obj

    with report("ops_6746_leaders_ticker_change_probe") as r:
        r.heading("ops 6746 - ticker changes and predecessors for the leaders universe (read-only) %s" % now.isoformat(timespec="minutes"))
        import bank_enrich
        import industry_etf_catalog
        import lambda_function as lf
        book = bank_enrich.rows(fields=lf.BOOK_FIELDS + ["ipo_date"])
        leaders, industries, stats = lf.select_leaders(book)
        name = {x["ticker"]: x["name"] for x in leaders}
        funds = sorted(industry_etf_catalog.ALL_ETFS)
        tickers = list(dict.fromkeys([x["ticker"] for x in leaders] + funds + [lf.BENCH]))
        want = set(tickers)

        r.section("1. FMP symbol-change list")
        key = "data/warm/fmp/symbol-change/latest.json.gz"
        rows, err, obj = get(key)
        if rows is None:
            r.warn("symbol-change not readable: %s" % err)
            rows = []
        else:
            r.log("%s: %d bytes, written %s, metadata %s" % (key, obj["ContentLength"], obj["LastModified"].isoformat(timespec="seconds"), json.dumps(obj.get("Metadata") or {})))
        rows = rows if isinstance(rows, list) else (rows.get("results") or rows.get("data") or [])
        r.log("rows %d | keys %s" % (len(rows), sorted({k for x in rows[:200] if isinstance(x, dict) for k in x})))
        ds = sorted(str(x.get("date") or "") for x in rows if isinstance(x, dict))
        r.log("date range %s .. %s | newest five %s" % (ds[0] if ds else None, ds[-1] if ds else None, json.dumps(sorted(rows, key=lambda x: str(x.get("date") or ""))[-5:])))
        by_new = {}
        for x in rows:
            if isinstance(x, dict) and x.get("newSymbol"):
                by_new.setdefault(lf.canon(x["newSymbol"]), []).append(x)
        hits = sorted(((t, x) for t in want for x in by_new.get(t, [])), key=lambda p: str(p[1].get("date") or ""))
        recent = [p for p in hits if str(p[1].get("date") or "") >= "2024-01-01"]
        r.log("rows whose new symbol is in the universe: %d (since 2024-01-01: %d)" % (len(hits), len(recent)))
        for t, x in recent:
            r.log("  %-6s <- %-6s %s  %s" % (t, x.get("oldSymbol"), x.get("date"), (x.get("companyName") or x.get("name") or "")[:60]))

        r.section("2. leaders with a break or a short history: the record against the tape")

        def fetch(t):
            try:
                return t, lf.read_bars(s3, t, 1400)
            except Exception as exc:  # noqa: BLE001
                return t, "%s" % type(exc).__name__
        bars_by = {}
        with ThreadPoolExecutor(max_workers=16) as pool:
            for t, b in pool.map(fetch, tickers):
                bars_by[t] = b if isinstance(b, list) else []
        cases = []
        for t in tickers:
            b = bars_by.get(t) or []
            if not b:
                continue
            brk = None
            for i in range(1, len(b)):
                if days(b[i - 1]["date"], b[i]["date"]) > GAP_DAYS:
                    brk = i
            start = b[brk]["date"] if brk is not None else b[0]["date"]
            own = len(b) - (brk or 0)
            if own < 600:                                    # what the engine reads: the last 600 sessions
                cases.append((t, start, own, brk is not None))
        r.log("cases %d (with a calendar gap %d, the rest simply short)" % (len(cases), sum(1 for c in cases if c[3])))
        olds = sorted({str(x.get("oldSymbol") or "") for t, _, _, _ in cases for x in by_new.get(t, []) if x.get("oldSymbol")})
        old_bars = {}
        with ThreadPoolExecutor(max_workers=16) as pool:
            for t, b in pool.map(fetch, [lf.canon(o) for o in olds]):
                old_bars[t] = b if isinstance(b, list) else []
        n_stitch = 0
        for t, start, own, gap in sorted(cases, key=lambda c: c[1]):
            b = bars_by[t]
            first = next(x for x in b if x["date"] == start)
            recs = sorted(by_new.get(t, []), key=lambda x: abs(days(str(x.get("date"))[:10], start)) if str(x.get("date") or "")[:10] else 99999)
            line = "%-6s %-26s own bars from %s (%d sessions%s) first close %.4f" % (t, (name.get(t) or ("fund" if t in funds else ""))[:26], start, own, ", after a gap" if gap else "", float(first["c"]))
            if not recs:
                r.log(line + " | no symbol-change row")
                continue
            x = recs[0]
            o = lf.canon(x.get("oldSymbol"))
            ob = [y for y in old_bars.get(o, []) if y["date"] < start]
            if not ob:
                r.log(line + " | record %s -> %s on %s | predecessor has no bars before %s (bank holds %d)" % (x.get("oldSymbol"), x.get("newSymbol"), x.get("date"), start, len(old_bars.get(o, []))))
                continue
            last = ob[-1]
            ratio = float(first["c"]) / float(last["c"]) if float(last["c"]) else float("nan")
            overlap = sum(1 for y in old_bars.get(o, []) if y["date"] >= start)
            ok = days(last["date"], start) <= GAP_DAYS and 0.7 <= ratio <= 1.4
            n_stitch += 1 if ok else 0
            r.log(line + " | record %s -> %s on %s | predecessor %d bars %s..%s last close %.4f | hand-over %d days, x%.4f, predecessor bars on/after the start %d | %s" % (
                x.get("oldSymbol"), x.get("newSymbol"), x.get("date"), len(ob), ob[0]["date"], last["date"], float(last["c"]), days(last["date"], start), ratio, overlap, "STITCHABLE" if ok else "NOT CONTINUOUS"))
        r.log("stitchable %d of %d cases" % (n_stitch, len(cases)))

        r.section("3. Polygon bank: ticker-events and ticker-details for the same tickers")
        for t, start, own, gap in sorted(cases, key=lambda c: c[1]):
            ev, e1, o1 = get("data/warm/polygon-bank/ticker-events/%s.json.gz" % t)
            det, e2, o2 = get("data/warm/polygon-bank/ticker-details/%s.json.gz" % t)
            d = det if isinstance(det, dict) else (det[0] if isinstance(det, list) and det else {})
            d = d.get("results", d) if isinstance(d, dict) else {}
            r.log("%-6s events %s | details %s" % (t, (json.dumps(ev)[:420] + " (written %s)" % o1["LastModified"].date().isoformat()) if ev is not None else e1,
                                                 json.dumps({k: d.get(k) for k in ("ticker", "name", "list_date", "delisted_utc", "primary_exchange", "type", "cik")}) if d else e2))

        r.section("4. the separation (CTVA 2026-10-01) and what records exist for it")
        for k in ("data/warm/polygon-bank/dividends/2026-09.json.gz", "data/warm/polygon-bank/dividends/2026-10.json.gz", "data/warm/polygon-bank/splits/2026-09.json.gz", "data/warm/polygon-bank/splits/2026-10.json.gz",
                  "data/warm/polygon-bank/ipos/2026-09.json.gz", "data/warm/polygon-bank/ipos/2026-10.json.gz"):
            doc, err, obj = get(k)
            xs = doc if isinstance(doc, list) else ((doc or {}).get("results") or []) if isinstance(doc, dict) else []
            hit = [x for x in xs if isinstance(x, dict) and str(x.get("ticker") or "").upper() in ("CTVA", "VYLR", "VMRK", "SKYD", "FDXF", "SUNB", "IOND", "DPC", "VSXY")]
            r.log("%s: %s rows %d | keys %s | hits %s" % (k, err or "ok", len(xs), sorted({kk for x in xs[:50] if isinstance(x, dict) for kk in x})[:24], json.dumps(hit)[:900]))
        for ds_id in ("splits", "dividends"):
            for sym in ("CTVA", "VYLR"):
                doc, err, obj = get("data/warm/fmp/%s/%s.json.gz" % (ds_id, sym))
                r.log("fmp %s/%s: %s" % (ds_id, sym, (json.dumps(doc[:3] if isinstance(doc, list) else doc)[:600] + " (written %s)" % obj["LastModified"].date().isoformat()) if doc is not None else err))
        for t in ("CTVA", "VYLR", "VMRK", "SKYD", "FDXF", "SUNB"):
            b = bars_by.get(t) or lf.read_bars(s3, t, 1400)
            x = [y for y in book if lf.canon(y.get("symbol")) == t]
            y = x[0] if x else {}
            r.log("%-5s bars %d %s..%s | book: %s | symbol-change rows %s" % (t, len(b), b[0]["date"] if b else None, b[-1]["date"] if b else None,
                                                                           json.dumps({k: y.get(k) for k in ("name", "industry", "mcap", "ipo_date", "cik")}), json.dumps(by_new.get(t, []))[:300]))
        ctva = bars_by.get("CTVA") or []
        vylr = lf.read_bars(s3, "VYLR", 1400)
        if ctva and vylr:
            d0 = vylr[0]["date"]
            pre = [x for x in ctva if x["date"] < d0]
            post = [x for x in ctva if x["date"] >= d0]
            if pre and post:
                r.log("CTVA last close before %s: %.4f | CTVA first close on it: %.4f | VYLR first close %.4f | CTVA + VYLR = %.4f (x%.4f of the last pre-separation close) | CTVA alone x%.4f" % (
                    d0, float(pre[-1]["c"]), float(post[0]["c"]), float(vylr[0]["c"]), float(post[0]["c"]) + float(vylr[0]["c"]), (float(post[0]["c"]) + float(vylr[0]["c"])) / float(pre[-1]["c"]), float(post[0]["c"]) / float(pre[-1]["c"])))
        r.kv(cases=str(len(cases)), stitchable=str(n_stitch), symbol_change_rows=str(len(rows)))
        r.ok("ticker-change probe complete (nothing written)")


if __name__ == "__main__":
    main()
    sys.exit(0)
