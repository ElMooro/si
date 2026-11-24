"""ops 6939 - READ-ONLY audit before Credit Before Equity is widened.

Khalid 2026-11-24: "expand it to cover as many companies as possible and it should have a heatmap that shows industries with
biggest corp spread widening which signal trouble and ones with lowes which signals safety also i should be able to sort them
by industry and sort them ascending and descending and also add companies CDS credit default swaps".

Nothing is invoked and nothing is written. It reads what is live and reports:

  1. The board today: how many issuers, where they come from, how deep its own ledger is.
  2. The structural model it reads (cds-monitor): how many names, what each row carries.
  3. The CDS desk (real single-name CDS prints from the public DTCC tape): how many companies are priced, how fresh, and
     how many of them can be tied to a listed company by name.
  4. The corporate bonds desk (bonds inside iShares / SPDR funds, each with a printed yield): bonds, issuers, industries,
     which fields each carries, and whether a yield spread over Treasuries can be measured from what is there.
  5. What the warehouse holds for a whole-market structural model: the factor book's fields, balance-sheet debt in the
     history book, the Treasury par curve.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared")]
LIVE = "justhodl-dashboard-live"


def short(v, n=220):
    s = json.dumps(v, default=str, ensure_ascii=False)
    return s if len(s) <= n else s[:n] + "..."


def shape(o, depth=0, limit=28):
    """A compact description of a document: keys, types, list lengths."""
    if isinstance(o, dict):
        if depth >= 2:
            return "{%d keys}" % len(o)
        return {k: shape(v, depth + 1) for k, v in list(o.items())[:limit]}
    if isinstance(o, list):
        return "[%d x %s]" % (len(o), (sorted(o[0].keys())[:40] if o and isinstance(o[0], dict) else type(o[0]).__name__ if o else "-"))
    return type(o).__name__ if not isinstance(o, (int, float, str, bool)) or isinstance(o, str) and len(o) > 60 else o


def main():
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        raw = o["Body"].read()
        if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw), len(raw), o["LastModified"]

    def tryget(key):
        try:
            return get(key)
        except Exception as e:  # noqa: BLE001
            return None, 0, "%s" % type(e).__name__

    def ls(prefix, limit=2000):
        out, tok = [], None
        while len(out) < limit:
            kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            out += [(x["Key"], x["Size"], x["LastModified"]) for x in resp.get("Contents") or []]
            tok = resp.get("NextContinuationToken")
            if not tok:
                break
        return out

    with report("ops_6939_credit_before_equity_audit") as r:
        r.heading("ops 6939 - Credit Before Equity: what is live and what the warehouse holds (read-only), %s" % now.isoformat(timespec="minutes"))

        # ------------------------------------------------------------------ 1. the board today
        r.section("1. The board today")
        doc, size, when = tryget("data/credit-before-equity.json")
        if doc:
            names = doc.get("names") or []
            sig = {}
            for x in names:
                sig[x.get("signal")] = sig.get(x.get("signal"), 0) + 1
            r.log("data/credit-before-equity.json: %d bytes, written %s, version %s, generated %s" % (size, when, doc.get("version"), doc.get("generated_at")))
            r.log("issuers %d | signals %s | degraded %s" % (len(names), sig, doc.get("degraded")))
            r.log("groups: %s" % {g: sum(1 for x in names if x.get("group") == g) for g in sorted({str(x.get("group")) for x in names})})
            r.log("tickers: %s" % " ".join(sorted(str(x.get("ticker")) for x in names)))
            r.log("a row: %s" % short(names[0] if names else None, 700))
            r.log("rows with no price change (the stock was not found in data/universe.json): %s" % [x.get("ticker") for x in names if x.get("d_price_pct") is None and x.get("prior_obs_date")])
        hist, hsize, hwhen = tryget("credit/credit-before-equity-history.json")
        if hist:
            obs = hist.get("obs") or {}
            depth = sorted(len(v) for v in obs.values())
            days = sorted({d for v in obs.values() for d in v})
            r.log("its ledger: %d bytes, %d tickers, %d distinct days from %s to %s, days per ticker min/median/max %s/%s/%s" % (
                hsize, len(obs), len(days), days[0] if days else None, days[-1] if days else None, depth[0] if depth else 0, depth[len(depth) // 2] if depth else 0, depth[-1] if depth else 0))
            gaps = [(a, b) for a, b in zip(days, days[1:]) if (datetime.fromisoformat(b) - datetime.fromisoformat(a)).days > 4]
            r.log("gaps of more than four days in the ledger: %s" % gaps[:12])

        # ------------------------------------------------------------------ 2. the structural model
        r.section("2. The structural model it reads (cds-monitor)")
        cm, size, when = tryget("data/cds-monitor.json")
        if cm:
            snc = cm.get("single_name_cds") or {}
            r.log("data/cds-monitor.json: %d bytes, written %s, generated %s, schema %s, errors %s" % (size, when, cm.get("generated_at"), cm.get("schema_version"), short(cm.get("errors"), 200)))
            r.log("single names: %s" % {k: (len(v) if isinstance(v, list) else type(v).__name__) for k, v in snc.items()})
            for g in ("corporates", "banks"):
                rows = snc.get(g) or []
                if rows:
                    r.log("  %s row: %s" % (g, short(rows[0], 900)))
            r.log("method: %s" % short(cm.get("method"), 900))
            r.log("sources: %s" % short(cm.get("sources"), 600))

        # ------------------------------------------------------------------ 3. the CDS desk
        r.section("3. The CDS desk: real single-name prints from the public tape")
        cds, size, when = tryget("data/cds-desk.json")
        corp_rows = []
        if cds:
            r.log("data/cds-desk.json: %d bytes, written %s, version %s, as of %s, tape %s" % (size, when, cds.get("version"), cds.get("as_of"), short(cds.get("source"), 300)))
            for g, blk in (cds.get("groups") or {}).items():
                rows = blk.get("rows") or []
                r.log("  %-12s tracked %s, priced %s, liquid %s, unpriced %s, dormant %s | median %s bp | rows carry: %s" % (
                    g, blk.get("n_tracked"), blk.get("n_priced"), blk.get("n_liquid"), blk.get("n_unpriced"), blk.get("n_dormant"), blk.get("median_bp"), sorted(rows[0].keys()) if rows else None))
                if g in ("us_corp", "global_corp"):
                    corp_rows += [dict(x, _g=g) for x in rows]
                    sec = {}
                    for x in rows:
                        sec[x.get("sector")] = sec.get(x.get("sector"), 0) + 1
                    r.log("    sectors: %s" % dict(sorted(sec.items(), key=lambda kv: -kv[1])))
                    stale = sorted(x.get("stale_days") or 0 for x in rows)
                    r.log("    days since the last priced print: median %s, 90th pct %s, max %s | with a 1-month change: %d, a 3-month change: %d | priced on 20+ days in a year: %d" % (
                        stale[len(stale) // 2] if stale else None, stale[int(len(stale) * 0.9)] if stale else None, stale[-1] if stale else None,
                        sum(1 for x in rows if x.get("chg_1m_bp") is not None), sum(1 for x in rows if x.get("chg_3m_bp") is not None), sum(1 for x in rows if (x.get("n_days_priced_1y") or 0) >= 20)))
                    r.log("    a row (spark left out): %s" % short({k: v for k, v in rows[0].items() if k not in ("spark", "curve_last")}, 900))
                    r.log("    names: %s" % "; ".join("%s %s" % (x.get("name"), x.get("spread_bp")) for x in rows[:400]))
                    r.log("    unpriced names: %s" % "; ".join(str(x.get("name")) for x in (blk.get("unpriced") or [])[:150]))
        bank, bsize, bwhen = tryget("data/warm/cds/bank.json.gz")
        if bank:
            series = bank.get("series") or {}
            r.log("the desk's bank: %d bytes unzipped, %d series, %d days %s .. %s | keys %s" % (bsize, len(series), len(bank.get("days") or []), (bank.get("days") or [None])[0], (bank.get("days") or [None])[-1], sorted(bank.keys())))
            k0 = next(iter(series), None)
            if k0:
                r.log("  a series %r: %s" % (k0, short(series[k0], 500)))
            em = bank.get("entity_map") or {}
            r.log("  entity map: %d entries; one: %s" % (len(em), short(next(iter(em.items()), None), 400)))

        # ------------------------------------------------------------------ 4. the corporate bonds desk
        r.section("4. The corporate bonds desk: bonds inside funds, each with a printed yield")
        cb, size, when = tryget("data/corp-bonds.json")
        bonds, issuers = [], []
        if cb:
            r.log("data/corp-bonds.json: %d bytes, written %s, generated %s | top keys %s" % (size, when, cb.get("generated_at"), sorted(cb.keys())))
            iss = cb.get("issues") or {}
            r.log("issues block: %s" % short({k: (v if not isinstance(v, (list, dict)) else ("[%d]" % len(v) if isinstance(v, list) else "{%d}" % len(v))) for k, v in iss.items()}, 900))
            bonds, issuers, sectors = iss.get("bonds") or [], iss.get("issuers") or [], iss.get("sectors") or []
            r.log("funds: %s" % short(iss.get("funds"), 1500))
            if bonds:
                r.log("a bond: %s" % short(bonds[0], 900))
                r.log("bond fields filled (of %d): %s" % (len(bonds), {k: sum(1 for b in bonds if b.get(k) is not None) for k in sorted({k for b in bonds[:400] for k in b})}))
                r.log("currencies: %s | books: %s" % (dict(sorted(((c, sum(1 for b in bonds if b.get("ccy") == c)) for c in {b.get("ccy") for b in bonds}), key=lambda kv: -kv[1])[:8]),
                                                     {c: sum(1 for b in bonds if c in (b.get("books") or [b.get("book")])) for c in sorted({x for b in bonds for x in (b.get("books") or [b.get("book")]) if x})}))
            if issuers:
                r.log("an issuer: %s" % short(issuers[0], 900))
                r.log("issuers %d | with a yield %d | with a ticker field %d" % (len(issuers), sum(1 for x in issuers if x.get("rate") is not None), sum(1 for x in issuers if x.get("ticker"))))
            if sectors:
                r.log("industries (%d): %s" % (len(sectors), short([{k: s.get(k) for k in ("sector", "n", "issuers", "rate", "chg", "dur")} for s in sectors], 3500)))
            r.log("notes: %s" % short(iss.get("notes") or iss.get("method"), 1500))
            r.log("sleeves (ICE BofA from FRED): %s" % short([{k: s.get(k) for k in ("fred", "label", "last", "chg_1m", "date")} for s in (cb.get("sleeves") or [])], 2500))
        for pfx in ("data/corp-bonds", "data/warm/corp-bonds", "data/warm/cds/", "credit/", "data/warm/treasury-par/"):
            rows = ls(pfx, 400)
            r.log("s3 %-26s %4d objects, %8.1f MB | %s" % (pfx, len(rows), sum(x[1] for x in rows) / 1e6, "; ".join("%s (%d KB, %s)" % (x[0][len(pfx):][:46], x[1] // 1024, str(x[2])[:10]) for x in rows[:10])))

        # ------------------------------------------------------------------ 5. what the warehouse holds for every listed company
        r.section("5. The warehouse: every listed company")
        par, psize, pwhen = tryget("data/warm/treasury-par/curve.json.gz")
        if par:
            rows = par.get("rows") or par.get("curve") or []
            r.log("Treasury par curve: written %s, keys %s, %s rows, last %s" % (pwhen, sorted(par.keys())[:12], len(rows) if isinstance(rows, list) else "-", short(rows[-1] if isinstance(rows, list) and rows else None, 400)))
        try:
            import bank_enrich
            idx = bank_enrich.index() if hasattr(bank_enrich, "index") else None
            fields = sorted((idx or {}).get("fields") or (idx or {}).get("columns") or [])
            r.log("factor book: %d fields: %s" % (len(fields), " ".join(fields)))
            want = [f for f in ("symbol", "name", "sector", "industry", "is_common", "security_type", "mcap", "price", "total_debt", "net_debt", "debt_to_equity", "interest_coverage", "cash",
                                "ev", "ret_21d", "ret_63d", "vol_63d", "vol_252d", "beta", "altman_z", "piotroski", "exchange", "country", "dollar_vol_20d", "cik", "isin", "cusip") if f in fields]
            rows = bank_enrich.rows(fields=want) or []
            common = [x for x in rows if x.get("is_common")]
            r.log("factor book rows %d, common stocks %d | read fields %s" % (len(rows), len(common), want))
            r.log("common stocks with: %s" % {f: sum(1 for x in common if x.get(f) not in (None, "")) for f in want})
            big = [x for x in common if (x.get("mcap") or 0) >= 3e8]
            r.log("common stocks worth $300M or more: %d; with a sector %d; with total debt %d" % (len(big), sum(1 for x in big if x.get("sector")), sum(1 for x in big if x.get("total_debt") not in (None, ""))))
            secs = {}
            for x in big:
                secs[x.get("sector")] = secs.get(x.get("sector"), 0) + 1
            r.log("their sectors: %s | distinct industries: %d" % (dict(sorted(secs.items(), key=lambda kv: -kv[1])), len({x.get("industry") for x in big})))
            sym = bank_enrich.factor("AAPL") if hasattr(bank_enrich, "factor") else None
            r.log("factor book document of AAPL: %s" % short(sym, 2600))
            # how many CDS desk companies and bond issuers carry a name the factor book also carries
            def norm(s):
                s = re.sub(r"[^A-Z0-9 ]", " ", str(s or "").upper())
                s = re.sub(r"\b(INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|LTD|LIMITED|PLC|SA|NV|AG|LLC|LP|THE|HOLDINGS|HOLDING|GROUP|CLASS [A-C]|COMMON STOCK|ORDINARY SHARES)\b", " ", s)
                return " ".join(s.split())
            book = {}
            for x in sorted(common, key=lambda x: -(x.get("mcap") or 0)):
                book.setdefault(norm(x.get("name")), x.get("symbol"))
            hit = [(x.get("name"), book[norm(x.get("name"))]) for x in corp_rows if norm(x.get("name")) in book]
            r.log("CDS desk companies whose name is a listed company's, exactly after normalising: %d of %d: %s" % (len(hit), len(corp_rows), short(hit[:60], 1800)))
            r.log("  not matched by exact name: %s" % short([x.get("name") for x in corp_rows if norm(x.get("name")) not in book][:150], 2600))
            ihit = [(x.get("issuer") or x.get("name"), book[norm(x.get("issuer") or x.get("name"))]) for x in issuers if norm(x.get("issuer") or x.get("name")) in book]
            r.log("bond issuers whose name is a listed company's, exactly after normalising: %d of %d: %s" % (len(ihit), len(issuers), short(ihit[:50], 1500)))
            r.log("  not matched by exact name (first 120): %s" % short([x.get("issuer") or x.get("name") for x in issuers if norm(x.get("issuer") or x.get("name")) not in book][:120], 2600))
        except Exception as e:  # noqa: BLE001
            r.warn("factor book: %s: %s" % (type(e).__name__, str(e)[:300]))
        for key in ("data/warm/history-book/balance-sheet-statement/quarter/AAPL.json.gz", "data/warm/history-book/balance-sheet-statement/annual/AAPL.json.gz"):
            d, n, w = tryget(key)
            if d:
                rows = d.get("rows") if isinstance(d, dict) else d
                last = rows[0] if rows and (rows[0].get("date") or "") > (rows[-1].get("date") or "") else rows[-1] if rows else {}
                r.log("%s: written %s, %d rows %s .. %s | debt fields: %s" % (key.split("history-book/")[1], str(w)[:10], len(rows or []), (rows or [{}])[-1].get("date"), (rows or [{}])[0].get("date"),
                                                                           {k: last.get(k) for k in last if re.search(r"debt|Debt|liabilit|Liabilit|cash|Cash|totalAssets|totalEquity|totalStockholders", k)}))
        hb = ls("data/warm/history-book/balance-sheet-statement/quarter/", 30000)
        r.log("history book, quarterly balance sheets: %d symbols" % len(hb))
        uni, n, w = tryget("data/universe.json")
        if uni:
            st = uni.get("stocks") or []
            r.log("data/universe.json (the board's price leg): written %s, %d stocks, a row: %s" % (w, len(st), short(st[0] if st else None, 500)))
        r.kv(board_issuers=len((doc or {}).get("names") or []), cds_companies=len(corp_rows), bond_issuers=len(issuers), bonds=len(bonds))
        r.ok("AUDIT COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
