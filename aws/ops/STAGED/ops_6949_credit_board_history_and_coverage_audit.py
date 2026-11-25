"""ops 6949 - READ-ONLY audit before Credit Before Equity 3.0: what exists to cover more companies and to say what a stock did next.

Khalid 2026-11-25: "increase the coverage to include a lot more companies and think of different ways and addition to improve
this engine so that it can predict what would happen to the stock of these companies. also make sure all the data from the
engines is viewable".

Nothing is invoked and nothing is written. It answers, from the warehouse and from one provider the bond desk already reads:

  1  the bond desk's feed: which earlier holdings dates does it list, and does it still serve a month-end older than its list
     (the board can only measure "what the stock did after credit moved" over the days it has spreads for)
  2  the CDS desk's bank: how many names, how many priced days each, since when
  3  the factor book: which columns hold what a credit analyst reads (debt, cash, coverage, leverage, Altman Z), and for how
     many common stocks
  4  the Treasury curve's depth and the price bank's depth
  5  government curves for the euro, sterling, Canadian and Australian dollar books the board does not read today
  6  the board's own ledger days, and the other blocks of the bond desk document
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-corp-bonds" / "source")]
LIVE = "justhodl-dashboard-live"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"       # the bond desk's own


def get_json(s3, key):
    raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw), len(raw)


def fetch(url, timeout=60):
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json,text/csv,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def main():
    import boto3
    from ops_report import report
    import bank_enrich
    import corp_bonds_issues as BI
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    with report("ops_6949_credit_board_history_and_coverage_audit") as r:
        r.heading("ops 6949 - audit for Credit Before Equity 3.0: history and coverage the warehouse can give (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. The bond desk's feed: dates listed, and whether an older month-end is still served")
        funds = {f["id"]: f for f in BI.FUNDS}
        for fid in ("USIG", "USHY", "CEMB"):
            try:
                blob = fetch(BI._api_url(funds[fid]))
                rows, as_of, dates = BI.parse_api(json.loads(blob.decode("utf-8")), funds[fid])
                r.log("%s latest: %d rows as of %s, %.1f MB; dates listed (%d): %s" % (fid, len(rows), as_of, len(blob) / 1e6, len(dates), dates))
                pts = json.loads(blob.decode("utf-8"))["componentsByNameMap"]["holdings"]["containersByNameMap"]["all"]["dataPointsByNameMap"]
                if fid == "USIG":
                    r.log("USIG data points the feed carries (%d): %s" % (len(pts), sorted(pts)))
            except Exception as e:  # noqa: BLE001
                r.log("%s latest: FAILED %s %s" % (fid, type(e).__name__, str(e)[:160]))
            time.sleep(2.5)
        for ymd in ("20260930", "20251231", "20251128", "20250630", "20241231", "20231229", "20221230", "20201231"):
            try:
                blob = fetch(BI._api_url(funds["USHY"], ymd))
                rows, as_of, dates = BI.parse_api(json.loads(blob.decode("utf-8")), funds["USHY"])
                with_y = sum(1 for x in rows if x.get("ytw") is not None or x.get("ytm") is not None)
                r.log("USHY asked %s -> %d rows as of %s (%d with a yield, %d with a price, %d with a duration), %.1f MB" % (
                    ymd, len(rows), as_of, with_y, sum(1 for x in rows if x.get("price") is not None), sum(1 for x in rows if x.get("dur") is not None), len(blob) / 1e6))
            except Exception as e:  # noqa: BLE001
                r.log("USHY asked %s -> FAILED %s %s" % (ymd, type(e).__name__, str(e)[:160]))
            time.sleep(2.5)

        r.section("2. The CDS desk's bank")
        try:
            bank, size = get_json(s3, "data/warm/cds/bank.json.gz")
            days = bank.get("days") or []
            series = bank.get("series") or {}
            r.log("bank: %.1f MB gzipped, keys %s, %d days %s .. %s, %d series" % (size / 1e6, sorted(bank), len(days), days[:1], days[-1:], len(series)))
            first = next(iter(series.items()), None)
            if first:
                r.log("one series (%s): %s" % (first[0], json.dumps(first[1])[:600]))
            meta = bank.get("meta") or {}
            m1 = next(iter(meta.items()), None)
            if m1:
                r.log("one meta (%s): %s" % (m1[0], json.dumps(m1[1])[:400]))
            counts = []
            for key, s in series.items():
                pts = s if isinstance(s, (list, dict)) else []
                n = len(pts)
                counts.append((n, key))
            counts.sort(reverse=True)
            r.log("series by number of points: over 400: %d, 200-400: %d, 60-200: %d, under 60: %d; most %s; least %s" % (
                sum(1 for n, _ in counts if n > 400), sum(1 for n, _ in counts if 200 < n <= 400), sum(1 for n, _ in counts if 60 < n <= 200), sum(1 for n, _ in counts if n <= 60), counts[:5], counts[-3:]))
        except Exception as e:  # noqa: BLE001
            r.log("CDS bank: FAILED %s %s" % (type(e).__name__, str(e)[:200]))
        try:
            hist, size = get_json(s3, "data/cds-desk-history.json")
            names = hist.get("names")
            r.log("public history: %.1f MB, keys %s, names %s" % (size / 1e6, sorted(hist), len(names) if hasattr(names, "__len__") else names))
            one = (list(names.items())[0] if isinstance(names, dict) else (names[0] if names else None))
            r.log("one name: %s" % json.dumps(one)[:500])
        except Exception as e:  # noqa: BLE001
            r.log("public CDS history: FAILED %s %s" % (type(e).__name__, str(e)[:200]))

        r.section("3. The factor book: what a credit analyst reads, and for how many common stocks")
        idx = bank_enrich.index() or {}
        cols = sorted(idx.get("columns") or [])
        r.log("factor book columns (%d): %s" % (len(cols), cols))
        want = [c for c in cols if any(w in c.lower() for w in ("debt", "cash", "interest", "coverage", "altman", "piotroski", "leverage", "ebitda", "current_ratio", "quick", "liab", "equity", "fcf",
                                                                 "free_cash", "net_income", "revenue", "mcap", "ev", "volatil", "beta", "rating", "score", "short", "dcf", "target", "ret_", "margin", "roic",
                                                                 "payout", "dividend", "buyback", "shares", "insider", "inst"))]
        base = [c for c in ("name", "sector", "industry", "mcap", "is_common", "is_adr", "is_otc", "is_etf", "is_fund", "is_shell", "is_preferred", "is_warrant_unit", "is_debt_or_structured", "security_type") if c in cols]
        rows = bank_enrich.rows(fields=sorted(set(base + want))) or []
        common = [x for x in rows if x.get("is_common") and not x.get("is_otc") and not x.get("is_etf") and not x.get("is_fund") and not x.get("is_shell")]
        r.log("factor book: %d rows, %d listed common stocks (not OTC), %d of them ADRs, as of %s" % (len(rows), len(common), sum(1 for x in common if x.get("is_adr")), json.dumps(bank_enrich.as_of())[:300]))
        for c in want:
            have = [x[c] for x in common if x.get(c) is not None]
            nums = sorted(v for v in have if isinstance(v, (int, float)))
            mid = nums[len(nums) // 2] if nums else None
            r.log("  %-34s %5d of %d common (%.0f%%) middle %s, p5 %s, p95 %s" % (c, len(have), len(common), 100.0 * len(have) / max(len(common), 1), mid, nums[len(nums) // 20] if nums else None, nums[-max(len(nums) // 20, 1)] if nums else None))
        for t in ("ORCL", "CRWV", "INTC", "AAPL", "PLUG"):
            x = next((y for y in rows if y.get("symbol") == t), None)
            r.log("  %s: %s" % (t, json.dumps({k: x.get(k) for k in want} if x else None)[:1400]))
        big = sorted(common, key=lambda x: -(x.get("mcap") or 0))
        r.log("common stocks by market value: over $10B %d, $2-10B %d, $300M-2B %d, under $300M %d" % (
            sum(1 for x in big if (x.get("mcap") or 0) >= 10e9), sum(1 for x in big if 2e9 <= (x.get("mcap") or 0) < 10e9), sum(1 for x in big if 3e8 <= (x.get("mcap") or 0) < 2e9), sum(1 for x in big if (x.get("mcap") or 0) < 3e8)))

        r.section("4. The Treasury curve's depth and the price bank's depth")
        try:
            par, size = get_json(s3, "data/warm/treasury-par/curve.json.gz")
            days = sorted(par.get("rows") or {})
            r.log("Treasury par curve: %d days %s .. %s (%.2f MB), tenors %s" % (len(days), days[:1], days[-1:], size / 1e6, sorted((par["rows"][days[-1]] or {}))))
        except Exception as e:  # noqa: BLE001
            r.log("par curve: FAILED %s %s" % (type(e).__name__, str(e)[:200]))
        for t in ("ORCL", "SPY", "CRWV", "HYG"):
            try:
                doc, size = get_json(s3, "data/warm/polygon-full/eod/%s.json.gz" % t)
                bars = doc.get("bars") or doc.get("rows") or doc.get("results") or []
                r.log("price bank %s: %.0f KB, keys %s, %d bars, first %s, last %s" % (t, size / 1e3, sorted(doc)[:10], len(bars), json.dumps(bars[0])[:120] if bars else None, json.dumps(bars[-1])[:120] if bars else None))
            except Exception as e:  # noqa: BLE001
                r.log("price bank %s: FAILED %s %s" % (t, type(e).__name__, str(e)[:160]))

        r.section("5. Government curves for the books the board does not read")
        for prefix in ("data/warm/treasury-par/", "data/warm/ecb-yc/", "data/warm/ecb/", "data/warm/boe/", "data/warm/boc/", "data/warm/rba/", "data/warm/bundesbank/", "data/warm/sovereign/", "data/warm/yield", "data/warm/gov"):
            try:
                got = s3.list_objects_v2(Bucket=LIVE, Prefix=prefix, MaxKeys=12)
                keys = [(o["Key"], o["Size"], o["LastModified"].strftime("%Y-%m-%d")) for o in got.get("Contents") or []]
                r.log("%-28s %s%s" % (prefix, keys[:8], " ..." if got.get("IsTruncated") else ""))
            except Exception as e:  # noqa: BLE001
                r.log("%-28s FAILED %s" % (prefix, type(e).__name__))
        top = s3.list_objects_v2(Bucket=LIVE, Prefix="data/warm/", Delimiter="/", MaxKeys=1000)
        r.log("banks under data/warm/ (%d): %s" % (len(top.get("CommonPrefixes") or []), [p["Prefix"][len("data/warm/"):-1] for p in top.get("CommonPrefixes") or []]))
        for key in ("data/bond-warroom.json", "data/sovereign-yields.json", "data/global-yields.json"):
            try:
                doc, size = get_json(s3, key)
                r.log("%s: %.0f KB, keys %s" % (key, size / 1e3, sorted(doc)[:30]))
                for k in ("yields", "sovereign", "markets", "curves", "countries"):
                    if k in doc:
                        v = doc[k]
                        r.log("   %s: %s" % (k, json.dumps(v if not isinstance(v, (list, dict)) else (v[:2] if isinstance(v, list) else dict(list(v.items())[:2])))[:900]))
            except Exception as e:  # noqa: BLE001
                r.log("%s: %s" % (key, type(e).__name__))

        r.section("6. The board's ledger, and the rest of the bond desk document")
        got = s3.list_objects_v2(Bucket=LIVE, Prefix="credit/cbe-spreads/")
        keys = [(o["Key"].rsplit("/", 1)[-1][:10], o["Size"]) for o in got.get("Contents") or []]
        r.log("ledger days (%d): %s" % (len(keys), keys))
        try:
            doc, size = get_json(s3, "data/corp-bonds.json")
            r.log("bond desk document: %.1f MB, keys %s" % (size / 1e6, {k: (type(v).__name__, len(v) if hasattr(v, "__len__") else v) for k, v in doc.items()}))
            iss = doc.get("issues") or {}
            r.log("issues block keys: %s" % {k: (type(v).__name__, len(v) if hasattr(v, "__len__") else v) for k, v in iss.items()})
            if isinstance(iss.get("bonds"), list) and iss["bonds"]:
                r.log("one bond: %s" % json.dumps(iss["bonds"][0])[:500])
        except Exception as e:  # noqa: BLE001
            r.log("bond desk document: FAILED %s %s" % (type(e).__name__, str(e)[:200]))
        r.ok("AUDIT COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
