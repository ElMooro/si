"""ops 6745 - READ-ONLY census of price discontinuities in the leaders universe's bars (Khalid 2026-11-04: "build a new
engine called leaders ...").

The dry run (ops 6744) printed one-year returns that no instrument can have had as itself: a ticker that used to belong
to another security (the bank keys bars by ticker, and a ticker can change hands), a spin-off, a corporate action
Polygon's split file does not carry. A horizon measured across such a break is not this instrument's return. This probe
measures the problem before the rule is written:

  1. every close-to-close jump of x1.8 / x0.56 or more inside the 600 sessions the engine reads, for every leader, every
     catalog fund and SPY: the ratio, the shares and dollars traded before and after, the calendar gap, whether
     Polygon records a split there, and what the engine's rebase does with it today;
  2. calendar gaps of more than 10 days between consecutive bars (a ticker that was dormant);
  3. the factor book's ipo_date against the first bar: bars that predate the listing belong to someone else;
  4. the monthly closes of the suspects the dry run surfaced.

Nothing is written to S3.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
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
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=48)
SUSPECTS = ["BNY", "SPCX", "JAN", "CTVA", "MRNA", "UWMC", "BSX", "CSGP", "FLUT", "ORCL", "SNDK", "BKNG", "SBS", "MDLN", "SKHY", "HNGE", "MRSH", "SKYD", "HTO", "PTRN", "SW", "B"]


def days(a, b):
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6745_leaders_discontinuity_probe") as r:
        r.heading("ops 6745 - discontinuities in the leaders universe's bars (read-only) %s" % now.isoformat(timespec="minutes"))
        import bank_enrich
        import industry_etf_catalog
        import lambda_function as lf
        import leaders_tape as T
        book = bank_enrich.rows(fields=lf.BOOK_FIELDS + ["ipo_date"])
        leaders, industries, stats = lf.select_leaders(book)
        ipo = {lf.canon(x.get("symbol")): x.get("ipo_date") for x in book if x.get("ipo_date")}
        name = {x["ticker"]: x["name"] for x in leaders}
        mcap = {x["ticker"]: x["mcap"] for x in leaders}
        funds = sorted(industry_etf_catalog.ALL_ETFS)
        tickers = list(dict.fromkeys([x["ticker"] for x in leaders] + funds + [lf.BENCH]))
        r.log("leaders %d | funds %d | tickers %d | leaders with an ipo_date %d" % (len(leaders), len(funds), len(tickers), sum(1 for x in leaders if ipo.get(x["ticker"]))))

        def fetch(t):
            try:
                return t, lf.read_bars(s3, t)
            except Exception as exc:  # noqa: BLE001
                return t, "%s: %s" % (type(exc).__name__, str(exc)[:80])
        bars_by = {}
        with ThreadPoolExecutor(max_workers=16) as pool:
            for t, b in pool.map(fetch, tickers):
                bars_by[t] = b
        bad = {t: b for t, b in bars_by.items() if isinstance(b, str)}
        r.log("read errors: %s" % json.dumps(bad))
        bars_by = {t: b for t, b in bars_by.items() if isinstance(b, list) and b}
        first_day = min(b[0]["date"] for b in bars_by.values())
        last_day = max(b[-1]["date"] for b in bars_by.values())
        rec, n_windows = lf.recorded_splits(s3, first_day, last_day, set(bars_by))
        r.log("bars for %d tickers, %s .. %s | split windows read %d | tickers with a recorded split in range %d" % (len(bars_by), first_day, last_day, n_windows, len(rec)))

        r.section("1. close-to-close jumps of x1.8 / x0.56 or more (raw bars)")
        n_jump = n_applied = 0
        census = []
        for t in tickers:
            b = bars_by.get(t)
            if not b:
                continue
            _, events = T.rebase(b, rec.get(t))
            applied = {e["date"]: e for e in events}
            for i in range(1, len(b)):
                pc, c = float(b[i - 1]["c"]), float(b[i]["c"])
                if pc <= 0 or c <= 0:
                    continue
                ratio = c / pc
                if 0.56 < ratio < 1.8:
                    continue
                n_jump += 1
                sh_b, sh_a = avg([x.get("v") for x in b[max(0, i - 5):i]]), avg([x.get("v") for x in b[i:i + 5]])
                dv_b = avg([(x.get("v") or 0) * float(x["c"]) for x in b[max(0, i - 20):i]])
                dv_a = avg([(x.get("v") or 0) * float(x["c"]) for x in b[i:i + 20]])
                near = [e for e in (rec.get(t) or []) if abs(days(e["date"], b[i]["date"])) <= 5]
                ap = applied.get(b[i]["date"])
                n_applied += 1 if ap else 0
                census.append((t, b[i]["date"], ratio, bool(ap)))
                r.log("%-6s %-26s %s x%.4f  %.4f -> %.4f | gap %2dd | shares 5d before %s after %s (x%.2f) | $vol 20d before %s after %s (x%.2f) | recorded %s | rebase %s | session %d of %d | ipo %s" % (
                    t, (name.get(t) or ("fund" if t in funds else ""))[:26], b[i]["date"], ratio, pc, c, days(b[i - 1]["date"], b[i]["date"]),
                    "%.0f" % sh_b if sh_b else "-", "%.0f" % sh_a if sh_a else "-", (sh_a / sh_b) if sh_a and sh_b else float("nan"),
                    "%.0f" % dv_b if dv_b else "-", "%.0f" % dv_a if dv_a else "-", (dv_a / dv_b) if dv_a and dv_b else float("nan"),
                    json.dumps(near) if near else "none", json.dumps(ap) if ap else "LEFT AS PRINTED", i, len(b), ipo.get(t)))
                r.log("        closes around: %s" % " ".join("%s:%.2f" % (x["date"][5:], float(x["c"])) for x in b[max(0, i - 4):i + 5]))
        r.log("jumps %d | rebased by the engine %d | left as printed %d | tickers %d" % (n_jump, n_applied, n_jump - n_applied, len({x[0] for x in census})))

        r.section("2. calendar gaps of more than 10 days between consecutive bars")
        n_gap = 0
        for t in tickers:
            b = bars_by.get(t)
            if not b:
                continue
            for i in range(1, len(b)):
                g = days(b[i - 1]["date"], b[i]["date"])
                if g > 10:
                    n_gap += 1
                    r.log("%-6s %-26s %s -> %s (%d days) | close %.4f -> %.4f (x%.3f) | session %d of %d | first bar %s" % (
                        t, (name.get(t) or "")[:26], b[i - 1]["date"], b[i]["date"], g, float(b[i - 1]["c"]), float(b[i]["c"]), float(b[i]["c"]) / float(b[i - 1]["c"]) if float(b[i - 1]["c"]) else float("nan"), i, len(b), b[0]["date"]))
        r.log("gaps %d" % n_gap)

        r.section("3. bars that predate the factor book's ipo_date")
        n_pre = 0
        for t in tickers:
            b = bars_by.get(t)
            d = ipo.get(t)
            if not b or not d or len(str(d)) < 10:
                continue
            d = str(d)[:10]
            pre = sum(1 for x in b if x["date"] < d)
            if pre:
                n_pre += 1
                first_after = next((x for x in b if x["date"] >= d), None)
                last_before = [x for x in b if x["date"] < d][-1]
                r.log("%-6s %-26s ipo_date %s | %d of %d bars predate it (first bar %s) | last before %s %.4f | first on/after %s | mcap %s" % (
                    t, (name.get(t) or "")[:26], d, pre, len(b), b[0]["date"], last_before["date"], float(last_before["c"]),
                    ("%s %.4f" % (first_after["date"], float(first_after["c"]))) if first_after else "none", "%.1fB" % (mcap[t] / 1e9) if t in mcap else "-"))
        r.log("tickers with bars before their ipo_date: %d" % n_pre)
        short = sorted((len(b), t) for t, b in bars_by.items() if len(b) < 600)
        r.log("tickers with fewer than 600 bars (%d): %s" % (len(short), " ".join("%s:%d" % (t, n) for n, t in short)))

        r.section("4. the suspects' closes every 21 sessions (newest last)")
        for t in SUSPECTS:
            b = bars_by.get(t)
            if not b:
                r.log("%-6s no bars" % t)
                continue
            idx = list(range(len(b) - 1, -1, -21))[::-1]
            r.log("%-6s %-24s ipo %s first bar %s n %d | %s" % (t, (name.get(t) or "")[:24], ipo.get(t), b[0]["date"], len(b), " ".join("%s:%.2f" % (b[i]["date"][2:], float(b[i]["c"])) for i in idx)))
        r.kv(jumps=str(n_jump), rebased=str(n_applied), gaps=str(n_gap), pre_ipo=str(n_pre), tickers=str(len(bars_by)))
        r.ok("discontinuity census complete (nothing written)")


if __name__ == "__main__":
    main()
    sys.exit(0)
