"""ops 6951 - READ-ONLY: the credit board rebuilt at every kept month-end, and what each stock did afterwards (the study's panel).

Runs the engine's own build_panel() on the runner against the real warehouse: the bond desk's dated holdings under
credit/cbe-holdings/ (ops 6950), the Treasury curve of each day, the factor book's listings, the price bank's whole history.
Nothing is invoked and nothing is written. The panel leaves in this report as a fixture so the study's arithmetic can be
written and tested against the real rows; a first plain reading of it is printed so the design starts from facts.

Safe to run again.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
STAGING = ROOT / "aws" / "ops" / "staging" / "cbe3"                 # the 3.0 source before it is deployed; the engine's own directory once it is
SRC = STAGING if (STAGING / "lambda_function.py").exists() else ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(SRC)]


def embed(r, name, blob):
    text = base64.b64encode(blob).decode("ascii")
    r.log("fixture %s: %d bytes" % (name, len(blob)))
    r.lines.append("<!-- FIXTURE %s %d bytes" % (name, len(blob)))
    r.lines.extend(text[i:i + 16000] for i in range(0, len(text), 16000))
    r.lines.append("FIXTURE-END -->")


def main():
    import boto3
    from ops_report import report
    import lambda_function as L
    import cbe_study as S
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    with report("ops_6951_credit_board_history_panel") as r:
        r.heading("ops 6951 - the credit board at every kept month-end and what the stocks did next (read-only), %s" % now.isoformat(timespec="minutes"))
        built = L.build_panel(s3, now=now, log=r.log)
        panel, meta, hist = built["panel"], built["meta"], built["history"]
        S.with_industry(panel)
        r.log("meta: %s" % json.dumps({k: v for k, v in meta.items() if k != "month_ends"})[:600])
        per = Counter(x["day"] for x in panel)
        r.log("rows a month-end: %s" % " ".join("%s:%d" % (d[2:7], per[d]) for d in sorted(per)))
        r.log("history: %d companies with a spread at some month-end, %d of them at the last" % (len(hist["companies"]), sum(1 for v in hist["companies"].values() if v[-1] is not None)))
        full = [x for x in panel if x.get("e1") is not None]
        r.log("rows with a next-month return: %d over %d month-ends; with three months: %d; stocks that stopped trading inside the month: %d" % (
            len(full), len({x["day"] for x in full}), sum(1 for x in panel if x.get("e3") is not None), sum(1 for x in panel if x.get("end1"))))

        def line(label, rows):
            a, b = S.describe(rows, "1"), S.describe(rows, "3")
            if not a.get("n"):
                r.log("  %-44s no rows" % label)
                return
            r.log("  %-44s n %6d in %2d months | next month vs market: mean %6s median %6s lagged %5s%% | month by month %6s t %5s | vs industry %6s | 3 months: mean %6s t %5s vs industry %6s" % (
                label, a["n"], a["months"], a["mean"], a["median"], a["lagged_pct"], a["monthly_mean"], a["t"], a["vs_industry"], b.get("mean"), b.get("t"), b.get("vs_industry")))

        r.section("A first reading (stock return less SPY's; entry one session after the holdings day)")
        line("every row", full)
        for book in ("ig", "hy", "em"):
            line("book %s" % book, [x for x in full if x["book"] == book])
        r.log("by the board's nine cells (credit past its book / stock, both over the month before):")
        for cell in ("worse/up", "worse/flat", "worse/down", "flat/down", "flat/flat", "flat/up", "better/down", "better/flat", "better/up"):
            line(cell, [x for x in full if x["cell"] == cell])
        line("no cell (no month's change)", [x for x in full if x["cell"] is None])
        r.log("by the lead the board calls:")
        for sig in ("CREDIT_LEADS_DOWN", "CREDIT_LEADS_UP", "NONE", "INSUFFICIENT_HISTORY"):
            line(sig, [x for x in full if x["sig"] == sig])
        r.log("by stress rank:")
        for lo, hi in ((0, 20), (20, 40), (40, 60), (60, 80), (80, 90), (90, 95), (95, 100.01)):
            line("stress %g to %g" % (lo, hi), [x for x in full if x["stress"] is not None and lo <= x["stress"] < hi])
        r.log("distressed (1,000 bp or more):")
        line("distressed", [x for x in full if x["dist"]])
        r.log("by the month's change past its book (bp), high yield only:")
        for lo, hi in ((-1e9, -50), (-50, -20), (-20, -5), (-5, 5), (5, 20), (20, 50), (50, 150), (150, 1e9)):
            line("x1 %s to %s" % (lo if lo > -1e8 else "-inf", hi if hi < 1e8 else "inf"), [x for x in full if x["book"] == "hy" and x["x1"] is not None and lo <= x["x1"] < hi])
        r.log("the same, investment grade:")
        for lo, hi in ((-1e9, -15), (-15, -5), (-5, 5), (5, 15), (15, 40), (40, 1e9)):
            line("x1 %s to %s" % (lo if lo > -1e8 else "-inf", hi if hi < 1e8 else "inf"), [x for x in full if x["book"] == "ig" and x["x1"] is not None and lo <= x["x1"] < hi])
        r.log("by year:")
        for year in sorted({x["day"][:4] for x in full}):
            line("leads down in %s" % year, [x for x in full if x["sig"] == "CREDIT_LEADS_DOWN" and x["day"][:4] == year])
            line("leads up in %s" % year, [x for x in full if x["sig"] == "CREDIT_LEADS_UP" and x["day"][:4] == year])
        by_t = defaultdict(int)
        for x in panel:
            by_t[x["t"]] += 1
        r.log("companies in the panel: %d; in every month-end: %d; for a year or less: %d" % (len(by_t), sum(1 for n in by_t.values() if n >= len(per)), sum(1 for n in by_t.values() if n <= 12)))

        r.section("Government curves the warehouse banks (for bonds not in US dollars) - a look only")
        try:
            got, token, keys = None, None, []
            while True:
                kw = {"Bucket": L.BUCKET, "Prefix": "data/warm/official-yields/"}
                if token:
                    kw["ContinuationToken"] = token
                got = s3.list_objects_v2(**kw)
                keys += [(o["Key"], o["Size"]) for o in got.get("Contents") or []]
                token = got.get("NextContinuationToken")
                if not token:
                    break
            r.log("official-yields: %d files" % len(keys))
            for key, size in sorted(keys):
                if not key.endswith(".json"):
                    r.log("  %s (%d bytes)" % (key, size))
                    continue
                try:
                    doc = json.loads(s3.get_object(Bucket=L.BUCKET, Key=key)["Body"].read())
                    obs = [o for o in (doc.get("observations") or []) if isinstance(o, dict) and o.get("value") is not None]
                    r.log("  %-34s %5d points %s .. %s last %s | %s" % (key.rsplit("/", 1)[-1], len(obs), obs[0]["date"] if obs else "-", obs[-1]["date"] if obs else "-",
                                                                     obs[-1]["value"] if obs else "-", str(doc.get("title") or doc.get("id") or "")[:70]))
                except Exception as e:                                               # a look only: a file that does not parse is reported, not fatal
                    r.log("  %s unreadable: %s" % (key, str(e)[:80]))
            tops = s3.list_objects_v2(Bucket=L.BUCKET, Prefix="data/warm/", Delimiter="/")
            r.log("data/warm/ lanes: %s" % " ".join(sorted(x["Prefix"][len("data/warm/"):].rstrip("/") for x in tops.get("CommonPrefixes") or [])))
        except Exception as e:
            r.warn("the curve look failed: %s" % str(e)[:160])

        embed(r, "cbe-panel", gzip.compress(json.dumps({"meta": meta, "table": S.to_table(panel)}, separators=(",", ":")).encode(), 9))
        r.kv(month_ends=len(per), rows=len(panel), with_next_month=len(full), companies=len(by_t))
        r.ok("PANEL BUILT: %d rows over %d month-ends; nothing was invoked or written" % (len(panel), len(per)))


if __name__ == "__main__":
    main()
