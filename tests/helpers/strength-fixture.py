"""Build tests/fixtures/strength-desk.json.gz from real feeds: a cut of the live whole-market table and of the forming-bar
file of the same instant, with the reads the ENGINE's own code makes of them (wide.py) - so tests/strength-desk.test.js
holds the page's port of that arithmetic to the engine's figures, digit for digit, on real rows.

    python3.12 tests/helpers/strength-fixture.py <strength-wide.json> <today.json> [<strength.json>]

The table is cut to every S&P 500 member plus every fourth other row (set-aside rows, rows with an announced split and
the names the tests look up are always kept); the forming-bar file to the tickers kept. Because the cut changes the
population, `last_session` is recomputed by wide.last_read over the cut - the same function the engine's own tests
hold equal to what build() stores. The desk artifact, when given, is cut to what the page's tearsheet test reads.
"""
import gzip
import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "aws" / "lambdas" / "justhodl-strength" / "source"), str(ROOT / "aws" / "shared")]
sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402
import wide as W  # noqa: E402

KEEP = {"AAPL", "MSFT", "NVDA", "WMT", "COST", "JPM", "XOM", "FICO", "MKC", "SNDK", "BRK.B", "QQQ", "IWM", "TLT", "GLD", "XLP", "XLU", "XLK", "SMH", "HYG", "IBIT", "VXX", "VIXY", "SH", "RWM", "JPST", "BTAL"}


def main():
    wide_path, today_path = sys.argv[1], sys.argv[2]
    doc = json.loads(Path(wide_path).read_text(encoding="utf-8"))
    today = json.loads(Path(today_path).read_text(encoding="utf-8"))
    cols = doc["cols"]
    ix = {c: i for i, c in enumerate(cols)}
    ahead = doc.get("splits_ahead") or {}
    rows = []
    for i, raw in enumerate(doc["rows"]):
        t = raw[ix["t"]]
        if raw[ix["sp"]] or raw[ix["why"]] or t in ahead or t in KEEP or i % 4 == 0:
            rows.append(raw)
    cut = dict(doc, rows=rows)
    cut["splits_ahead"] = {t: v for t, v in ahead.items() if any(r[0] == t for r in rows)}
    cut["last_session"] = W.last_read(cut, class_labels=LF.CLASS_LABEL)
    cut["fixture"] = {"rows_kept": len(rows), "rows_published": len(doc["rows"]), "rule": "every S&P 500 member, every set-aside row, every row with an announced split, a few named tickers, and every fourth other row"}
    kept = {r[0] for r in rows} | {doc["benchmark"]["ticker"]}
    tcut = dict(today, rows={t: v for t, v in (today.get("rows") or {}).items() if t in kept})
    tcut["n"] = len(tcut["rows"])
    live = W.live_read(cut, tcut, class_labels=LF.CLASS_LABEL)
    j = W.live_join(cut, tcut)
    sample = {}
    if j:
        by = {r["t"]: r for r in j["rows"]}
        for t in sorted(KEEP | set(cut["splits_ahead"]) | set((live or {}).get("unconfirmed") or [])):
            r = by.get(t)
            if r:
                sample[t] = {"ret": r["ret"], "unconfirmed": r["unconfirmed"], "split": r["split"], "live": r["live"], "prev": r.get("prev"), "has_bar": bool(r.get("bar"))}
    out = {"note": "real feeds, cut: see tests/helpers/strength-fixture.py", "class_labels": LF.CLASS_LABEL,
           "wide": cut, "today": tcut, "expect": {"last": cut["last_session"], "live": live, "spy": (j or {}).get("spy"), "join": sample}}
    if len(sys.argv) > 3:
        desk = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
        out["desk"] = {k: desk.get(k) for k in ("contract", "version", "as_of", "generated_at", "classes")}
    body = json.dumps(out, separators=(",", ":"), default=str).encode("utf-8")
    dst = ROOT / "tests" / "fixtures" / "strength-desk.json.gz"
    dst.write_bytes(gzip.compress(body, 9, mtime=0))
    print("%s: %d rows of %d, %d forming bars, %d bytes json, %d gzipped; live read %s" % (
        dst.relative_to(ROOT), len(rows), len(doc["rows"]), len(tcut["rows"]), len(body), dst.stat().st_size, "yes" if live else "none (no session after the table's)"))


if __name__ == "__main__":
    main()
