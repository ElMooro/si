"""ops 6842 - READ-ONLY: what stands behind the chart's Wyckoff icons, before anything is built (re-runnable).

Khalid 2026-11-19: "now do the same design for climax, capitulation, bc climax, nrspread, sp spring, accumulation
(all these have icons on chart.html) by the way, accumulation, bottom, distribution engines should be built for the
icons there it should show all the corresponding stocks and then give their ui/ux that design".

Audit before building.  The icons SC / Capit / BC / NR / Spring open wyckoff-desk.html tabs and
capitulation-desk.html; Acc / Bot / Dist only mark the symbol on the chart.  This op reads the five feeds those desks
stand on as published (bottom, capitulation-desk, phase-detector, katlin, accumulation-radar), reports their shape,
counts and the wholes a ring could stand on, reads the factor book's identity columns (the universe an all-stock scan
would cover), and embeds trimmed feeds plus the real banked daily bars of a handful of tickers so the scan port and the
sheets are designed and tested on the real thing.  Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=90)
FEEDS = ["data/bottom.json", "data/capitulation-desk.json", "data/phase-detector.json", "data/katlin.json", "data/accumulation-radar.json"]
BARS = ["SPY", "QQQ", "IWM", "NVDA", "AAPL", "TSLA", "INTC", "PFE", "NKE", "BA", "XOM", "SMCI", "MRNA", "PYPL", "DIS", "UNH", "LULU", "ENPH", "PLTR", "CVS"]
EOD_ROOT = "data/warm/polygon-full/eod/"
BOOK_ROOT = "data/warm/factor-book/"


def shape(v, depth=0, cap=3):
    if isinstance(v, dict):
        if depth >= cap:
            return "{%d keys}" % len(v)
        return {k: shape(x, depth + 1, cap) for k, x in list(v.items())[:70]}
    if isinstance(v, list):
        return "[%d] of %s" % (len(v), shape(v[0], depth + 1, cap) if v else "nothing")
    return type(v).__name__ if v is None or isinstance(v, bool) else (repr(v)[:36] if depth else type(v).__name__)


def read(s3, key):
    head = s3.head_object(Bucket=LIVE, Key=key)
    body = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    raw = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
    return json.loads(raw), len(body), len(raw), head["LastModified"].isoformat(timespec="seconds")


def tally(rows, key):
    out = {}
    for x in rows:
        k = key(x)
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


def embed(r, name, doc):
    blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
    r.log("%s embedded: %d chars" % (name, len(blob)))
    r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN %s" % name)
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FEED-PACK-GZ-B64-END -->")


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6842_wyckoff_icons_feed_audit") as r:
        r.heading("ops 6842 - the feeds behind the chart's Wyckoff icons, as published %s (read-only)" % now.isoformat(timespec="minutes"))
        docs = {}
        for key in FEEDS:
            try:
                doc, stored, size, mod = read(s3, key)
            except Exception as exc:  # noqa: BLE001
                r.fail("%s: %s %s" % (key, type(exc).__name__, str(exc)[:160]))
                continue
            docs[key] = doc
            r.section(key)
            r.log("%d bytes stored, %d bytes of JSON, last modified %s; engine %s %s, generated %s, as of %s" % (
                stored, size, mod, doc.get("engine"), doc.get("version"), doc.get("generated_at"), doc.get("as_of") or doc.get("session") or doc.get("session_date")))
            r.log("shape: %s" % json.dumps({k: (shape(v, 1, 2) if not isinstance(v, list) else "[%d] of %s" % (len(v), shape(v[0], 2, 2) if v else "nothing")) for k, v in doc.items()}, default=str)[:3600])

        bottom = docs.get("data/bottom.json") or {}
        board, board_all = bottom.get("board") or [], bottom.get("board_all") or []
        r.section("bottom harvest: the boards the SC / Capit / Bottom / Pump tabs read")
        if board:
            r.log("a board row (chart payload dropped): %s" % json.dumps({k: v for k, v in board[0].items() if k not in ("chart", "bars")}, default=str)[:3000])
            r.log("board %d rows, keys of the largest row: %s" % (len(board), sorted(max(board, key=len).keys())))
            r.log("board by frame: %s; by state: %s; by desk: %s" % (tally(board, lambda x: x.get("frame")), tally(board, lambda x: x.get("state")), tally(board, lambda x: x.get("desk"))))
        if board_all:
            r.log("a board_all row: %s" % json.dumps(board_all[0], default=str)[:1500])
            r.log("board_all %d rows; by frame %s; by state %s; by desk %s" % (len(board_all), tally(board_all, lambda x: x.get("frame")), tally(board_all, lambda x: x.get("state")), tally(board_all, lambda x: x.get("desk"))))
            r.log("board_all rows carrying market cap: %d, sector: %d, company: %d" % (
                len([x for x in board_all if x.get("mcap") or x.get("mcap_usd") or x.get("market_cap")]), len([x for x in board_all if x.get("sector")]), len([x for x in board_all if x.get("company")])))
        for k in ("market", "capitulation", "by_desk", "universe", "counts", "summary"):
            if bottom.get(k) is not None:
                r.log("%s: %s" % (k, json.dumps(bottom.get(k), default=str)[:1400]))

        cap = docs.get("data/capitulation-desk.json") or {}
        r.section("capitulation desk")
        for k, v in cap.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                r.log("%s: %s" % (k, v))
        for k in ("breadth", "regime", "latest", "summary", "follow_stats", "definitions", "counts", "by_desk", "window"):
            if cap.get(k) is not None:
                r.log("%s: %s" % (k, json.dumps(cap.get(k), default=str)[:1800]))
        boards = cap.get("boards") or cap.get("prints") or {}
        if isinstance(boards, dict):
            for name, rows in boards.items():
                if isinstance(rows, list) and rows:
                    r.log("board %s: %d rows; a row: %s" % (name, len(rows), json.dumps(rows[0], default=str)[:900]))
                elif isinstance(rows, dict):
                    r.log("board %s: %s" % (name, json.dumps(shape(rows, 0, 2), default=str)[:900]))

        phase = docs.get("data/phase-detector.json") or {}
        tk = phase.get("tickers") or {}
        r.section("phase detector: the BC / Spring / Accumulation tabs")
        if tk:
            first = next(iter(tk.items()))
            r.log("a ticker: %s -> %s" % (first[0], json.dumps(first[1], default=str)[:1500]))
            ev = {}
            for t, v in tk.items():
                for e in (v or {}).get("events") or []:
                    ev[e.get("type")] = ev.get(e.get("type"), 0) + 1
            r.log("%d tickers; by phase %s; live events by type %s" % (len(tk), tally(tk.values(), lambda x: (x or {}).get("phase")), dict(sorted(ev.items(), key=lambda kv: -kv[1]))))
            r.log("tickers carrying sector %d, name %d, mcap %d" % (len([1 for v in tk.values() if (v or {}).get("sector")]), len([1 for v in tk.values() if (v or {}).get("name")]), len([1 for v in tk.values() if (v or {}).get("mcap") or (v or {}).get("market_cap")])))
        for k in ("phases_all", "summary", "universe", "counts"):
            if phase.get(k) is not None:
                r.log("%s: %s" % (k, json.dumps(shape(phase.get(k), 0, 2), default=str)[:900]))

        kat = docs.get("data/katlin.json") or {}
        picks = kat.get("picks") or []
        r.section("katlin: the Candle spread tab and one leg of Accumulation")
        if picks:
            r.log("a pick: %s" % json.dumps(picks[0], default=str)[:2600])
            sq = [x for x in picks if x and x.get("bbw_pctile") is not None and x.get("bbw_pctile") <= 20 and (x.get("squeeze") or ((x.get("accum_legs") or {}).get("bb_tight") or 0) >= 70)]
            r.log("%d picks; on the Candle spread board %d (squeeze %d, tight %d); by tier %s; by asset class %s; by sector %s" % (
                len(picks), len(sq), len([x for x in sq if x.get("squeeze")]), len([x for x in sq if not x.get("squeeze")]), tally(picks, lambda x: x.get("tier")),
                tally(picks, lambda x: x.get("asset_class") or x.get("class") or x.get("kind")), tally(sq, lambda x: x.get("sector"))))
            r.log("picks carrying market cap: %d" % len([x for x in picks if x.get("mcap") or x.get("mcap_usd") or x.get("market_cap")]))

        radar = docs.get("data/accumulation-radar.json") or {}
        r.section("accumulation radar")
        for k, v in radar.items():
            if isinstance(v, dict):
                r.log("%s: %s" % (k, json.dumps({a: (len(b) if isinstance(b, list) else shape(b, 1, 2)) for a, b in v.items()}, default=str)[:700]))
        acc = radar.get("accumulating") or {}
        for k in ("stocks", "etfs", "countries"):
            if acc.get(k):
                r.log("accumulating.%s: %d; a row: %s" % (k, len(acc[k]), json.dumps(acc[k][0], default=str)[:900]))

        r.section("the universe an all-stock scan would read")
        try:
            idx = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=BOOK_ROOT + "matrix/index.json.gz")["Body"].read()))
            syms = idx.get("symbols") or []
            r.log("factor book: %d symbols, %d columns; index keys %s" % (len(syms), len(idx.get("columns") or []), sorted(idx.keys())))
            ident = idx.get("identity") or idx
            flags = {}
            for name in ("is_common", "is_etf", "is_otc", "is_fund", "is_preferred", "is_warrant_unit", "is_shell", "is_adr", "is_debt_or_structured"):
                col = ident.get(name)
                if isinstance(col, list):
                    flags[name] = sum(1 for v in col if v)
            r.log("identity flags (count true): %s" % json.dumps(flags))
            for name in ("security_type", "exchange", "sector"):
                col = ident.get(name)
                if isinstance(col, list):
                    r.log("%s: %s" % (name, json.dumps(tally(col, lambda v: v))[:900]))
            cols = {}
            for name in ("mcap", "dollar_vol_20d", "close"):
                try:
                    cols[name] = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=BOOK_ROOT + "matrix/cols/%s.json.gz" % name)["Body"].read()))
                except Exception as exc:  # noqa: BLE001
                    r.log("column %s: %s" % (name, type(exc).__name__))
            common, etf, otc = ident.get("is_common") or [], ident.get("is_etf") or [], ident.get("is_otc") or []
            if common and cols.get("dollar_vol_20d"):
                dv, mc = cols["dollar_vol_20d"], cols.get("mcap") or []
                for label, pick in (("common, listed", lambda i: common[i] and not (otc and otc[i])), ("etf", lambda i: bool(etf and etf[i]))):
                    ids = [i for i in range(len(syms)) if pick(i)]
                    tiers = {}
                    for floor in (0, 250e3, 1e6, 5e6, 25e6):
                        tiers["$%s+" % ("%dk" % (floor / 1e3) if floor < 1e6 else "%dM" % (floor / 1e6))] = sum(1 for i in ids if isinstance(dv[i], (int, float)) and dv[i] >= floor)
                    r.log("%s: %d symbols; with a market cap %d; by 20-day dollar volume %s" % (label, len(ids), sum(1 for i in ids if mc and isinstance(mc[i], (int, float)) and mc[i] > 0), json.dumps(tiers)))
        except Exception as exc:  # noqa: BLE001
            r.fail("factor book index: %s %s" % (type(exc).__name__, str(exc)[:160]))
        try:
            man = json.loads(s3.get_object(Bucket=LIVE, Key=EOD_ROOT + "manifest.json")["Body"].read())
            r.log("Polygon EOD bank manifest: %s" % json.dumps({k: (v if not isinstance(v, (list, dict)) else "%s of %d" % (type(v).__name__, len(v))) for k, v in man.items()}, default=str)[:900])
        except Exception as exc:  # noqa: BLE001
            r.log("EOD manifest: %s" % type(exc).__name__)

        r.section("real banked bars for the scan's parity tests")
        bars = {}
        for t in BARS:
            try:
                doc = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=EOD_ROOT + t + ".json.gz")["Body"].read()))
                rows = [x[:6] for x in doc.get("rows") or [] if isinstance(x, list) and len(x) >= 6 and x[0] is not None and x[4] is not None]
                bars[t] = rows
                r.log("%s: %d rows, first %s, last %s; top-level keys %s" % (t, len(rows), rows[0][:1] if rows else None, rows[-1] if rows else None, sorted(k for k in doc.keys() if k != "rows")))
            except Exception as exc:  # noqa: BLE001
                r.log("%s: %s" % (t, type(exc).__name__))

        # trimmed embeds: drop chart payloads, keep everything a sheet or a board could read
        if bottom:
            slim = {k: v for k, v in bottom.items() if k not in ("board", "weekly_live", "top_picks")}
            slim["board"] = [{k: v for k, v in x.items() if k not in ("chart", "bars")} for x in board]
            embed(r, "bottom", slim)
        if cap:
            embed(r, "capitulation-desk", cap)
        if phase:
            embed(r, "phase-detector", phase)
        if kat:
            embed(r, "katlin", kat)
        if radar:
            embed(r, "accumulation-radar", radar)
        if bars:
            embed(r, "bars", bars)
        r.kv(feeds=str(len(docs)), board=str(len(board)), board_all=str(len(board_all)), phase_tickers=str(len(tk)), katlin_picks=str(len(picks)), bars=str(len(bars)))
        r.ok("AUDIT: %d of %d feeds read; bottom board %d / %d, phase %d tickers, katlin %d picks, bars for %d tickers" % (len(docs), len(FEEDS), len(board), len(board_all), len(tk), len(picks), len(bars)))


if __name__ == "__main__":
    main()
    sys.exit(0)
