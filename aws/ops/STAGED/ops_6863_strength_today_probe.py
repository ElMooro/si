"""ops 6863 - READ-ONLY probe for the strength desk's "holding up today" read (re-runnable).

Khalid 2026-11-20 12:44 ET: "Strength vs S&P 500 needs improvement like today everything is dumping hard however i cant
see which stocks and etfs are showing strength and dumping less than sp500".  Before anything is built this reads what
the desk has to work with, right now, on a dump day:

  1. data/strength.json - the nightly artifact the page reads (version, session, rows, what a row carries).
  2. data/warm/polygon-full/today.json.gz - today's forming day bar for every US stock and ETF (the 5-minute lane),
     and how the public /data route serves it to a browser (status, type, edge TTL, size).
  3. the factor book - the identity of every listed security (the whole-market universe a wide read would draw).
  4. data/stock-volatility.json - last night's whole-market table (its last close is today's prior close).
  5. the join of 2 x 4: today's move of every measured stock and ETF against SPY's - up / flat / down less / down
     more - so the build is designed on today's real distribution.

Nothing is written.  Compact packs of the feeds are embedded for the page's offline harness.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import sys
import time
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import bank_enrich  # noqa: E402
import fmp_bank  # noqa: E402

BUCKET = "justhodl-dashboard-live"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 JustHodlBot/ops-6863"
FLAT = 0.25


def get(s3, key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    raw = o["Body"].read()
    if key.endswith(".gz") or o.get("ContentEncoding") == "gzip":
        try:
            raw = gzip.decompress(raw)
        except OSError:
            pass
    return json.loads(raw), o


def embed(r, name, obj):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")
    blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
    r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN %s" % name)
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FEED-PACK-GZ-B64-END -->")
    r.log("embedded %s: %d bytes json, %d b64" % (name, len(body), len(blob)))


def outcome(ret, spy):
    if ret > FLAT:
        return "up"
    if ret >= -FLAT:
        return "flat"
    return "down_less" if ret > spy else "down_more"


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=60))
    with report("ops_6863_strength_today_probe") as r:
        r.heading("ops 6863 - strength desk, what a dump day can be read from, %s (read-only)" % now.isoformat(timespec="minutes"))

        r.section("1. data/strength.json")
        doc, o = get(s3, "data/strength.json")
        rows = doc.get("rows") or []
        r.log("version %s contract %s as_of %s generated %s, %d bytes, last modified %s" % (doc.get("version"), doc.get("contract"), doc.get("as_of"), doc.get("generated_at"), o["ContentLength"], o["LastModified"].isoformat()))
        r.log("universe %s" % json.dumps(doc.get("universe"))[:600])
        r.log("top-level keys: %s" % ", ".join(sorted(doc.keys())))
        r.log("row keys: %s" % ", ".join(sorted(rows[0].keys())) if rows else "no rows")
        r.log("benchmark %s" % json.dumps(doc.get("benchmark"))[:400])
        th = doc.get("thesis") or {}
        ls = th.get("last_session") or {}
        r.log("thesis keys: %s" % ", ".join(sorted(th.keys())))
        r.log("thesis verdict: %s" % (th.get("verdict") or "")[:1200])
        r.log("last session %s spy %s dump %s rally %s" % (ls.get("date"), ls.get("spy_ret_pct"), ls.get("dump"), ls.get("rally")))
        r.log("boards keys: %s; breadth %s" % (", ".join(sorted((doc.get("boards") or {}).keys())), json.dumps((doc.get("boards") or {}).get("breadth"))))
        r.log("classes %s" % json.dumps(doc.get("classes"))[:500])
        r.log("splits: %s" % json.dumps({k: v for k, v in (doc.get("splits") or {}).items() if k != "refused" and k != "note"}))
        r.log("sample row AAPL: %s" % json.dumps(next((x for x in rows if x["ticker"] == "AAPL"), None))[:2600])
        r.log("sample row XLU: %s" % json.dumps(next((x for x in rows if x["ticker"] == "XLU"), None))[:1800])
        embed(r, "strength", doc)

        r.section("2. today's forming bars")
        today, o2 = get(s3, "data/warm/polygon-full/today.json.gz")
        trow = today.get("rows") or {}
        r.log("session %s as_of %s generated %s n %d fields %s; object %d bytes, encoding %s, cache %s, last modified %s (%.0fs ago)" % (
            today.get("session"), today.get("as_of"), today.get("generated_at"), len(trow), today.get("fields"), o2["ContentLength"], o2.get("ContentEncoding"), o2.get("CacheControl"),
            o2["LastModified"].isoformat(), (now - o2["LastModified"]).total_seconds()))
        for t in ("SPY", "QQQ", "IWM", "AAPL", "NVDA", "TLT", "GLD", "XLU", "BRK.B", "BF.B"):
            r.log("  %-6s %s" % (t, trow.get(t)))
        for url in ("https://justhodl.ai/data/warm/polygon-full/today.json.gz", "https://justhodl.ai/data/warm/polygon-full/today-manifest.json", "https://justhodl.ai/data/strength.json"):
            try:
                t0 = time.time()
                req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip"})
                with urllib.request.urlopen(req, timeout=40) as resp:
                    raw = resp.read()
                    hd = {k: resp.headers.get(k) for k in ("Content-Type", "Content-Encoding", "Cache-Control", "X-Edge-TTL", "X-Edge-Cache", "Last-Modified", "ETag", "Age")}
                body = gzip.decompress(raw) if hd.get("Content-Encoding") == "gzip" else raw
                try:
                    j = json.loads(body)
                    shape = "json ok: %s" % (", ".join(sorted(j.keys()))[:160])
                except Exception as exc:  # noqa: BLE001
                    shape = "NOT json after one decode (%s); first bytes %r" % (type(exc).__name__, body[:12])
                r.log("  GET %s -> %d wire bytes, %d decoded, %.2fs, %s | %s" % (url.replace("https://justhodl.ai", ""), len(raw), len(body), time.time() - t0, json.dumps(hd), shape))
            except Exception as exc:  # noqa: BLE001
                r.warn("  GET %s failed: %s: %s" % (url, type(exc).__name__, str(exc)[:160]))

        r.section("3. the factor book")
        man = bank_enrich.manifest(max_age_s=0) or {}
        r.log("manifest: %s" % json.dumps({k: man.get(k) for k in ("as_of", "generated_at", "version", "symbols", "updated_at")}, default=str)[:600])
        idx = bank_enrich.index(max_age_s=0) or {}
        cols = sorted(idx.get("columns") or [])
        r.log("index: %d symbols; identity columns %s" % (len(idx.get("symbols") or []), ", ".join(sorted((idx.get("identity") or {}).keys()))))
        r.log("matrix columns (%d): %s" % (len(cols), ", ".join(cols)[:3000]))
        book = bank_enrich.rows(fields=["mcap", "dollar_vol_20d", "ipo_date", "exchange", "price", "close", "avg_volume", "ret_1d", "ret_5d", "ret_21d", "ret_63d", "ret_126d", "ret_252d", "beta"])
        r.log("book rows %d; keys of one: %s" % (len(book), ", ".join(sorted(book[0].keys())) if book else None))
        r.log("AAPL in the book: %s" % json.dumps(next((x for x in book if x.get("symbol") == "AAPL"), None), default=str)[:900])
        r.log("SPY in the book: %s" % json.dumps(next((x for x in book if x.get("symbol") == "SPY"), None), default=str)[:900])
        try:
            sp = fmp_bank.walker("sp500-constituent") or []
            r.log("S&P 500 constituents: %d rows; keys %s" % (len(sp), ", ".join(sorted(sp[0].keys())) if sp else None))
        except Exception as exc:  # noqa: BLE001
            sp = []
            r.warn("sp500 walker: %s" % exc)
        spset = {str(x.get("symbol") or "").upper().replace("-", ".") for x in sp}

        r.section("4. last night's whole-market table (stock-volatility)")
        vol, o4 = get(s3, "data/stock-volatility.json")
        vrows = [dict(zip(vol["cols"], x)) for x in vol["rows"]]
        r.log("version %s as_of %s generated %s; %d rows; %d bytes; cols %s" % (vol.get("version"), vol.get("as_of"), vol.get("generated_at"), len(vrows), o4["ContentLength"], ", ".join(vol["cols"])))
        eod_man, _ = get(s3, "data/warm/polygon-full/eod/manifest.json")
        r.log("EOD bank manifest: last_session %s, updated %s, tickers %s" % (eod_man.get("last_session"), eod_man.get("updated_at"), eod_man.get("tickers")))

        r.section("5. today against the S&P 500, every measured stock and ETF")
        spy = trow.get("SPY")
        spy_prev = next((x["px"] for x in vrows if x["t"] == "SPY"), None) or (doc.get("benchmark") or {}).get("px")
        if not spy or not spy_prev or vol.get("as_of") >= today.get("session"):
            r.warn("no join: SPY forming bar %s, prior close %s, table as_of %s vs forming session %s" % (spy, spy_prev, vol.get("as_of"), today.get("session")))
            r.kv(joined="0")
            return
        spy_ret = (spy[3] / spy_prev - 1) * 100
        r.log("SPY prior close %s (table as of %s), forming bar %s -> today %+.2f%%, from the open %+.2f%%, range position %.0f%%, vs VWAP %+.2f%%" % (
            spy_prev, vol.get("as_of"), spy, spy_ret, (spy[3] / spy[0] - 1) * 100, 100 * (spy[3] - spy[2]) / max(1e-9, spy[1] - spy[2]), (spy[3] / spy[5] - 1) * 100))
        strength_by = {x["ticker"]: x for x in rows}
        joined, miss = [], 0
        for x in vrows:
            b = trow.get(x["t"])
            if not b or not x.get("px"):
                miss += 1
                continue
            ret = (b[3] / x["px"] - 1) * 100
            s = strength_by.get(x["t"]) or {}
            joined.append({"t": x["t"], "name": x["name"], "kind": x["kind"], "sector": x["sector"], "industry": x["industry"], "mcap": x["mcap"], "adv": x["adv"], "pc": x["px"], "bar": b[:6],
                           "ret": round(ret, 2), "x": round(ret - spy_ret, 2), "out": outcome(ret, spy_ret), "aside": x.get("aside"), "why": x.get("why"), "sp500": x["t"] in spset,
                           "strength": s.get("strength"), "dump_held": ((s.get("capture") or {}).get("63") or {}).get("dump_held_pct")})
        r.log("joined %d of %d table rows (%d without a forming bar or a prior close)" % (len(joined), len(vrows), miss))
        big_moves = sorted([j for j in joined if abs(j["ret"]) > 40], key=lambda j: -abs(j["ret"]))
        r.log("moves beyond 40%% today (%d): %s" % (len(big_moves), "; ".join("%s %+.1f%% $%.0fM %s" % (j["t"], j["ret"], (j["mcap"] or 0) / 1e6, j["why"] or "") for j in big_moves[:40])))

        def dist(group, label):
            n = len(group)
            if not n:
                r.log("  %-34s none" % label)
                return
            c = {k: sum(1 for j in group if j["out"] == k) for k in ("up", "flat", "down_less", "down_more")}
            med = sorted(j["ret"] for j in group)[n // 2]
            r.log("  %-34s n %5d  up %5.1f%%  flat %5.1f%%  down less %5.1f%%  down more %5.1f%%  held up %5.1f%%  median %+.2f%%" % (
                label, n, 100 * c["up"] / n, 100 * c["flat"] / n, 100 * c["down_less"] / n, 100 * c["down_more"] / n, 100 * (n - c["down_more"]) / n, med))
        st = [j for j in joined if j["kind"] == "stock"]
        et = [j for j in joined if j["kind"] == "etf"]
        dist(st, "all stocks")
        dist([j for j in st if (j["mcap"] or 0) >= 1e9], "stocks $1B+")
        dist([j for j in st if (j["mcap"] or 0) >= 10e9], "stocks $10B+")
        dist([j for j in st if j["sp500"]], "S&P 500 members")
        dist(et, "ETFs")
        ranked = [j for j in st if j["strength"] is not None]
        ranked.sort(key=lambda j: j["strength"])
        k = max(1, len(ranked) // 10)
        dist(ranked[-k:], "desk rank: strongest tenth (stocks)")
        dist(ranked[:k], "desk rank: weakest tenth (stocks)")
        for sec in sorted({j["sector"] or "Unclassified" for j in st}):
            dist([j for j in st if (j["sector"] or "Unclassified") == sec and (j["mcap"] or 0) >= 1e9], "$1B+ " + sec[:28])
        b1 = [j for j in st if (j["mcap"] or 0) >= 1e9 and not j["aside"]]
        r.log("up the most, $1B+ stocks: %s" % ", ".join("%s %+.2f%%" % (j["t"], j["ret"]) for j in sorted(b1, key=lambda j: -j["ret"])[:40]))
        r.log("down the most, $1B+ stocks: %s" % ", ".join("%s %+.2f%%" % (j["t"], j["ret"]) for j in sorted(b1, key=lambda j: j["ret"])[:25]))
        r.log("up the most, $10B+ stocks: %s" % ", ".join("%s %+.2f%%" % (j["t"], j["ret"]) for j in sorted([j for j in b1 if j["mcap"] >= 10e9], key=lambda j: -j["ret"])[:40]))
        r.log("S&P 500 members up on the day: %s" % ", ".join("%s %+.2f%%" % (j["t"], j["ret"]) for j in sorted([j for j in st if j["sp500"] and j["ret"] > FLAT], key=lambda j: -j["ret"])[:80]))
        e1 = [j for j in et if (j["adv"] or 0) >= 5e6 and not j["aside"]]
        r.log("ETFs ($5M+ a day) up the most: %s" % ", ".join("%s %+.2f%%" % (j["t"], j["ret"]) for j in sorted(e1, key=lambda j: -j["ret"])[:40]))
        strong_hold = sorted([j for j in st if (j["strength"] or 0) >= 80 and j["out"] != "down_more"], key=lambda j: -j["x"])
        r.log("desk rank 80+ and holding up (%d): %s" % (len(strong_hold), ", ".join("%s %+.2f%% (rank %.0f)" % (j["t"], j["ret"], j["strength"]) for j in strong_hold[:50])))
        embed(r, "today", today)
        embed(r, "joined", {"spy_ret": round(spy_ret, 3), "spy_prev": spy_prev, "spy_bar": spy, "session": today.get("session"), "as_of": today.get("as_of"), "table_as_of": vol.get("as_of"), "rows": joined})
        r.kv(joined=str(len(joined)), spy_today="%+.2f%%" % spy_ret, stocks=str(len(st)), etfs=str(len(et)), forming_as_of=str(today.get("as_of")))
        r.ok("READ: SPY %+.2f%% on %s as of %s; %d stocks and %d ETFs joined to today's forming bar; nothing written" % (spy_ret, today.get("session"), today.get("as_of"), len(st), len(et)))


if __name__ == "__main__":
    main()
    sys.exit(0)
