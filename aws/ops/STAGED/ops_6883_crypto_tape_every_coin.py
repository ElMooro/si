"""ops 6883 - READ-ONLY: the crypto tape the chart reads, for every banked coin, after the same-coin rule and the forming bar.

ops 6881 (before): the outside history joined onto the banked bars was another coin for ARB, TON, AERO and JUP, and 22 coins
had no bar for today.  The data proxy now joins an outside series only when it is the same coin and takes the days after the
bank's last completed day from Coinbase Exchange, then Binance's public market-data host.  For all banked coins this reads
the proxy's /ohlc answer and checks it against the bank and against Coinbase's own latest daily candle:

  - every banked day on the tape carries the banked close (nothing overwrote the bank);
  - the newest bar is today's (UTC) and within 3% of Coinbase's current candle where Coinbase lists the coin;
  - no day-to-day move on the last five bars beyond what the bank and the venue themselves show (the 99% drop is gone);
  - which source supplied history and today, and what was refused.
Writes nothing; no key is used.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6768", str(Path(__file__).resolve().parent / "ops_6768_live_data_freshness_probe.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)
ROOT_KEY = "data/warm/katlin/crypto-bars/"


def day_of(t):
    try:
        t = float(t)
        if t > 1e12:
            t /= 1000.0
        return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")
    except Exception:  # noqa: BLE001
        return None


def judge(bank, tape, today, coinbase_today):
    """bank: {day: close}; tape: [(day, close)] ascending.  -> (verdict, notes)."""
    notes = []
    if not tape:
        return "NO TAPE", notes
    by_day = dict(tape)
    wrong = [d for d, c in bank.items() if d in by_day and abs(by_day[d] / c - 1.0) > 1e-9]
    if wrong:
        notes.append("%d banked days overwritten" % len(wrong))
    last_day, last_close = tape[-1]
    bank_last = max(bank) if bank else None
    if last_day < today:
        notes.append("ends %s" % last_day)
    if coinbase_today and last_day == today and abs(last_close / coinbase_today - 1.0) > 0.03:
        notes.append("today %.4g vs Coinbase %.4g" % (last_close, coinbase_today))
    jump = 0.0
    seg = tape[-6:]
    for (d0, c0), (d1, c1) in zip(seg, seg[1:]):
        if c0 > 0:
            jump = max(jump, abs(c1 / c0 - 1.0))
    if jump > 0.5:
        notes.append("a %.0f%% move on the last bars" % (jump * 100))
    if wrong or jump > 0.5 or any("vs Coinbase" in n for n in notes):
        return "WRONG", notes
    if last_day < today:
        return "BEHIND" if bank_last and last_day >= bank_last else "WRONG", notes
    return "CURRENT", notes


def selftest():
    bank = {"2026-11-19": 100.0, "2026-11-20": 101.0}
    ok = [("2026-11-18", 99.0), ("2026-11-19", 100.0), ("2026-11-20", 101.0), ("2026-11-21", 102.0)]
    assert judge(bank, ok, "2026-11-21", 102.5)[0] == "CURRENT"
    assert judge(bank, ok[:-1], "2026-11-21", 102.5) == ("BEHIND", ["ends 2026-11-20"])
    assert judge(bank, ok[:-1] + [("2026-11-21", 0.4)], "2026-11-21", None)[0] == "WRONG"           # the 99% drop
    assert judge(bank, ok[:-1] + [("2026-11-21", 110.0)], "2026-11-21", 102.5)[0] == "WRONG"        # today is not the venue's
    assert judge(bank, [("2026-11-19", 100.0), ("2026-11-20", 55.0), ("2026-11-21", 56.0)], "2026-11-21", None)[0] == "WRONG"   # a banked day overwritten
    assert judge(bank, [], "2026-11-21", None)[0] == "NO TAPE" and day_of(1795219200) is not None


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name=P.REGION, config=P.CFG)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with report("ops_6883_crypto_tape_every_coin") as r:
        r.heading("ops 6883 - the crypto tape for every banked coin (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        keys, tok = [], None
        while True:
            kw = {"Bucket": P.LIVE, "Prefix": ROOT_KEY, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            res = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in res.get("Contents", [])]
            if not res.get("IsTruncated"):
                break
            tok = res.get("NextContinuationToken")
        coins = sorted(k.split("/")[-1].replace(".json.gz", "") for k in keys)
        r.section("1. Every banked coin through the data proxy's /ohlc (today is %s UTC)" % today)
        r.log("%-8s %-9s %5s  %-11s %-12s | %-11s %-9s %-10s | %s" % ("coin", "verdict", "bars", "newest", "close", "bank last", "history", "today from", "notes / refused"))
        tally, by_forming, by_history, refused_all, behind, wrong = {}, {}, {}, {}, [], []
        for c in coins:
            try:
                doc = json.loads(gzip.decompress(s3.get_object(Bucket=P.LIVE, Key=ROOT_KEY + c + ".json.gz")["Body"].read()))
                bank = {str(x[0])[:10]: float(x[4]) for x in (doc.get("rows") or []) if x and x[4]}
            except Exception as e:  # noqa: BLE001
                r.log("%-8s bank unreadable: %s" % (c, type(e).__name__))
                continue
            st, body, _ = P.get(P.SITE + "/ohlc", params={"ticker": c + "-USD", "span": "day", "mult": 1, "days": 12000})
            body = body if isinstance(body, dict) else {}
            tape = [(day_of(b.get("time")), float(b["close"])) for b in (body.get("bars") or []) if b.get("close")]
            st2, cb, _ = P.get("https://api.exchange.coinbase.com/products/%s-USD/candles" % c, params={"granularity": 86400})
            cb_today = None
            if isinstance(cb, list) and cb and isinstance(cb[0], list) and day_of(cb[0][0]) == today:
                cb_today = float(cb[0][4])
            verdict, notes = judge(bank, tape, today, cb_today)
            tally[verdict] = tally.get(verdict, 0) + 1
            forming, history = body.get("forming_source") or "-", body.get("history_source") or "-"
            by_forming[forming] = by_forming.get(forming, 0) + 1
            by_history[history] = by_history.get(history, 0) + 1
            refused = ["%s(%s days, %s)" % (x.get("source"), x.get("shared_days"), "-" if x.get("median_distance") is None else "%.0f%%" % (x["median_distance"] * 100)) for x in (body.get("outside_refused") or [])]
            for x in (body.get("outside_refused") or []):
                refused_all.setdefault(x.get("source"), []).append(c)
            if verdict == "BEHIND":
                behind.append(c)
            if verdict in ("WRONG", "NO TAPE"):
                wrong.append(c)
            r.log("%-8s %-9s %5d  %-11s %-12.6g | %-11s %-9s %-10s | %s" % (c, verdict, len(tape), tape[-1][0] if tape else "-", tape[-1][1] if tape else 0, max(bank) if bank else "-", history, forming,
                                                                         "; ".join(notes + (["refused " + " ".join(refused)] if refused else []))[:120]))
            time.sleep(0.1)
        r.section("2. Totals")
        r.log("%d coins: %s" % (len(coins), json.dumps(tally, sort_keys=True)))
        r.log("today's bar from: %s" % json.dumps(by_forming, sort_keys=True))
        r.log("older history from: %s" % json.dumps(by_history, sort_keys=True))
        for src, cs in sorted(refused_all.items()):
            r.log("refused as another coin or unverifiable - %s: %d (%s)" % (src, len(cs), " ".join(cs)))
        r.log("tape ends before today (no same-coin venue lists the coin, or the bank stopped): %s" % (" ".join(behind) or "-"))
        r.log("wrong or missing: %s" % (" ".join(wrong) or "none"))
        r.kv(coins=len(coins), current=tally.get("CURRENT", 0), behind=tally.get("BEHIND", 0), wrong=tally.get("WRONG", 0) + tally.get("NO TAPE", 0))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
