"""ops 6973 - dry run of the five engine changes made for the chart tabs. Nothing is written and no Lambda is invoked.

Khalid 2026-11-26: "audit their engine before you add them and see if you can expand it to cover more data". The audit
(ops 6971 / 6972) led to five changes, each run here with the engine's own code on the runner against live inputs:

  1. catalyst calendar 2.0   build() with the kept-document sources: economic releases, the wider earnings calendar,
                             IPOs, lock-ups, quarter-end, market holidays, monthly OPEX, VIX expiry; the Fed's revised
                             2027 schedule; contract awards given a date. Compared with the live calendar.
  2. options, 155 names      each of the 97 added names asked once for an options contract; then the whole run with
                             the two writes captured instead of sent.
  3. insider price check     the live insider document's rows screened against the factor book's tape.
  4. on-chain join           the live on-chain document joined with the kept CryptoQuant document.
  5. plumbing count          the live document's own rows counted against its header.

Safe to run again.
"""
from datetime import date, datetime, timezone
from pathlib import Path
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
NAME = "ops_6973_chart_tab_engines_dry_run"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"


def engine(name, alias):
    """The engine's own module, loaded from the repository under a private name."""
    src = ROOT / "aws" / "lambdas" / name / "source"
    sys.path.insert(0, str(src))
    try:
        spec = importlib.util.spec_from_file_location(alias, str(src / "lambda_function.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        pass


class Capture:
    """Stands in for an S3 client: reads pass through, writes are kept here and never sent."""
    def __init__(self, real):
        self.real, self.puts = real, {}

    def put_object(self, **kw):
        body = kw.get("Body")
        self.puts[kw.get("Key")] = body if isinstance(body, (bytes, bytearray)) else str(body).encode()
        return {}

    def delete_object(self, **kw):
        return {}

    def __getattr__(self, name):
        return getattr(self.real, name)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=120))
    now = datetime.now(timezone.utc)
    fails = []

    def get(key):
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())

    with report(NAME) as r:
        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)

        r.heading("6973 - dry run of the engine changes for the chart tabs (nothing written), %s" % now.isoformat(timespec="minutes"))

        # ---------------------------------------------------------------- 1
        r.section("1. Catalyst calendar 2.0")
        try:
            C = engine("justhodl-catalyst-calendar", "eng_catalyst")
            live = get("data/catalyst-calendar.json")
            t0 = time.time()
            doc = C.build()
            r.log("built in %.1f s | events %d (ahead %d, past %d) against the live calendar's %d" % (time.time() - t0, doc["n_events"], doc["n_ahead"], doc["n_past"], live.get("n_events", 0)))
            r.log("by type, ahead: " + json.dumps(doc["by_type_ahead"]))
            r.log("by type, live calendar: " + json.dumps(live.get("by_type")))
            for s in doc["sources"]:
                r.log("   source %-15s %4d events | %s%s%s" % (s["source"], s["events"], s["document"] or "", (" | %s h old" % s["hours_old"]) if s["hours_old"] is not None else "", (" | " + s["note"]) if s["note"] else ""))
            ev = doc["events"]
            keys = [(e.get("date"), e.get("type"), e.get("title"), e.get("ticker")) for e in ev]
            ck(len(keys) == len(set(keys)), "no event is listed twice (%d events)" % len(keys))
            ck(all(e.get("impact") in ("HIGH", "MEDIUM", "LOW") and e.get("title") and e.get("type") for e in ev), "every event has a type, a title and an impact")
            ck(all(isinstance(e.get("days_to"), int) for e in ev) and ev == sorted(ev, key=lambda e: (e["date"], e.get("type", ""))), "every event is dated and the list is in date order")
            old = {(e.get("date"), e.get("type"), e.get("title")) for e in live.get("events", []) if e.get("type") not in ("EARNINGS_ACTUAL",)}
            new = {(e.get("date"), e.get("type"), e.get("title")) for e in ev}
            gone = sorted(k for k in old if k not in new and k[0] >= date.today().isoformat())
            ck(not gone, "every event ahead on the live calendar is still there (%d missing%s)" % (len(gone), (": " + json.dumps(gone[:4])) if gone else ""))
            ck(doc["by_type_ahead"].get("ECON", 0) >= 20, "economic releases are on the calendar (%d)" % doc["by_type_ahead"].get("ECON", 0))
            ck(doc["n_ahead"] >= 3 * max(1, sum(1 for e in live.get("events", []) if (e.get("days_to") or -1) >= 0)), "at least three times as many events ahead as the live calendar (%d against %d)" % (doc["n_ahead"], sum(1 for e in live.get("events", []) if (e.get("days_to") or -1) >= 0)))
            fomc = [e for e in ev if e["type"] == "FOMC"]
            r.log("FOMC rows: " + json.dumps([(e["date"], e["title"]) for e in fomc]))
            hol = [e for e in ev if e["type"] == "MARKET_HOLIDAY"]
            r.log("market holidays: " + json.dumps([(e["date"], e["title"], e.get("time")) for e in hol][:8]))
            gov = [e for e in ev if e["type"] == "GOV_CONTRACT"]
            r.log("contract awards: %d | date taken from: %s | sample %s" % (len(gov), json.dumps(sorted({str(e.get("date_basis")) for e in gov})), json.dumps([(e["date"], e.get("ticker"), e["title"]) for e in gov[:3]])))
            for kind in ("ECON", "EARNINGS", "IPO", "LOCKUP", "QUARTER_END", "OPEX", "VIX_EXPIRY"):
                rows = [e for e in ev if e["type"] == kind and e["days_to"] >= 0]
                r.log("   %-12s %3d ahead | first: %s" % (kind, len(rows), json.dumps([(e["date"], e["title"], e.get("subtitle"), e["impact"]) for e in rows[:2]], ensure_ascii=False)[:330]))
            size = len(json.dumps(doc, default=str))
            r.log("document size %s bytes against %s live" % (format(size, ","), format(len(json.dumps(live)), ",")))
        except Exception as exc:  # noqa: BLE001
            ck(False, "catalyst calendar dry run raised %s: %s" % (type(exc).__name__, str(exc)[:300]))

        # ---------------------------------------------------------------- 2
        r.section("2. Options: 155 names")
        try:
            O = engine("justhodl-options-analytics", "eng_options")
            added = O.UNIVERSE[58:]
            exp = O.third_fridays(1)[0]
            r.log("nearest monthly expiry %s | %d names added to the first 58" % (exp, len(added)))
            empty = []
            for t in added:
                try:
                    j = O._safe_http("%s/v3/snapshot/options/%s?expiration_date=%s&limit=1&apiKey=%s" % (O.BASE, t, exp, O.POLY))
                    if not (j or {}).get("results"):
                        empty.append(t)
                except Exception:  # noqa: BLE001
                    empty.append(t)
            ck(len(empty) <= 3, "the added names have listed options at the provider (%d without: %s)" % (len(empty), ", ".join(empty) or "none"))
            cap = Capture(O.S3)
            O.S3 = cap
            t0 = time.time()
            out = O.lambda_handler({}, None)
            took = time.time() - t0
            doc = json.loads(cap.puts[O.OUT_KEY]) if hasattr(O, "OUT_KEY") and O.OUT_KEY in cap.puts else json.loads(next(v for k, v in cap.puts.items() if k.endswith("options-analytics.json")))
            r.log("whole run in %.1f s | universe %s, analysed %s, not analysed: %s | writes captured, not sent: %s" % (took, doc.get("universe_size"), doc.get("n_analyzed"), ", ".join(doc.get("not_analyzed") or []) or "none", ", ".join("%s (%s bytes)" % (k, format(len(v), ",")) for k, v in cap.puts.items())))
            r.log("distribution " + json.dumps(doc.get("distribution")) + " | status " + str((out or {}).get("statusCode")))
            ck(doc.get("universe_size") == 155 and (doc.get("n_analyzed") or 0) >= 140, "at least 140 of the 155 names analysed (%s)" % doc.get("n_analyzed"))
            ck(took <= 40, "the run stays light for the five-minute lane (%.1f s)" % took)
            fresh = [x for x in doc.get("board", []) if x.get("ticker") in set(added)]
            r.log("added names on the board: %d | sample %s" % (len(fresh), json.dumps([(x["ticker"], x.get("signal"), x.get("gamma_regime"), x.get("net_gex_musd_per_1pct")) for x in fresh[:6]])))
            ck(all(x.get("spot") and x.get("signal") for x in fresh), "every added name on the board has a spot price and a signal")
        except Exception as exc:  # noqa: BLE001
            ck(False, "options dry run raised %s: %s" % (type(exc).__name__, str(exc)[:300]))

        # ---------------------------------------------------------------- 3
        r.section("3. Insider trades: price check against the tape")
        try:
            I = engine("justhodl-insider-trades", "eng_insider")
            live = get("data/insider-trades.json")
            rows = [t for t in (live.get("transactions") or []) + (live.get("sell_transactions") or []) + (live.get("set_aside_as_filed") or []) if isinstance(t, dict) and t.get("ticker")]
            t0 = time.time()
            counted, as_filed, aside, tally = I.screen_against_tape(rows)
            r.log("screened %d rows in %.1f s | %s" % (len(rows), time.time() - t0, json.dumps(tally)))
            for a in aside[:12]:
                r.log("   set aside: %s %s %s shares at %s = $%s | tape %s | %s" % (a["ticker"], a["side"], format(int(a["shares"] or 0), ","), a["price"], format(float(a["value"] or 0), ",.0f"), a["tape_price"], a["reason"]))
            before = I.stats_summary([t for t in rows])
            after = I.stats_summary(counted)
            r.log("buys counted: %d -> %d | total value $%s -> $%s" % (before["total_buys"], after["total_buys"], format(before["total_value_usd"], ",.0f"), format(after["total_value_usd"], ",.0f")))
            ck(tally["tickers_on_the_book"] > 50, "the factor book answered for the tickers (%d)" % tally["tickers_on_the_book"])
            ck(any(a["ticker"] == "LONA" for a in aside), "the LONA line is set aside")
            ck(len(counted) + len(aside) == len(rows) and tally["n_set_aside"] <= 0.01 * len(rows) + 3, "every row is either counted or set aside, and very few are set aside (%d of %d)" % (len(aside), len(rows)))
            ck(after["total_value_usd"] < before["total_value_usd"] - 9.0e9, "the buying total no longer carries the $9.9 billion line")
        except Exception as exc:  # noqa: BLE001
            ck(False, "insider dry run raised %s: %s" % (type(exc).__name__, str(exc)[:300]))

        # ---------------------------------------------------------------- 4
        r.section("4. On-chain: the join with the kept CryptoQuant document")
        try:
            N = engine("justhodl-onchain-ratios", "eng_onchain")
            live = get("data/onchain-ratios.json")
            before = {"cryptoquant" in live, "cryptoquant" in (live.get("eth") or {})}
            doc = N._cq_join(json.loads(json.dumps(live)), s3, LIVE)
            cq, eth = doc.get("cryptoquant") or {}, (doc.get("eth") or {}).get("cryptoquant") or {}
            r.log("live document had the join: %s | after: status %s, written %s, %s metrics kept" % (sorted(before), cq.get("status"), str(cq.get("generated_at"))[:16], cq.get("n_metrics_kept")))
            r.log("Bitcoin core set: " + json.dumps({k: (v.get("value") if isinstance(v, dict) else v) for k, v in cq.items() if k.startswith("btc_")}))
            r.log("Ethereum metrics joined: %d | %s" % (len(eth), json.dumps({k: v.get("value") for k, v in list(eth.items())[:8]})))
            ck(bool(cq) and sum(1 for k, v in cq.items() if k.startswith("btc_") and isinstance(v, dict)) >= 5, "the Bitcoin core set is joined again")
            ck(len(eth) >= 10 and all(v.get("label") for v in eth.values()), "Ethereum has its metrics (%d)" % len(eth))
        except Exception as exc:  # noqa: BLE001
            ck(False, "on-chain dry run raised %s: %s" % (type(exc).__name__, str(exc)[:300]))

        # ---------------------------------------------------------------- 5
        r.section("5. Plumbing: the indicator count")
        try:
            live = get("data/plumbing-stress.json")
            raw = live.get("raw_indicators") or {}
            n = len(raw)
            layers = live.get("layers") or {}
            per = sum((v or {}).get("n_indicators", 0) for v in (layers.values() if isinstance(layers, dict) else layers))
            r.log("live header says %s indicators, %s with data | the document carries %d rows, its layers add to %d" % (live.get("n_indicators"), live.get("n_with_data"), n, per))
            ck(n == per and n >= (live.get("n_with_data") or 0), "the rows and the layers agree on the count the new header will print (%d)" % n)
        except Exception as exc:  # noqa: BLE001
            ck(False, "plumbing check raised %s: %s" % (type(exc).__name__, str(exc)[:300]))

        r.kv(failed_checks=len(fails))
        if fails:
            r.fail("%d check(s) failed: %s" % (len(fails), "; ".join(fails)[:600]))
        else:
            r.ok("all checks passed; nothing written, nothing invoked")


if __name__ == "__main__":
    main()
