"""ops 6952 - Credit Before Equity 3.0.0 goes live: the deployed function, one dry run inside Lambda, one publication.

Khalid 2026-11-25: "i like it you should add an icon that would sort distrissed. also you should increase the coverage to
include a lot more companies and think of different ways and addition to improve this engine so that it can predict what
would happen to the stock of these companies. also make sure all the data from the engines is viewable in the iux".

3.0 adds, beside everything 2.1 publishes: bonds in four more currencies read against their own governments (in books of
their own - a company with dollar bonds stays where it was), what each company's own bonds say (maturity wall, coupon,
price, near against far), the nine credit / stock cells, the record of what stocks did after each reading on the board's
own month-ends, the chance of a fall that record supports for each listed company, the day's calls kept to be scored, and
every bond under its company in a document of its own.

  1. Guard: the deployed package's files are this checkout's, byte for byte (cbe_study.py among them), with the memory
     and the time the study needs; the once-a-day rule. If not, nothing is invoked.
  2. One invoke with {"dry_run": true}: the engine builds the whole board inside Lambda and writes nothing (a dry run does
     not start the study). If that fails, or it sees fewer companies than the board has, nothing is published.
  3. One invoke with {}: the evidence has never been written, so this run does the study inside Lambda (the board rebuilt
     at every kept month-end), then publishes. This is a one-off run outside the engine's 22:35 UTC schedule, disclosed to
     Khalid; the schedule itself is untouched and finds the evidence not due.
  4. Read back: the document is 3.0 on the v2 contract; every 2.0 and 2.1 column is where it was; every company that was on
     the board is still on it, in the book it stood in, with the spread it had (when the holdings day is the same); the
     record, the history, the bond document, the calls and the holdings are there and agree with each other; every chance
     of a fall is the record's own table read again here; and the site serves all three documents.

Safe to run again: each run is one more dry run and one more publication of the kind the schedule makes (the study runs
again only when a month has finished).
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import hashlib
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops")]
FN = "justhodl-credit-before-equity"
LIVE = "justhodl-dashboard-live"
OUT_KEY, HIST_KEY, LEDGER_ROOT = "data/credit-before-equity.json", "credit/credit-before-equity-history.json", "credit/cbe-spreads/"
BONDS_OUT_KEY, EVIDENCE_KEY, HISTORY_OUT_KEY = "data/credit-before-equity-bonds.json", "data/credit-before-equity-evidence.json", "data/credit-before-equity-history.json"
CALLS_ROOT, HOLD_ROOT, TRACK_KEY = "credit/cbe-calls/", "credit/cbe-holdings/", "credit/cbe-track.json"
FILES = ("lambda_function.py", "cbe_core.py", "cbe_names.py", "cbe_themes.py", "cbe_study.py", "leaders_tape.py")
COLS_2_0 = ["id", "ticker", "name", "industry", "book", "country", "n_bonds", "value", "yield_pct", "dur", "spread_bp", "d_w1", "d_m1", "x_m1", "pct_m1", "n_m1", "d_m3", "d_ytd",
            "cds_bp", "cds_d1m", "px_m1", "direction", "signal", "distressed", "tie"]
COLS_2_1 = ["pct_ytd", "stress", "rk_level", "rk_year", "rk_month", "theme", "layer", "cds_d3m", "cds_rank", "px_ytd", "pct_w1", "pct_m3"]
BANDS = ((0, 100), (100, 200), (200, 400), (400, 700), (700, 1000), (1000, None))
DOLLAR = ("ig", "hy", "em")
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe"
LEADS = ("CREDIT_LEADS_DOWN", "CREDIT_LEADS_UP")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=930, connect_timeout=30, retries={"max_attempts": 0}))
    ev_ = boto3.client("events", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    fails = []

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return o["Body"].read(), o["LastModified"]

    def parse(raw):
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    def there(key):
        try:
            s3.head_object(Bucket=LIVE, Key=key)
            return True
        except Exception:  # noqa: BLE001
            return False

    def days_of(root):
        out, token = [], None
        while True:
            kw = {"Bucket": LIVE, "Prefix": root}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            out += [o["Key"][len(root):][:10] for o in page.get("Contents") or []]
            token = page.get("NextContinuationToken")
            if not token:
                return sorted(out)

    def site(path):
        req = urllib.request.Request("https://justhodl.ai/" + path + "?probe=%d" % int(time.time()), headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        return json.loads(urllib.request.urlopen(req, timeout=90).read())

    def tail_of(resp, r, words):
        try:
            tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
            for line in tail.splitlines():
                if any(w in line for w in words):
                    r.log("  lambda: %s" % line.strip()[:360])
        except Exception:  # noqa: BLE001
            pass

    with report("ops_6952_credit_before_equity_three_goes_live") as r:
        r.heading("ops 6952 - Credit Before Equity 3.0.0: the record, the chance of a fall, more companies and every figure go live, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard: the deployed function is this checkout's")
        g = lam.get_function(FunctionName=FN)
        cfg = g["Configuration"]
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in FILES:
            hits = [x for x in z.namelist() if x == name]
            same[name] = bool(hits) and z.read(hits[0]) == (SOURCE / name).read_bytes()
        shared = [x for x in z.namelist() if x in ("bank_enrich.py",)]
        r.log("%s updated %s | memory %s MB, timeout %s s | files as in this checkout: %s | shared modules in the package: %s" % (
            FN, (cfg.get("LastModified") or "")[:19], cfg.get("MemorySize"), cfg.get("Timeout"), same, shared))
        if not all(same.values()) or not shared:
            r.fail("the deployed package is not this checkout's (or lacks the factor-book reader); nothing was invoked")
            sys.exit(1)
        if (cfg.get("Timeout") or 0) < 900 or (cfg.get("MemorySize") or 0) < 3008:
            r.fail("the deployed function has %s s and %s MB; the study needs 900 s and 3008 MB. Nothing was invoked" % (cfg.get("Timeout"), cfg.get("MemorySize")))
            sys.exit(1)
        try:
            rule = ev_.describe_rule(Name="justhodl-credit-before-equity-daily")
            r.log("schedule: %s %s %s" % (rule.get("Name"), rule.get("ScheduleExpression"), rule.get("State")))
            if rule.get("ScheduleExpression") != "cron(35 22 * * ? *)" or rule.get("State") != "ENABLED":
                fails.append("the engine's daily rule is not cron(35 22 * * ? *) ENABLED")
        except Exception as e:  # noqa: BLE001
            r.log("schedule: the rule could not be read (%s)" % type(e).__name__)
        raw0, _ = get(OUT_KEY)
        was = json.loads(raw0)
        was_cols = was["issuers"]["cols"]
        was_rows = {row[0]: dict(zip(was_cols, row)) for row in was["issuers"]["rows"]}
        ledger0, held0, calls0 = days_of(LEDGER_ROOT), days_of(HOLD_ROOT), days_of(CALLS_ROOT)
        hist0 = hashlib.sha256(get(HIST_KEY)[0]).hexdigest()
        r.log("before: %s version %s, %d bytes, generated %s, holdings of %s, %s companies, %s names, %s leads | ledger days %d (%s .. %s) | dated holdings %d | days of calls %d | evidence there: %s" % (
            OUT_KEY, was.get("version"), len(raw0), was.get("generated_at"), was.get("as_of"), len(was_rows), was.get("n_names"), was.get("n_leads"), len(ledger0), ledger0[0] if ledger0 else None,
            ledger0[-1] if ledger0 else None, len(held0), len(calls0), there(EVIDENCE_KEY)))

        r.section("2. One dry run inside Lambda (nothing written)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps({"dry_run": True}).encode())
        dry = json.loads(resp["Payload"].read() or b"{}")
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(dry)[:900]))
        tail_of(resp, r, ("[cbe", "REPORT", "Error", "Traceback"))
        if resp.get("FunctionError") or not dry.get("dry_run") or (dry.get("companies") or 0) < 3500 or (dry.get("listed") or 0) < 900:
            r.fail("the dry run inside Lambda failed or saw too few companies; nothing was published: %s" % json.dumps(dry)[:600])
            sys.exit(1)
        if hashlib.sha256(get(OUT_KEY)[0]).hexdigest() != hashlib.sha256(raw0).hexdigest() or days_of(LEDGER_ROOT) != ledger0 or days_of(HOLD_ROOT) != held0 or days_of(CALLS_ROOT) != calls0 or \
                hashlib.sha256(get(HIST_KEY)[0]).hexdigest() != hist0:
            r.fail("the dry run wrote something")
            sys.exit(1)
        r.log("the document, the ledger, the dated holdings, the calls and the model ledger are what they were: the dry run wrote nothing")
        if dry.get("version") != "3.0.0" or not dry.get("stressed") or (dry.get("themes") or {}).get("ai", 0) < 50:
            r.fail("the deployed engine's dry run is not 3.0.0 with its stress ranks and the AI stack (%s, %s, %s); nothing was published" % (dry.get("version"), dry.get("stressed"), dry.get("themes")))
            sys.exit(1)

        r.section("3. One publication (a one-off run outside the 22:35 UTC schedule; the study runs inside it when the evidence is due)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
        out = json.loads(resp["Payload"].read() or b"{}")
        took = time.time() - t0
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), took, json.dumps(out)[:400]))
        tail_of(resp, r, ("[cbe", "REPORT", "Error", "Traceback"))
        if resp.get("FunctionError") or out.get("statusCode") != 200:
            r.fail("the engine did not publish: %s" % json.dumps(out)[:600])
            sys.exit(1)

        r.section("4. Read back")
        raw, when = get(OUT_KEY)
        doc = json.loads(raw)
        cov, names = doc["coverage"], doc["names"]
        leads = [x for x in names if x["signal"] in LEADS]
        ledger, held, calls_days = days_of(LEDGER_ROOT), days_of(HOLD_ROOT), days_of(CALLS_ROOT)
        cols = doc["issuers"]["cols"]
        table = [dict(zip(cols, row)) for row in doc["issuers"]["rows"]]
        ev = doc.get("evidence") or {}
        r.log("%s: %d bytes (%d gzipped) written %s | %s %s | generated %s | bonds of %s, stocks to %s" % (
            OUT_KEY, len(raw), len(gzip.compress(raw)), when.strftime("%H:%M:%S"), doc.get("contract"), doc.get("version"), doc.get("generated_at"), doc.get("as_of"), doc.get("stock_as_of")))
        r.log("%d companies (%d listed, %d credit only), %d bonds with a spread (%s not in dollars), %d leads, %d CDS levels (%d tied) | by book %s | by currency %s | degraded %s" % (
            cov["companies"], cov["listed"], cov["unlisted"], cov["bonds_with_spread"], cov.get("bonds_not_usd"), len(leads), cov["cds_priced"], cov["cds_priced_tied"], cov.get("by_book"), cov.get("by_currency"), doc["degraded"]))
        r.log("windows: %s" % {k: (v.get("ref"), v.get("source") or v.get("why")) for k, v in doc["windows"].items()})
        r.log("curves: %s" % json.dumps(doc.get("curve")))

        # the board that was: every company still there, where it stood
        same_day = was.get("as_of") == doc.get("as_of")
        gone = [i for i in was_rows if i not in {x["id"] for x in table}]
        now_by = {x["id"]: x for x in table}
        # (a company that had no spread in dollars and has one now in another currency has gained a reading, not moved)
        moved = [i for i, w in was_rows.items() if i in now_by and w.get("spread_bp") is not None and (now_by[i]["book"] != w["book"] or now_by[i]["industry"] != w["industry"] or now_by[i]["tie"] != w["tie"])]
        changed = [i for i, w in was_rows.items() if i in now_by and w.get("spread_bp") is not None and w.get("spread_bp") != now_by[i]["spread_bp"]]
        r.log("against the document it replaces (holdings of %s then, %s now): %d companies were on the board, %d are no longer, %d changed book, industry or tie, %d have another spread; %d companies are new" % (
            was.get("as_of"), doc.get("as_of"), len(was_rows), len(gone), len(moved), len(changed), len(now_by) - (len(was_rows) - len(gone))))
        if gone or moved or (same_day and changed):
            r.log("  no longer: %s | changed book: %s | another spread: %s" % (gone[:8], moved[:8], [(i, was_rows[i].get("spread_bp"), now_by[i]["spread_bp"]) for i in changed[:6]]))

        # the record, the history, the bonds, the calls, the holdings
        ev_doc = parse(get(EVIDENCE_KEY)[0]) if there(EVIDENCE_KEY) else None
        hist_doc = parse(get(HISTORY_OUT_KEY)[0]) if there(HISTORY_OUT_KEY) else None
        bonds_doc = parse(get(BONDS_OUT_KEY)[0]) if there(BONDS_OUT_KEY) else None
        calls = parse(get(CALLS_ROOT + doc["as_of"] + ".json.gz")[0]) if doc["as_of"] in calls_days else None
        kept = parse(get(HOLD_ROOT + doc["as_of"] + ".json.gz")[0]) if doc["as_of"] in held else None
        track = parse(get(TRACK_KEY)[0]) if there(TRACK_KEY) else None
        per = {}
        for row in (bonds_doc or {}).get("rows") or []:
            per[row[0]] = per.get(row[0], 0) + 1
        ident = re.compile(r"^(?:[A-Z]{2}[A-Z0-9]{9}[0-9]|[0-9A-Z]{8}[0-9])$")
        leaks = sum(1 for row in (bonds_doc or {}).get("rows") or [] for v in row if isinstance(v, str) and ident.match(v))
        cuts, wrong_fc, given = (doc.get("risk") or {}).get("vol_cuts"), 0, 0
        for x in table:
            if x["book"] not in DOLLAR or x.get("risk_3m") is None:
                wrong_fc += 1 if (x["book"] not in DOLLAR and x.get("risk_3m") is not None) else 0
                continue
            given += 1
            band = next(i for i, (lo, hi) in enumerate(BANDS) if x["spread_bp"] >= lo and (hi is None or x["spread_bp"] < hi))
            third = None if (x.get("vol") is None or not cuts) else 0 if x["vol"] < cuts[0] else 1 if x["vol"] < cuts[1] else 2
            cell = ((ev.get("risk") or {}).get("h3") or {}).get(str(band)) or {}
            sub = cell["thirds"][third] if third is not None and cell.get("thirds") else None
            wrong_fc += (sub["fell"] if sub else cell.get("fell")) != x["risk_3m"]
        if ev:
            lead = ev["direction"]["lead_down"]
            r.log("the record: %d month-ends %s .. %s, %d company-months of %d companies; measured in %s s inside Lambda (the whole run took %.0f s) | left out %s" % (
                ev["month_ends"], ev["first"], ev["last"], ev["company_months"], ev["companies"], ev.get("seconds"), took, ev.get("skipped")))
            r.log("  %s" % ev["direction"]["words"])
            r.log("  a lead down: %s%% over the next month against the board's average stock, %s company-months, how sure %s | a lead up: %s%%, how sure %s" % (
                lead["mean"], lead["n"], lead["t"], ev["direction"]["lead_up"]["mean"], ev["direction"]["lead_up"]["t"]))
            r.log("  fell 20%% inside three months, by spread band: %s | the whole board %s%%" % ("; ".join("%s %s%% of %s" % (b["words"], ev["risk"]["h3"][str(b["i"])]["fell"], ev["risk"]["h3"][str(b["i"])]["n"])
                                                                                                  for b in ev["risk"]["bands"] if str(b["i"]) in ev["risk"]["h3"]), ev["board"]["h3"]["fell_big"]))
            chk = ev["check"]["h3"] or {}
            r.log("  the check: %s; the faller had the higher chance %s%% of the time; %s company-months over %s months" % (
                "; ".join("said %s%% happened %s%% (%s)" % (b["said"], b["happened"], b["n"]) for b in chk.get("buckets") or []), chk.get("auc"), chk.get("n"), chk.get("months")))
            if ev.get("cds"):
                r.log("  traded CDS: %s names, %s weeks, %s name-weeks; %s" % (ev["cds"]["names"], ev["cds"]["weeks"], ev["cds"]["name_weeks"],
                                                                             "; ".join("%s: next week %s%% (how sure %s)" % (x["words"], x["w1"]["mean"], x["w1"]["t"]) for x in ev["cds"]["readings"][:5])))
        r.log("the chance of a fall: %d listed companies have one (%s declared), %d differ from the record's table; %s at one in three or more; volatility thirds cut at %s" % (
            given, (doc.get("risk") or {}).get("n"), wrong_fc, (doc.get("risk") or {}).get("one_in_three"), cuts))
        for x in sorted([x for x in table if x.get("risk_3m") is not None and x["value"] >= 50e6], key=lambda x: (-x["risk_3m"], -x["value"]))[:15]:
            r.log("  %-6s %-36s %s spread %7s bp, own volatility %5s -> %5s%% in 3 months, %5s%% in one | stress %s | %s | stock %s%% in the month" % (
                x["ticker"], str(x["name"])[:36], x["book"], x["spread_bp"], x.get("vol"), x["risk_3m"], x.get("risk_1m"), x["stress"], x.get("cell"), x["px_m1"]))
        r.log("distressed: %d companies (%d listed) | the nine cells today: %s" % (sum(1 for x in table if x["distressed"]), sum(1 for x in table if x["distressed"] and x["ticker"]), json.dumps((doc.get("matrix") or {}).get("cells"))))
        r.log("kept: ledger days %d (was %d), dated holdings %d (was %d), days of calls %d (was %d); bond document %s rows under %d companies; history %s companies over %s month-ends; track %s" % (
            len(ledger), len(ledger0), len(held), len(held0), len(calls_days), len(calls0), len((bonds_doc or {}).get("rows") or []), len(per), len((hist_doc or {}).get("companies") or {}),
            len((hist_doc or {}).get("dates") or []), json.dumps(doc.get("track"))[:400]))
        orcl = next((x for x in table if x["ticker"] == "ORCL"), {})
        r.log("Oracle: %s" % json.dumps({k: orcl.get(k) for k in ("book", "ccy", "spread_bp", "stress", "cell", "signal", "pd_1y", "due2y", "px", "px_min", "curve_bp", "vol", "risk_1m", "risk_3m", "lo_3m", "hi_3m", "n_bonds", "n_other")}))
        st, at = doc.get("stress") or {}, (doc.get("stress") or {}).get("stressed_at")
        hot = [x for x in table if x.get("stress") is not None and at is not None and x["stress"] >= at]
        ai = (doc.get("themes") or [{}])[0]
        mine = [x for x in table if x.get("theme") == "ai"]
        checks = {
            "the document is 3.0 on the v2 contract": doc.get("contract") == "credit-before-equity.v2" and doc.get("version") == "3.0.0",
            "the 2.0 and 2.1 columns are where they were, the 3.0 columns after them": cols[:25] == COLS_2_0 and cols[25:37] == COLS_2_1 and all(len(row) == len(cols) for row in doc["issuers"]["rows"]) and "risk_3m" in cols[37:],
            "every company that was on the board is still on it, in its book and its industry, tied as it was": not gone and not moved,
            "with the same holdings day, every company that was there has the spread it had": (not same_day) or not changed,
            "the stress counts are the rows": st.get("n_stressed") == len(hot) > 100 and st.get("n_stressed_listed") == sum(1 for x in hot if x["ticker"]),
            "the AI stack's count is its rows and its layers": ai.get("n") == len(mine) >= 50 and sum(layer["n"] for layer in ai.get("layers") or []) == len(mine),
            "no lead where a liquid CDS says the opposite": all(x.get("cds_says") != "contradicts" for x in leads),
            "the counts are the rows": doc["n_names"] == len(names) and doc["n_leads"] == len(leads) and doc["leads"] == leads[:20] and cov["companies"] == len(doc["issuers"]["rows"]),
            "it is the size the dry run inside Lambda saw": cov["companies"] == dry["companies"] and cov["listed"] == dry["listed"] and doc["n_leads"] == dry["leads"],
            "every key the first engine published is there": all(k in doc for k in ("version", "generated_at", "n_names", "n_leads", "n_awaiting_history", "names", "leads", "degraded", "gaps", "thresholds", "method", "attribution")),
            "the model's names kept their fields": sum(1 for x in names if isinstance(x.get("distance_to_default"), (int, float)) and "d_distance_to_default" in x and "hist_n" in x) >= 18,
            "the run was not degraded": not doc["degraded"],
            "a lot more companies: the non-dollar books are there, in their own books": (cov.get("by_book") or {}).get("xig", 0) >= 500 and cov["companies"] >= len(was_rows) + 800 and
                all(x["ccy"] != "USD" for x in table if x["book"] not in DOLLAR) and all(x["ccy"] == "USD" for x in table if x["book"] in DOLLAR),
            "the record was measured inside Lambda and is in the document": bool(ev.get("readings")) and ev.get("month_ends", 0) >= 60 and ev_doc == ev and bool((ev.get("check") or {}).get("h3")),
            "the month-by-month document holds every month-end of the record": hist_doc is not None and len(hist_doc["dates"]) == ev.get("month_ends") and doc.get("history", {}).get("key") == HISTORY_OUT_KEY,
            "every chance of a fall is the record's own table, and no non-dollar company has one": wrong_fc == 0 and given == (doc.get("risk") or {}).get("n", -1) and given >= 800,
            "every bond is under its company, with no identifier": bonds_doc is not None and len(bonds_doc["rows"]) == doc["bonds"]["n"] and leaks == 0 and "k" not in bonds_doc["cols"] and
                all(per.get(x["id"], 0) == x["n_bonds"] + (x["n_other"] or 0) for x in table),
            "the nine cells count the listed companies with a reading": sum((doc.get("matrix") or {}).get("cells", {}).values()) == (doc.get("matrix") or {}).get("n") == sum(1 for x in names if x.get("cell")),
            "the day's calls were kept as they were said": calls is not None and len(calls["rows"]) == given and calls["as_of"] == doc["as_of"],
            "the day's holdings were kept, so the history grows": kept is not None and len(kept["rows"]) >= cov["bonds_read"],
            "the track counts today's calls and has nothing to score yet": (doc.get("track") or {}).get("last_call") == doc["as_of"] and track is not None,
            "three months is a real ledger day now": bool(doc["windows"]["m3"].get("ref")) and doc["windows"]["m3"].get("source") == "the board's own ledger",
            "no ledger day, dated holdings or day of calls was removed": set(ledger0) <= set(ledger) and set(held0) <= set(held) and set(calls0) <= set(calls_days),
        }
        for what, ok in checks.items():
            r.log("check %-78s %s" % (what, "ok" if ok else "FAILED"))
            if not ok:
                fails.append(what)
        served = {}
        for attempt in range(12):
            try:
                a, b, c = site(OUT_KEY), site(BONDS_OUT_KEY), site(HISTORY_OUT_KEY)
                served = {"document": a.get("generated_at") == doc.get("generated_at"), "bonds": b.get("generated_at") == doc.get("generated_at") and len(b["rows"]) == doc["bonds"]["n"],
                          "history": c.get("dates") == (hist_doc or {}).get("dates")}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(10)
        r.log("the site serves the three documents of this run: %s" % json.dumps(served))
        if not (served and all(v is True for v in served.values())):
            fails.append("the site does not serve this run's documents")
        r.kv(companies=cov["companies"], listed=cov["listed"], leads=len(leads), cds_levels=cov["cds_priced"], non_dollar=sum(1 for x in table if x["book"] not in DOLLAR), chances=given,
             month_ends=ev.get("month_ends"), run_seconds=round(took), failed_checks=len(fails), document=doc["generated_at"][:16])
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("3.0.0 LIVE: %d companies (%d listed; %d read in another currency), %d distressed, the record of %d month-ends measured inside Lambda in %s s, %d listed companies with the chance of a fall it supports, "
             "every bond under its company; one dry run and one publication, both by hand, disclosed" % (
                 cov["companies"], cov["listed"], sum(1 for x in table if x["book"] not in DOLLAR), sum(1 for x in table if x["distressed"]), ev.get("month_ends"), ev.get("seconds"), given))


if __name__ == "__main__":
    main()
