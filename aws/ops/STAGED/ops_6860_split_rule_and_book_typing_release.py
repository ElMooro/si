"""ops 6860 - release: the factor book's typing (1.0.9) and the split rule (leaders 1.0.1, campaign-scan 1.0.2,
strength 1.0.13, stock-volatility 1.0.1), one run each, verified on what they wrote.

Khalid 2026-11-20: "fix these: The shared split-detection rule reads 53 real one-day crashes and spikes as stock splits
... Leaders, the Wyckoff scan and Strength still use the old rule. / The factor book lists 33 notes and preferred
shares as common stock ..."

Order is the nightly order: the book first, then the four engines that read it.
  G0  the governor holds none of the five and the stage is NORMAL; each deployed artifact is the new version.
  1   justhodl-bank-factors is invoked once (Event, force) and its manifest polled: coverage.other_issues names every
      row moved out of common stock; each is then read back from the book (is_common false, its flag set) and the
      controls (share classes, ADRs, the issuers themselves) are still common stock.
  2   strength, leaders, campaign scan and stock volatility are invoked once each (Event) and their artifacts polled
      for the new version: no moved ticker is left on any of them, the refused "splits" are published, and what
      changed on each artifact against the copy read before the run is listed.
A same-session re-run keeps each desk's day-over-day record (leaders / campaign scan changes_since).  Re-runnable.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import bank_enrich  # noqa: E402

LIVE = "justhodl-dashboard-live"
GOVERNOR = "justhodl-spend-governor"
BOOK = "justhodl-bank-factors"
BOOK_VERSION = "1.0.9"
ENGINES = {  # function: (artifact key, version, file that must carry the at-the-open test)
    "justhodl-strength": ("data/strength.json", "1.0.13", "lambda_function.py"),
    "justhodl-leaders": ("data/leaders.json", "1.0.1", "leaders_tape.py"),
    "justhodl-campaign-scan": ("data/campaign-scan.json", "1.0.2", "leaders_tape.py"),
    "justhodl-stock-volatility": ("data/stock-volatility.json", "1.0.1", "leaders_tape.py"),
}
CONTROLS = ["GOOGL", "GOOG", "FOXA", "FOX", "NWSA", "NWS", "BBD", "CIB", "BAK", "ARM", "AGNC", "HBAN", "PFBC", "MCHP", "ACGL", "XEL", "TPG", "ATLC"]
EXPECTED = ["AGNCO", "AGNCL", "HBANL", "HBANM", "HBANZ", "ATLCL", "ATLCZ", "BHFAL", "MCHPP", "ACGLN", "XELLL", "TPGXL", "WAFDP", "CTGG", "EMP", "JSM"]
TICKER_KEYS = ("ticker", "t", "symbol")


def tickers_of(doc):
    """Every ticker an artifact carries in a row: dict rows keyed ticker / t / symbol, cols + rows tables, and lists of ticker strings under boards."""
    found = set()

    def walk(x, key=None):
        if isinstance(x, dict):
            if isinstance(x.get("cols"), list) and isinstance(x.get("rows"), list) and x["rows"] and isinstance(x["rows"][0], list) and x["cols"] and x["cols"][0] in TICKER_KEYS:
                found.update(str(r[0]) for r in x["rows"] if r)
            for k in TICKER_KEYS:
                if isinstance(x.get(k), str):
                    found.add(x[k])
            for k, v in x.items():
                walk(v, k)
        elif isinstance(x, list):
            if key and x and all(isinstance(v, str) and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", v) for v in x):
                found.update(x)
            for v in x:
                walk(v, key)
    walk(doc)
    return found


def read(s3, key):
    return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())


def ready(lam, fn, r, wait_s=240):
    t0 = time.time()
    while True:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            return c
        if time.time() - t0 > wait_s:
            r.fail("%s not ready: State %s LastUpdateStatus %s" % (fn, c.get("State"), c.get("LastUpdateStatus")))
            sys.exit(1)
        time.sleep(10)


def deployed(lam, fn, member):
    blob = urllib.request.urlopen(lam.get_function(FunctionName=fn)["Code"]["Location"], timeout=120).read()
    z = zipfile.ZipFile(io.BytesIO(blob))
    src = z.read("lambda_function.py").decode("utf-8", "replace")
    ver = re.search(r'^VERSION = "([^"]+)"', src, re.M)
    return (ver.group(1) if ver else None), (z.read(member).decode("utf-8", "replace") if member in z.namelist() else ""), z.namelist()


def common_rows():
    bank_enrich.matrix(max_age_s=0)
    return {str(x["symbol"]).upper(): x for x in bank_enrich.rows(fields=["mcap", "dollar_vol_20d"]) if x.get("symbol")}


def main():
    now = datetime.now(timezone.utc)
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
    lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
    with report("ops_6860_split_rule_and_book_typing_release") as r:
        r.heading("ops 6860 - the factor book's typing and the split rule, released %s" % now.isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        # ------------------------------------------------------------------ G0
        r.section("G0. holds, stage, deployed versions")
        st = {}
        for _ in range(4):
            try:
                st = json.loads(lam.invoke(FunctionName=GOVERNOR, Payload=b'{"action":"status"}')["Payload"].read() or b"{}")
                break
            except lam.exceptions.TooManyRequestsException:
                time.sleep(30)
        held = set(st.get("capped") or []) | {fn for fn, _ in (st.get("anomaly_held") or [])}
        r.log("governor stage %s | capped %s | anomaly %s" % (st.get("stage"), st.get("capped"), st.get("anomaly_held")))
        mine = [BOOK] + list(ENGINES)
        if held & set(mine) or st.get("stage") not in ("NORMAL", None):
            r.fail("an engine is held (%s) or the stage is not NORMAL - nothing invoked" % sorted(held & set(mine)))
            sys.exit(1)
        ready(lam, BOOK, r)
        ver, src, names = deployed(lam, BOOK, "other_issues.py")
        r.log("%s: deployed VERSION %s, other_issues.py %s" % (BOOK, ver, "present" if "def find(" in src else "ABSENT"))
        if ver != BOOK_VERSION or "def find(" not in src:
            r.fail("%s is not %s on Lambda - deploy-lambdas has not landed; re-run this op after it does" % (BOOK, BOOK_VERSION))
            sys.exit(1)
        for fn, (key, want, member) in ENGINES.items():
            ready(lam, fn, r)
            ver, src, _ = deployed(lam, fn, member)
            r.log("%s: deployed VERSION %s, at-the-open test in %s: %s" % (fn, ver, member, "present" if "def at_open(" in src else "ABSENT"))
            if ver != want or "def at_open(" not in src:
                r.fail("%s is not %s on Lambda - deploy-lambdas has not landed; re-run this op after it does" % (fn, want))
                sys.exit(1)

        # ------------------------------------------------------------------ before
        before_book = common_rows()
        n_common0 = sum(1 for x in before_book.values() if x.get("is_common"))
        before = {}
        for fn, (key, _, _) in ENGINES.items():
            d = read(s3, key)
            before[fn] = {"version": d.get("version"), "generated_at": d.get("generated_at"), "as_of": d.get("as_of"), "tickers": tickers_of(d)}
            r.log("before: %s %s generated %s as of %s, %d tickers on it" % (key, d.get("version"), d.get("generated_at"), d.get("as_of"), len(before[fn]["tickers"])))
        r.log("before: the book has %d rows, %d common stock" % (len(before_book), n_common0))

        # ------------------------------------------------------------------ 1. the book
        r.section("1. %s: one run, then the book read back" % BOOK)
        man0 = bank_enrich.manifest(max_age_s=0) or {}
        t_inv = time.time()
        res = lam.invoke(FunctionName=BOOK, InvocationType="Event", Payload=json.dumps({"force": True, "trigger": "ops-6860"}).encode())
        r.log("invoke status %s (manifest before: generated %s, version %s)" % (res.get("StatusCode"), man0.get("generated_at") or man0.get("updated_at"), man0.get("version")))
        man = {}
        while time.time() - t_inv < 870:
            time.sleep(30)
            try:
                if s3.head_object(Bucket=LIVE, Key=bank_enrich.ROOT + "manifest.json")["LastModified"].timestamp() > t_inv:
                    man = bank_enrich.manifest(max_age_s=0) or {}
                    break
            except Exception:  # noqa: BLE001
                pass
        if not man:
            r.fail("the factor book's manifest did not refresh within 14.5 min of the invoke")
            sys.exit(1)
        oi = (man.get("coverage") or {}).get("other_issues") or {}
        moved = oi.get("tickers") or {}
        r.log("the book: version %s, written %.0fs after the invoke; other_issues n %s, by flag %s, by evidence %s, structure test %s, reference names %s" % (
            man.get("version"), time.time() - t_inv, oi.get("n"), json.dumps(oi.get("by_flag")), json.dumps(oi.get("by_evidence")), oi.get("structure_test"), oi.get("reference_names")))
        ck(man.get("version") == BOOK_VERSION and oi.get("n") and len(moved) == oi["n"] and oi.get("structure_test") is True, "the manifest does not carry the 1.0.9 pass: version %s, other_issues %s" % (man.get("version"), json.dumps(oi)[:200]))
        book = common_rows()
        n_common1 = sum(1 for x in book.values() if x.get("is_common"))
        bad = [s for s in moved if (book.get(s) or {}).get("is_common") is not False or not ((book.get(s) or {}).get("is_preferred") or (book.get(s) or {}).get("is_debt_or_structured") or (book.get(s) or {}).get("is_warrant_unit"))]
        ck(not bad, "named as moved but still common stock in the book: %s" % bad[:12])
        ck(all((book.get(s) or {}).get("is_common") for s in CONTROLS if s in book), "a control lost is_common: %s" % [s for s in CONTROLS if s in book and not book[s].get("is_common")])
        ck(all(s in moved for s in EXPECTED if s in book), "expected to move and did not: %s" % [s for s in EXPECTED if s in book and s not in moved])
        was_common = [s for s in moved if (before_book.get(s) or {}).get("is_common")]
        r.log("common stock %d -> %d; %d of the %d moved rows were common stock before this run" % (n_common0, n_common1, len(was_common), len(moved)))
        for flag in ("is_preferred", "is_debt_or_structured", "is_warrant_unit"):
            names = sorted(s for s in moved if (book.get(s) or {}).get(flag) and not (flag != "is_preferred" and (book.get(s) or {}).get("is_preferred") and "preferred" in moved[s]))
            r.log("  %s (%d): %s" % (flag, len(names), ", ".join(names)))
        for s in sorted(moved)[:80]:
            r.log("    %-7s %s" % (s, moved[s]))
        r.log("controls still common stock: %s" % ", ".join(s for s in CONTROLS if (book.get(s) or {}).get("is_common")))
        liquid = sorted(s for s in moved if ((before_book.get(s) or {}).get("dollar_vol_20d") or 0) >= 250000)
        r.log("moved rows that trade $250k a day or more: %d - %s" % (len(liquid), ", ".join(liquid)))

        # ------------------------------------------------------------------ 2. the four engines
        r.section("2. strength, leaders, campaign scan, stock volatility: one run each")
        t_eng = time.time()
        for fn in ENGINES:
            res = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=json.dumps({"trigger": "ops-6860"}).encode())
            r.log("%s invoke status %s" % (fn, res.get("StatusCode")))
        after, waiting = {}, set(ENGINES)
        while waiting and time.time() - t_eng < 780:
            time.sleep(20)
            for fn in sorted(waiting):
                key = ENGINES[fn][0]
                try:
                    if s3.head_object(Bucket=LIVE, Key=key)["LastModified"].timestamp() > t_eng:
                        after[fn] = read(s3, key)
                        waiting.discard(fn)
                        r.log("%s wrote %s %.0fs after the invoke" % (fn, key, time.time() - t_eng))
                except Exception:  # noqa: BLE001
                    pass
        if waiting:
            r.fail("no new artifact within 13 min from: %s" % sorted(waiting))
            sys.exit(1)
        for fn, (key, want, _) in ENGINES.items():
            d, b = after[fn], before[fn]
            now_t = tickers_of(d)
            left = sorted(set(moved) & now_t)
            gone, came = sorted(b["tickers"] - now_t), sorted(now_t - b["tickers"])
            ck(d.get("version") == want, "%s wrote version %s, not %s" % (key, d.get("version"), want))
            ck(not left, "%s still carries rows the book moved out of common stock: %s" % (key, left[:12]))
            ck(not (d.get("n_errors") or 0) or (d.get("n_errors") or 0) < 25, "%s: %s errors" % (key, d.get("n_errors")))
            r.log("%s: version %s, generated %s, as of %s | %d tickers (before %d) | left the artifact: %d %s | came on: %d %s" % (
                key, d.get("version"), d.get("generated_at"), d.get("as_of"), len(now_t), len(b["tickers"]), len(gone), ", ".join(gone[:40]), len(came), ", ".join(came[:40])))
            r.log("  of those that left, moved by the book: %s" % (", ".join(sorted(set(gone) & set(moved))) or "none"))
            if fn in ("justhodl-leaders", "justhodl-strength"):
                sp = d.get("splits") or {}
                ck("n_refused" in sp, "%s does not publish splits.n_refused" % key)
                r.log("  splits: inferred %s, refused %s %s" % (sp.get("inferred"), sp.get("n_refused"), json.dumps(sp.get("refused"))[:300]))
            else:
                sr = (d.get("universe") or {}).get("splits_refused") or {}
                ck(isinstance(sr.get("n"), int) and sr["n"] >= 40, "%s refused %s inferred splits; the audit found 53" % (key, sr.get("n")))
                r.log("  splits refused %s: %s" % (sr.get("n"), "; ".join("%s %s x%s" % (x["t"], x["date"], x["ratio"]) for x in (sr.get("sample") or [])[:60])))
                r.log("  universe left_out: %s" % json.dumps((d.get("universe") or {}).get("left_out")))
            if fn == "justhodl-leaders":
                ch = d.get("changes") or {}
                r.log("  leaders changes kept: prev_as_of %s, new %s, gone %s; leaders %s" % (ch.get("prev_as_of"), ch.get("n_new", len(ch.get("new") or [])), ch.get("n_gone", len(ch.get("gone") or [])), len(d.get("rows") or [])))
            if fn == "justhodl-campaign-scan":
                r.log("  counts: %s | changes prev_as_of %s" % (json.dumps({k: (v or {}).get("n") for k, v in (d.get("counts") or {}).items()}), (d.get("changes") or {}).get("prev_as_of")))
            if fn == "justhodl-stock-volatility":
                u = d.get("universe") or {}
                ck(not u.get("reclassified"), "the volatility engine's own guard still finds rows the book calls common stock: %s" % json.dumps(u.get("reclassified"))[:300])
                r.log("  universe: stocks %s, ETFs %s, set aside %s, own guard found %d" % (u.get("scanned_stock"), u.get("scanned_etf"), json.dumps(u.get("set_aside")), len(u.get("reclassified") or [])))

        r.kv(failures=len(fails), moved_out_of_common=len(moved), liquid_moved=len(liquid), common_before=n_common0, common_after=n_common1,
             refused_campaign=((after["justhodl-campaign-scan"].get("universe") or {}).get("splits_refused") or {}).get("n"),
             refused_volatility=((after["justhodl-stock-volatility"].get("universe") or {}).get("splits_refused") or {}).get("n"))
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("RELEASED: the book moved %d second issues out of common stock (controls unchanged); strength %s, leaders %s, campaign scan %s and stock volatility %s ran on it with the at-the-open split test, none carries a moved row" % (
            len(moved), after["justhodl-strength"].get("version"), after["justhodl-leaders"].get("version"), after["justhodl-campaign-scan"].get("version"), after["justhodl-stock-volatility"].get("version")))


if __name__ == "__main__":
    main()
