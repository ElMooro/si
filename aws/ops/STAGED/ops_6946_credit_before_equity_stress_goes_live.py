"""ops 6946 - Credit Before Equity 2.1.0 goes live: the deployed function, one dry run inside Lambda, one publication.

2.1 adds, beside everything 2.0 publishes: a stress rank for every company (where it stands in its own book on spread
level, the year's widening and the month's - it does not look at the stock), the AI stack theme, and the rule that no
lead is called when the bonds and a liquid CDS disagree. Khalid 2026-11-24: "ai companies and oracle are under enormous
stress why arent they showing".

  1. Guard: the deployed package's files are this checkout's, byte for byte (cbe_themes.py among them); the once-a-day
     rule. If not, nothing is invoked.
  2. One invoke with {"dry_run": true}: the engine builds the whole board inside Lambda and writes nothing. If that
     fails, or it sees fewer companies than the board has, nothing is published.
  3. One invoke with {}: the engine publishes data/credit-before-equity.json. This is a one-off run outside the engine's
     22:35 UTC schedule (which already ran today, on 2.0), disclosed to Khalid; the schedule itself is untouched.
  4. Read back: the document is 2.1 on the v2 contract, every 2.0 key and column is where it was, the stress ranks and
     the theme reconcile with the rows, Oracle is under stress and in the theme, and the site serves it.

Safe to run again: each run is one more dry run and one more publication of the kind the schedule makes.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import hashlib
import io
import json
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
FILES = ("lambda_function.py", "cbe_core.py", "cbe_names.py", "cbe_themes.py", "leaders_tape.py")
COLS_2_0 = ["id", "ticker", "name", "industry", "book", "country", "n_bonds", "value", "yield_pct", "dur", "spread_bp", "d_w1", "d_m1", "x_m1", "pct_m1", "n_m1", "d_m3", "d_ytd",
            "cds_bp", "cds_d1m", "px_m1", "direction", "signal", "distressed", "tie"]
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe"
LEADS = ("CREDIT_LEADS_DOWN", "CREDIT_LEADS_UP")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=320, retries={"max_attempts": 0}))
    ev = boto3.client("events", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    fails = []

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return o["Body"].read(), o["LastModified"]

    def ledger():
        return sorted(o["Key"][len(LEDGER_ROOT):-8] for o in s3.list_objects_v2(Bucket=LIVE, Prefix=LEDGER_ROOT).get("Contents") or [])

    with report("ops_6946_credit_before_equity_stress_goes_live") as r:
        r.heading("ops 6946 - Credit Before Equity 2.1.0: the stress rank and the AI stack go live, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

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
        try:
            rule = ev.describe_rule(Name="justhodl-credit-before-equity-daily")
            r.log("schedule: %s %s %s" % (rule.get("Name"), rule.get("ScheduleExpression"), rule.get("State")))
            if rule.get("ScheduleExpression") != "cron(35 22 * * ? *)" or rule.get("State") != "ENABLED":
                fails.append("the engine's daily rule is not cron(35 22 * * ? *) ENABLED")
        except Exception as e:  # noqa: BLE001
            r.log("schedule: the rule could not be read (%s)" % type(e).__name__)
        raw0, when0 = get(OUT_KEY)
        was = json.loads(raw0)
        days0 = ledger()
        hist0 = hashlib.sha256(get(HIST_KEY)[0]).hexdigest()
        r.log("before: %s version %s, %d bytes, generated %s, %s names, %s leads | ledger days %d (%s .. %s)" % (
            OUT_KEY, was.get("version"), len(raw0), was.get("generated_at"), was.get("n_names"), was.get("n_leads"), len(days0), days0[0] if days0 else None, days0[-1] if days0 else None))

        r.section("2. One dry run inside Lambda (nothing written)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps({"dry_run": True}).encode())
        dry = json.loads(resp["Payload"].read() or b"{}")
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(dry)[:700]))
        try:
            tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
            for line in tail.splitlines():
                if "[cbe]" in line or "REPORT" in line or "Error" in line or "Traceback" in line:
                    r.log("  lambda: %s" % line.strip()[:330])
        except Exception:  # noqa: BLE001
            pass
        if resp.get("FunctionError") or not dry.get("dry_run") or (dry.get("companies") or 0) < 2500 or (dry.get("listed") or 0) < 900:
            r.fail("the dry run inside Lambda failed or saw too few companies; nothing was published: %s" % json.dumps(dry)[:600])
            sys.exit(1)
        if hashlib.sha256(get(OUT_KEY)[0]).hexdigest() != hashlib.sha256(raw0).hexdigest() or ledger() != days0 or hashlib.sha256(get(HIST_KEY)[0]).hexdigest() != hist0:
            r.fail("the dry run wrote something")
            sys.exit(1)
        r.log("the document, the ledger and the model ledger are what they were: the dry run wrote nothing")

        if dry.get("version") != "2.1.0" or not dry.get("stressed") or (dry.get("themes") or {}).get("ai", 0) < 50:
            r.fail("the deployed engine's dry run is not 2.1.0 with its stress ranks and the AI stack (%s, %s, %s); nothing was published" % (dry.get("version"), dry.get("stressed"), dry.get("themes")))
            sys.exit(1)

        r.section("3. One publication (a one-off run outside the 22:35 UTC schedule)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
        out = json.loads(resp["Payload"].read() or b"{}")
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(out)[:400]))
        try:
            tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
            for line in tail.splitlines():
                if "[cbe]" in line or "REPORT" in line:
                    r.log("  lambda: %s" % line.strip()[:330])
        except Exception:  # noqa: BLE001
            pass
        if resp.get("FunctionError") or out.get("statusCode") != 200:
            r.fail("the engine did not publish: %s" % json.dumps(out)[:600])
            sys.exit(1)

        r.section("4. Read back")
        raw, when = get(OUT_KEY)
        doc = json.loads(raw)
        cov, names = doc["coverage"], doc["names"]
        leads = [x for x in names if x["signal"] in LEADS]
        days = ledger()
        r.log("%s: %d bytes (%d gzipped) written %s | %s %s | generated %s | bonds of %s, stocks to %s" % (
            OUT_KEY, len(raw), len(gzip.compress(raw)), when.strftime("%H:%M:%S"), doc.get("contract"), doc.get("version"), doc.get("generated_at"), doc.get("as_of"), doc.get("stock_as_of")))
        r.log("%d companies (%d listed, %d credit only), %d bonds with a spread, %d leads, %d CDS levels (%d tied) | degraded %s" % (
            cov["companies"], cov["listed"], cov["unlisted"], cov["bonds_with_spread"], len(leads), cov["cds_priced"], cov["cds_priced_tied"], doc["degraded"]))
        r.log("windows: %s" % {k: (v.get("ref"), v.get("source") or v.get("why")) for k, v in doc["windows"].items()})
        r.log("ledger days now %d: %s" % (len(days), days))
        cols = doc["issuers"]["cols"]
        table = [dict(zip(cols, row)) for row in doc["issuers"]["rows"]]
        st, at = doc.get("stress") or {}, (doc.get("stress") or {}).get("stressed_at")
        hot = [x for x in table if x.get("stress") is not None and at is not None and x["stress"] >= at]
        themes = doc.get("themes") or []
        ai = themes[0] if themes else {}
        mine = [x for x in table if x.get("theme") == "ai"]
        orcl = next((x for x in table if x["ticker"] == "ORCL"), {})
        r.log("stress: %s | %d at %s or more (%d listed)" % (json.dumps(st), len(hot), at, sum(1 for x in hot if x["ticker"])))
        r.log("AI stack: %s companies (%s listed), $%.1fB held, middle spread %s bp, %s under stress holding %s%% of its value: %s | year %s" % (
            ai.get("n"), ai.get("n_listed"), (ai.get("value") or 0) / 1e9, ai.get("spread_bp"), ai.get("n_stressed"), ai.get("stressed_value_pct"), ai.get("largest_stressed"), (ai.get("win") or {}).get("ytd")))
        for x in sorted([x for x in table if x.get("stress") is not None and x["value"] >= 50e6], key=lambda x: (-x["stress"], -x["value"]))[:12]:
            r.log("  %-6s %-36s %s $%5.0fM spread %7s bp, year %s bp (%s%%), month %s bp, CDS %s bp | stress %s | stock %s%% in the month | %s -> %s" % (
                x["ticker"] or "", str(x["name"])[:36], x["book"], x["value"] / 1e6, x["spread_bp"], x["d_ytd"], x["pct_ytd"], x["d_m1"], x["cds_bp"], x["stress"], x["px_m1"], x["theme"] or "", x["signal"]))
        clash = [x for x in names if x.get("credit_conflict")]
        r.log("leads held back because a liquid CDS says the opposite of the bonds: %s" % [(x["ticker"], x.get("d_spread_bp"), x.get("cds_d1m")) for x in clash])
        checks = {
            "the document is 2.1 on the v2 contract": doc.get("contract") == "credit-before-equity.v2" and doc.get("version") == "2.1.0",
            "the 2.0 columns are where they were, the 2.1 columns after them": cols[:25] == COLS_2_0 and all(len(row) == len(cols) for row in doc["issuers"]["rows"]) and "stress" in cols[25:],
            "the stress counts are the rows": st.get("n_stressed") == len(hot) > 100 and st.get("n_stressed_listed") == sum(1 for x in hot if x["ticker"]),
            "every rank is between 0 and 100": all(0 <= x["stress"] <= 100 for x in table if x.get("stress") is not None) and sum(1 for x in table if x.get("stress") is not None) > 2500,
            "the AI stack's count is its rows and its layers": ai.get("n") == len(mine) >= 50 and sum(layer["n"] for layer in ai.get("layers") or []) == len(mine),
            "Oracle is in the theme and under stress, and is not a lead": orcl.get("theme") == "ai" and orcl.get("layer") == "hyperscalers" and (orcl.get("stress") or 0) >= (at or 101) and orcl.get("signal") == "NONE",
            "no lead where a liquid CDS says the opposite": all(x.get("cds_says") != "contradicts" for x in leads) and all(x["signal"] == "NONE" for x in clash),
            "the industries carry the stress figures": all("stressed_value_pct" in i and "n_stressed" in i for i in doc["industries"]["all"]),
            "a window that is not there yet says when it can be": all(v.get("ref") or ("began on" not in (v.get("why") or "") and v.get("available_from")) for v in doc["windows"].values()),
            "the counts are the rows": doc["n_names"] == len(names) and doc["n_leads"] == len(leads) and doc["leads"] == leads[:20] and cov["companies"] == len(doc["issuers"]["rows"]),
            "it is the size the runner's dry run saw": cov["companies"] == dry["companies"] and cov["listed"] == dry["listed"] and doc["n_leads"] == dry["leads"],
            "every key the first engine published is there": all(k in doc for k in ("version", "generated_at", "n_names", "n_leads", "n_awaiting_history", "names", "leads", "degraded", "gaps", "thresholds", "method", "attribution")),
            "the model's names kept their fields": sum(1 for x in names if isinstance(x.get("distance_to_default"), (int, float)) and "d_distance_to_default" in x and "hist_n" in x) >= 18,
            "the run was not degraded": not doc["degraded"],
            "the ledger holds today's holdings day and the two reference days": doc["as_of"] in days and all(v.get("ref") in days for v in doc["windows"].values() if v.get("ref")),
            "a one-week comparison exists from the board's own ledger": doc["windows"]["w1"].get("source") == "the board's own ledger",
            "no ledger day was removed": set(days0) <= set(days),
        }
        for what, ok in checks.items():
            r.log("check %-62s %s" % (what, "ok" if ok else "FAILED"))
            if not ok:
                fails.append(what)
        site = None
        for attempt in range(12):
            try:
                req = urllib.request.Request("https://justhodl.ai/data/credit-before-equity.json?probe=%d" % (int(time.time()) + attempt), headers={"User-Agent": UA, "Cache-Control": "no-cache"})
                site = json.loads(urllib.request.urlopen(req, timeout=60).read())
                if site.get("generated_at") == doc.get("generated_at"):
                    break
            except Exception as e:  # noqa: BLE001
                site = {"error": str(e)[:80]}
            time.sleep(10)
        ok = bool(site) and site.get("generated_at") == doc.get("generated_at")
        r.log("the site serves the document generated %s -> %s" % ((site or {}).get("generated_at") or site, "the same" if ok else "NOT the warehouse's"))
        if not ok:
            fails.append("the site does not serve the new document")
        for x in leads[:8]:
            r.log("  lead %-6s %-32s spread %s bp, %+.1f bp in the month (%+.1f past its book), stock %+.2f%% -> %s" % (
                x["ticker"], str(x["name"])[:32], x.get("spread_bp"), x.get("d_spread_bp") or 0, x.get("x_spread_bp") or 0, x.get("d_price_pct") or 0, x["signal"]))
        r.kv(companies=cov["companies"], listed=cov["listed"], leads=len(leads), cds_levels=cov["cds_priced"], ledger_days=len(days), failed_checks=len(fails), document=doc["generated_at"][:16], under_stress=len(hot), ai_stack=ai.get("n"))
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("2.1.0 LIVE: %d companies (%d listed), %d under stress (%d listed), AI stack %s companies with %s under stress, %d leads; one dry run and one publication, both by hand, disclosed" % (
            cov["companies"], cov["listed"], len(hot), sum(1 for x in hot if x["ticker"]), ai.get("n"), ai.get("n_stressed"), len(leads)))


if __name__ == "__main__":
    main()
