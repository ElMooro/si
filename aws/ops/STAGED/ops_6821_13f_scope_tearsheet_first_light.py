"""ops 6821 - 13F book 1.5.0 first light (re-runnable): scope tearsheets, largest option lines, board totals - live.

Same shape as ops 6818 (whose checks on every manager's tearsheet run again here), plus the 1.5.0 blocks:
book.tearsheet[cohort | concentrated] must equal its members (totals, counts, the four lists per view ranked by
estimate, each row naming a member), every manager with option rows carries its largest call / put lines, and each
cross-manager board says how many identities it ranks.

(the 1.4.0 text follows)
ops 6818 - 13F book 1.4.0 first light (re-runnable): the manager tearsheet, live.

  1. the function carries the new code (LastModified shown; the version in the published book decides);
  2. invoke justhodl-13f-book once, synchronously (a root run under the trigger cap) - it reads the native research
     artifacts and rewrites data/13f-book.json + data/13f-tickers.json, as it does every day;
  3. read the live book: version 1.4.0, a tearsheet on every manager, the invariants the page relies on (change
     classes add up to the line counts, top lines ranked, lists consistent, the lines add up to the cover page,
     nothing reclassified is listed as new or absent), the size, and each manager's headline numbers;
  4. the cohort side of the same release: the re-registered issuers (Exxon under its new CUSIP) no longer read as
     new holders in the ticker table / contenders.
Invokes one engine (the book, a reader). Writes nothing but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE, BOOK = "us-east-1", "justhodl-dashboard-live", "justhodl-13f-book"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=330)
LISTS = ("initiations", "increases", "reductions", "terminations")


def money(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "-"
    a, s = abs(v), "-" if v < 0 else ""
    if a >= 1e9:
        return "%s$%.2fB" % (s, a / 1e9)
    if a >= 1e6:
        return "%s$%.1fM" % (s, a / 1e6)
    return "%s$%.0fK" % (s, a / 1e3)


def name(r):
    return "-" if not r else "%s%s" % (r.get("ticker") or (r.get("name") or r.get("issuer") or "")[:20], (" " + r["pc"].lower() + "s") if r.get("pc") else "")


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6821_13f_scope_tearsheet_first_light") as r:
        r.heading("ops 6821 - 13F book 1.5.0 first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. deployed code + one run")
        cf = lam.get_function_configuration(FunctionName=BOOK)
        r.log("%s: modified %s, memory %s, timeout %s, state %s, update %s, sha %s" % (BOOK, (cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), cf.get("LastUpdateStatus"), (cf.get("CodeSha256") or "")[:12]))
        before = s3.head_object(Bucket=LIVE, Key="data/13f-book.json")
        t0 = time.time()
        resp = lam.invoke(FunctionName=BOOK, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6821_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:700]))
            raise SystemExit(1)
        r.log("invoke %.1fs: %s" % (time.time() - t0, json.dumps(payload)))
        obj = s3.get_object(Bucket=LIVE, Key="data/13f-book.json")
        body = obj["Body"].read()
        book = json.loads(body)
        b = book["book"]
        m = b["managers"]
        r.log("book: version %s contract %s generated %s trigger %s run %ss | %d bytes (was %d) cache %s | period %s cohort %d/%d errors %d" % (
            book.get("version"), book.get("contract"), book.get("generated_at"), json.dumps(book.get("trigger")), book.get("run_seconds"), len(body), before["ContentLength"],
            obj.get("CacheControl"), b.get("required_period"), len(b.get("cohort") or []), len(m), len(b.get("errors") or [])))
        if book.get("version") != "1.5.0" or b.get("errors"):
            r.fail("the live book is not 1.5.0 or carries artifact errors (%s, %s) - was the function deployed?" % (book.get("version"), b.get("errors")))
            raise SystemExit(1)

        r.section("2. the tearsheet on every manager")
        bad = []
        for fk, x in sorted(m.items(), key=lambda kv: -((kv[1].get("tearsheet") or {}).get("value_usd") or 0)):
            t = x.get("tearsheet")
            if not t:
                bad.append("%s: no tearsheet" % fk)
                continue
            c, o, src = t["counts"], t["options"], t.get("source") or {}
            add, cut = t["notable"]["add"], t["notable"]["cut"]
            r.log("%-12s %s vs %s %-7s | 13F value %s (prior %s, %s%%) lines %d (prior %d) top-10 %s%% | new %d inc %d red %d absent %d flat %d reclass %d n/c %d | options %s%% | cover ok %s | add %s %s | cut %s %s | top %s" % (
                fk, x["current_period"], x["prior_period"], x["status"], money(t["value_usd"]), money(t["value_prior_usd"]), t["value_change_pct"], t["n_lines"], t["n_lines_prior"], t["top10_pct"],
                c["new"], c["increased"], c["reduced"], c["closed"], c["unchanged"], c["reclassified"], c["not_compared"], o["share_pct"], src.get("reconciles"),
                name(add), money((add or {}).get("est_usd")), name(cut), money((cut or {}).get("est_usd")), ", ".join("%s %s%%" % (name(z), z["pct"]) for z in t["lines"][:4])))
            if c["new"] + c["increased"] + c["reduced"] + c["unchanged"] + c["not_compared"] + c["reclassified"] != t["n_lines"]:
                bad.append("%s: change classes do not add up to the line count" % fk)
            if not c["not_compared"] and c["closed"] + c["reclassified_prior"] + c["increased"] + c["reduced"] + c["unchanged"] != t["n_lines_prior"]:
                bad.append("%s: change classes do not add up to the prior line count" % fk)
            if [z["value_usd"] for z in t["lines"]] != sorted((z["value_usd"] for z in t["lines"]), reverse=True):
                bad.append("%s: top lines not ranked" % fk)
            for lab in LISTS:
                L = t[lab]
                if L["n"] != len(L["rows"]) + len(L["others"]) + L["n_more"]:
                    bad.append("%s: %s rows + others + more != n" % (fk, lab))
            if t["reclassified"]["n"] != c["reclassified"] or any(z.get("reclass") for lab in ("initiations", "terminations") for z in t[lab]["rows"]):
                bad.append("%s: a reclassified line is listed as new or absent" % fk)
            if src.get("reconciles") is not True:
                bad.append("%s: lines do not reconcile to the cover page (%s, gap %s)" % (fk, src.get("reconciles"), src.get("cover_gap_usd")))
            if o["n_lines"] != x["n_option_rows"] or (not o["n_lines"] and not x["n_debt_rows"] and abs((t["value_usd"] or 0) - (x["total_value_usd"] or 0)) > 0.5):
                bad.append("%s: option lines / equity value disagree with the grid" % fk)
            for k in ("top", "newly_present", "absent", "increased", "decreased", "mix", "filings", "counts", "total_value_usd", "top10_pct_of_book"):
                if k not in x:
                    bad.append("%s: an existing field is gone: %s" % (fk, k))
        tear = sum(len(json.dumps(x.get("tearsheet"), separators=(",", ":"))) for x in m.values())
        r.log("tearsheets: %d of %d managers, %d bytes of the book" % (sum(1 for x in m.values() if x.get("tearsheet")), len(m), tear))

        r.section("2b. scope tearsheets, option lines, board totals (1.5.0)")
        for scope in ("cohort", "concentrated"):
            t = (b.get("tearsheet") or {}).get(scope)
            if not t:
                bad.append("%s: no scope tearsheet" % scope)
                continue
            c, mem = t["counts"], [m[fk]["tearsheet"] for fk in t["members"]]
            va = t["views"]["all"]
            add, cut = va["notable"]["add"], va["notable"]["cut"]
            r.log("%s: %d books | 13F value %s (prior %s, %s%%) | manager-lines %d | new %d inc %d red %d absent %d flat %d reclass %d | options %s%% in %d books | add %s %s %s | cut %s %s %s | books %s" % (
                scope, len(t["members"]), money(t["value_usd"]), money(t["value_prior_usd"]), t["value_change_pct"], t["n_lines"], c["new"], c["increased"], c["reduced"], c["closed"], c["unchanged"], c["reclassified"],
                t["options"]["share_pct"], t["options"]["n_managers"], (add or {}).get("fk"), name(add), money((add or {}).get("est_usd")), (cut or {}).get("fk"), name(cut), money((cut or {}).get("est_usd")),
                ", ".join("%s %s%%" % (x["fk"], x["pct"]) for x in t["by_manager"][:5])))
            if t["members"] != (b["cohort"] if scope == "cohort" else b["concentrated"]):
                bad.append("%s: members differ from the book's scope" % scope)
            if abs(t["value_usd"] - sum(x["value_usd"] for x in mem)) > 1 or t["n_lines"] != sum(x["n_lines"] for x in mem) or abs(va["value_usd"] - t["value_usd"]) > 1:
                bad.append("%s: totals are not the members' totals" % scope)
            if abs(t["views"]["non_option"]["value_usd"] + t["views"]["option"]["value_usd"] - va["value_usd"]) > 1:
                bad.append("%s: view values do not add up" % scope)
            for lab, key in (("initiations", "new"), ("increases", "increased"), ("reductions", "reduced"), ("terminations", "closed")):
                if va[lab]["n"] != c[key] or t["views"]["non_option"][lab]["n"] + t["views"]["option"][lab]["n"] != c[key]:
                    bad.append("%s: %s count disagrees with the lists" % (scope, key))
                for kind in ("all", "non_option", "option"):
                    L = t["views"][kind][lab]
                    if L["n"] != len(L["rows"]) + len(L["others"]) + L["n_more"] or [z["est_usd"] for z in L["rows"]] != sorted((z["est_usd"] for z in L["rows"]), reverse=True):
                        bad.append("%s/%s/%s: list not consistent or not ranked" % (scope, kind, lab))
                    if any(z["fk"] not in t["members"] or (kind != "all" and bool(z.get("pc")) != (kind == "option")) for z in L["rows"]):
                        bad.append("%s/%s/%s: a row of the wrong book or kind" % (scope, kind, lab))
            best = max((z for x in mem for lab in ("initiations", "increases") for z in x[lab]["rows"]), key=lambda z: z.get("est_usd") or 0, default=None)
            if best and abs((add or {}).get("est_usd", 0) - best["est_usd"]) > 0.5:
                bad.append("%s: the notable addition is not the members' largest" % scope)
            for u in ("all", "stocks", "etfs"):
                bd = b["boards"][scope][u]
                if not all(isinstance(bd.get(k), int) for k in ("n_newly_present", "n_dropped", "n_increased", "n_decreased")) or bd["n_newly_present"] < len(bd["newly_present"]):
                    bad.append("boards %s/%s: totals missing or below the rows" % (scope, u))
            bd = b["boards"][scope]["all"]
            r.log("boards %s/all: held %s | newly present %s dropped %s increased %s decreased %s | value %s prior %s" % (scope, bd["n_held"], bd.get("n_newly_present"), bd.get("n_dropped"), bd.get("n_increased"), bd.get("n_decreased"), money(bd["value_usd"]), money(bd.get("value_prior_usd"))))
        for fk, x in m.items():
            o = x["tearsheet"]["options"]
            if len(o.get("top_calls") or []) != min(3, o["n_calls"]) or len(o.get("top_puts") or []) != min(3, o["n_puts"]):
                bad.append("%s: largest option lines do not match the option counts" % fk)
        r.log("largest option lines: %s" % " | ".join("%s calls %s puts %s" % (fk, name((x["tearsheet"]["options"]["top_calls"] or [None])[0]), name((x["tearsheet"]["options"]["top_puts"] or [None])[0])) for fk, x in m.items() if x["tearsheet"]["options"]["n_lines"]))

        r.section("3. the cohort side: re-registered issuers")
        tk = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-tickers.json")["Body"].read())
        r.log("ticker table: version %s, %d tickers, generated %s | XOM row: %s | XPRO row: %s" % (tk.get("version"), tk.get("n_tickers"), tk.get("generated_at"),
                                                                                                json.dumps((tk.get("by_ticker") or {}).get("XOM")), json.dumps((tk.get("by_ticker") or {}).get("XPRO"))))
        for scope in ("cohort", "concentrated"):
            ct = b["contenders"][scope]["all"]
            r.log("contenders %s: rising %s | fading %s" % (scope, ", ".join(str(z.get("ticker") or z.get("issuer")) for z in ct.get("rising") or []), ", ".join(str(z.get("ticker") or z.get("issuer")) for z in ct.get("fading") or [])))
            en = next((z for z in b["sectors"][scope].get("sectors") or [] if z.get("name") == "Energy"), {})
            r.log("sector %s / Energy: inflow %s outflow %s net %s stance %s money %s" % (scope, money(en.get("inflow_usd")), money(en.get("outflow_usd")), money(en.get("net_usd")), en.get("stance"), en.get("money_stance")))
        rising = {z.get("ticker") for z in b["contenders"]["cohort"]["all"].get("rising") or []}
        if "XOM" in rising:
            bad.append("XOM still reads as a rising contender (new CUSIP counted as new holders)")
        r.kv(version=book.get("version"), book_bytes=str(len(body)), tearsheets=str(sum(1 for x in m.values() if x.get("tearsheet"))), invariant_failures=str(len(bad)), run_seconds=str(book.get("run_seconds")))
        if bad:
            for x in bad:
                r.fail(x)
            raise SystemExit(1)
        r.ok("FIRST LIGHT: book %s live with %d tearsheets, every invariant holds, %d bytes, run %ss" % (book.get("version"), len(m), len(body), book.get("run_seconds")))


if __name__ == "__main__":
    main()
    sys.exit(0)
