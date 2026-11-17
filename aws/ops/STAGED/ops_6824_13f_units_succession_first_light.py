"""ops 6824 - 13F book 1.6.0 first light (re-runnable): value units + CUSIP successions, live.

Khalid 2026-11-17: "Fix these: Baupost shows as $5M because of its units, and Exxon has no row in the ticker table
this quarter."

  1. the function carries the new code; invoke justhodl-13f-book once, synchronously (a root run under the trigger
     cap) - it reads the native research artifacts and rewrites data/13f-book.json + data/13f-tickers.json + the
     period snapshot, as it does every day;
  2. value units: Baupost's table is judged 'thousands' in both periods against the other books and reads x1,000 -
     a $5B book that adds up to its cover page; every other book is 'dollars' (or has no peer at its period);
  3. every manager's tearsheet still holds its invariants and still adds up to its cover page;
  4. successions: every merged identity has holders on both sides that add up, no votes without a verified share
     basis, and is no longer flagged or listed as newly present / dropped;
  5. the LIVE ticker table has Exxon's row again - holders now and before - and the period snapshot is on the new basis.
Invokes one engine (the book, a reader). Writes nothing but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
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
WANT = "1.6.0"
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


def read_json(s3, key):
    obj = s3.get_object(Bucket=LIVE, Key=key)
    body = obj["Body"].read()
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return json.loads(body), obj, len(body)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6824_13f_units_succession_first_light") as r:
        r.heading("ops 6824 - 13F book 1.6.0 first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. deployed code + one run")
        cf = lam.get_function_configuration(FunctionName=BOOK)
        r.log("%s: modified %s, memory %s, timeout %s, state %s, update %s, sha %s" % (BOOK, (cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), cf.get("LastUpdateStatus"),
                                                                                   (cf.get("CodeSha256") or "")[:12]))
        before = s3.head_object(Bucket=LIVE, Key="data/13f-book.json")
        tk_before, _, _ = read_json(s3, "data/13f-tickers.json")
        t0 = time.time()
        resp = lam.invoke(FunctionName=BOOK, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6824_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:700]))
            raise SystemExit(1)
        r.log("invoke %.1fs: %s" % (time.time() - t0, json.dumps(payload)[:900]))
        book, obj, n_bytes = read_json(s3, "data/13f-book.json")
        b = book["book"]
        m = b["managers"]
        r.log("book: version %s contract %s generated %s run %ss | %d bytes (was %d) cache %s | period %s cohort %d/%d errors %d" % (
            book.get("version"), book.get("contract"), book.get("generated_at"), book.get("run_seconds"), n_bytes, before["ContentLength"], obj.get("CacheControl"), b.get("required_period"),
            len(b.get("cohort") or []), len(m), len(b.get("errors") or [])))
        if book.get("version") != WANT or b.get("errors"):
            r.fail("the live book is not %s or carries artifact errors (%s, %s) - was the function deployed?" % (WANT, book.get("version"), b.get("errors")))
            raise SystemExit(1)
        bad = []

        r.section("2. value units")
        scaled = []
        for fk in sorted(m):
            u = m[fk].get("units") or {}
            cur, pri = u.get("current") or {}, u.get("prior") or {}
            r.log("%-13s current %s %s x%s (%s of %s dollars, %s thousands) | prior %s %s x%s (%s of %s dollars, %s thousands) | flags %s" % (
                fk, cur.get("period"), cur.get("status"), cur.get("factor"), cur.get("n_dollars"), cur.get("n_compared"), cur.get("n_thousands"), pri.get("period"), pri.get("status"), pri.get("factor"),
                pri.get("n_dollars"), pri.get("n_compared"), pri.get("n_thousands"), [x for x in m[fk]["flags"] if x.startswith("UNITS_")]))
            if "unverified" in (cur.get("status"), pri.get("status")):
                bad.append("%s: value unit unverified" % fk)
            if 1000 in (cur.get("factor"), pri.get("factor")):
                scaled.append(fk)
            if b["units"].get(fk, {}).get("current") is None and cur:
                bad.append("%s: book.units lacks the verdict" % fk)
        if scaled != ["BAUPOST"]:
            bad.append("read x1,000: %s (expected Baupost alone)" % scaled)
        x = m["BAUPOST"]
        t, src = x["tearsheet"], x["tearsheet"]["source"]
        r.log("**BAUPOST: 13F value %s (prior %s, %s%%) | equity value %s | cover page as filed %s x%s = %s, gap %s, reconciles %s | status %s flags %s | in cohort %s**" % (
            money(t["value_usd"]), money(t["value_prior_usd"]), t["value_change_pct"], money(x["total_value_usd"]), (src.get("current") or {}).get("reported_value_sum"), src.get("cover_scale"), money(src.get("cover_value_sum")),
            money(src.get("cover_gap_usd")), src.get("reconciles"), x["status"], x["flags"], x.get("cohort")))
        r.log("    top lines: %s" % " · ".join("%s %s %s%%" % (z.get("ticker") or z.get("issuer"), money(z["value_usd"]), z.get("pct")) for z in t["lines"][:8]))
        if not (4e9 < t["value_usd"] < 6e9) or src.get("reconciles") is not True or src.get("cover_scale") != 1000 or "UNITS_THOUSANDS" not in x["flags"]:
            bad.append("Baupost does not read as a ~$5B book that adds up to its cover page: %s %s %s %s" % (t["value_usd"], src.get("reconciles"), src.get("cover_scale"), x["flags"]))
        if (x["units"]["current"] or {}).get("status") != "thousands" or (x["units"]["prior"] or {}).get("status") != "thousands":
            bad.append("Baupost: both periods must be judged thousands: %s" % json.dumps(x["units"]))
        byman = {z["fk"]: z for z in b["tearsheet"]["cohort"]["by_manager"]}
        r.log("    in the cohort tearsheet: %s = %s%% of the cohort's 13F value %s" % (money(byman["BAUPOST"]["value_usd"]), byman["BAUPOST"]["pct"], money(b["tearsheet"]["cohort"]["value_usd"])))

        r.section("3. every manager's tearsheet")
        for fk, x in sorted(m.items(), key=lambda kv: -((kv[1].get("tearsheet") or {}).get("value_usd") or 0)):
            t = x["tearsheet"]
            c, src = t["counts"], t.get("source") or {}
            r.log("%-13s %s vs %s %-7s | 13F value %s (prior %s) lines %d | new %d inc %d red %d absent %d flat %d reclass %d | cover ok %s (x%s)" % (
                fk, x["current_period"], x["prior_period"], x["status"], money(t["value_usd"]), money(t["value_prior_usd"]), t["n_lines"], c["new"], c["increased"], c["reduced"], c["closed"], c["unchanged"],
                c["reclassified"], src.get("reconciles"), src.get("cover_scale")))
            if c["new"] + c["increased"] + c["reduced"] + c["unchanged"] + c["not_compared"] + c["reclassified"] != t["n_lines"]:
                bad.append("%s: change classes do not add up to the line count" % fk)
            for lab in LISTS:
                L = t[lab]
                if L["n"] != len(L["rows"]) + len(L["others"]) + L["n_more"]:
                    bad.append("%s: %s rows + others + more != n" % (fk, lab))
            if src.get("reconciles") is not True:
                bad.append("%s: lines do not reconcile to the cover page (%s, gap %s)" % (fk, src.get("reconciles"), src.get("cover_gap_usd")))
            for k in ("top", "newly_present", "absent", "increased", "decreased", "mix", "filings", "counts", "total_value_usd", "top10_pct_of_book", "units"):
                if k not in x:
                    bad.append("%s: a field is gone: %s" % (fk, k))
        for scope in ("cohort", "concentrated"):
            t = b["tearsheet"][scope]
            mem = [m[fk]["tearsheet"] for fk in t["members"]]
            if abs(t["value_usd"] - sum(z["value_usd"] for z in mem)) > 1 or t["n_lines"] != sum(z["n_lines"] for z in mem):
                bad.append("%s: tearsheet totals are not the members' totals" % scope)
            r.log("%s tearsheet: %d members, 13F value %s (prior %s), %d lines" % (scope, len(t["members"]), money(t["value_usd"]), money(t["value_prior_usd"]), t["n_lines"]))

        r.section("4. successions - one security under a new CUSIP or class text")
        for scope in ("cohort", "concentrated"):
            rows = b["successions"][scope]
            basis = {}
            for z in rows:
                basis[z["succession"]["basis"] or "not compared"] = basis.get(z["succession"]["basis"] or "not compared", 0) + 1
            r.log("**%s: %d merged** (%s)" % (scope, len(rows), json.dumps(basis, sort_keys=True)))
            for z in rows[:14 if scope == "cohort" else 5]:
                x = z["succession"]
                r.log("%-6s %s -> %s | unit price x%s %s | holders %d (prior %d) | new %s absent %s more %s fewer %s flat %d | value %s (prior %s) net est %s | %s" % (
                    z["ticker"] or "-", x["from_cusip"] if not x["same_cusip"] else "class %s" % x["from_class"], z["cusip"] if not x["same_cusip"] else z["class"], x["unit_price_ratio"],
                    x["basis"] or "QTY NOT COMPARED", z["n_holders"], z["n_prior_holders"], z["new"], z["absent"], z["inc"], z["dec"], z["n_flat"], money(z["value_usd"]), money(z["value_prior_usd"]), money(z["net_usd"]), z["stance"]))
            for z in rows:
                x = z["succession"]
                if not z["n_holders"] or not z["n_prior_holders"] or z["reclass_candidate"]:
                    bad.append("%s/%s: no holder on one side, or still flagged" % (scope, z["ticker"] or z["cusip"]))
                if not x["basis_verified"] and (z["n_inc"] or z["n_dec"]):
                    bad.append("%s/%s: votes without a verified share basis" % (scope, z["ticker"] or z["cusip"]))
                if x["n_continuing"] != z["n_inc"] + z["n_dec"] + z["n_flat"] or z["n_holders"] != x["n_continuing"] + z["n_new"] or z["n_prior_holders"] != x["n_continuing"] + z["n_absent"]:
                    bad.append("%s/%s: holders do not add up" % (scope, z["ticker"] or z["cusip"]))
            merged = {z["id"] for z in rows}
            for u in ("all", "stocks", "etfs"):
                bd = b["boards"][scope][u]
                for k in ("newly_present", "dropped"):
                    if any(z["id"] in merged for z in bd.get(k) or []):
                        bad.append("boards %s/%s/%s lists a merged identity" % (scope, u, k))
            bd = b["boards"][scope]["all"]
            r.log("boards %s/all: identities %s held %s | newly present %s dropped %s increased %s decreased %s | value %s prior %s" % (
                scope, bd["n_identities"], bd["n_held"], bd["n_newly_present"], bd["n_dropped"], bd["n_increased"], bd["n_decreased"], money(bd["value_usd"]), money(bd["value_prior_usd"])))
        xom = next((z for z in b["successions"]["cohort"] if z["ticker"] == "XOM"), None)
        if not xom or xom["n_holders"] < 5 or xom["n_prior_holders"] < 5 or not xom["succession"]["basis_verified"]:
            bad.append("Exxon is not one identity with holders on both sides: %s" % json.dumps(xom)[:300])
        sec = {z["name"]: z for z in b["sectors"]["cohort"]["sectors"]}
        r.log("cohort Energy sector: value %s (prior %s) | in %s out %s net %s | %s" % (money(sec["Energy"]["value_usd"]), money(sec["Energy"]["value_prior_usd"]), money(sec["Energy"]["inflow_usd"]), money(sec["Energy"]["outflow_usd"]),
                                                                                  money(sec["Energy"]["net_usd"]), sec["Energy"]["stance"]))

        r.section("5. the live ticker table + the period snapshot")
        tk, tobj, tbytes = read_json(s3, "data/13f-tickers.json")
        cols = tk["columns"]
        gained = sorted(set(tk["by_ticker"]) - set(tk_before["by_ticker"]))
        lost = sorted(set(tk_before["by_ticker"]) - set(tk["by_ticker"]))
        r.log("data/13f-tickers.json: %s generated %s period %s | %d tickers (%d before this run) %d bytes cache %s | gained %d: %s | lost %d: %s" % (
            tk.get("contract"), tk.get("generated_at"), tk.get("period"), len(tk["by_ticker"]), len(tk_before["by_ticker"]), tbytes, tobj.get("CacheControl"), len(gained), gained[:60], len(lost), lost))
        for t in ("XOM", "XPRO", "TEVA", "OKE", "GTES", "FWONK", "WVE", "NFE"):
            row = dict(zip(cols, tk["by_ticker"][t])) if t in tk["by_ticker"] else None
            if not row:
                bad.append("%s has no row in the live ticker table" % t)
                continue
            r.log("%-6s holders %s (prior %s, delta %s) | value %s (prior %s) | in %s out %s net %s | bull %s bear %s %s | %s / %s" % (
                t, row["n_holders"], row["n_prior_holders"], row["holder_delta"], money(row["value_usd"]), money(row["value_prior_usd"]), money(row["inflow_usd"]), money(row["outflow_usd"]), money(row["net_usd"]),
                row["bull"], row["bear"], row["stance"], row.get("sector"), row.get("industry")))
        x = dict(zip(cols, tk["by_ticker"].get("XOM") or []))
        if not x or x["n_holders"] < 5 or x["n_prior_holders"] < 5 or not x["value_prior_usd"]:
            bad.append("Exxon's live row does not carry both quarters: %s" % x)
        if lost:
            bad.append("the ticker table lost rows: %s" % lost)
        snap_info = b["history"]["snapshot"]
        snap, _, sbytes = read_json(s3, snap_info["key"])
        sx = next((v for v in snap["by_identity"].values() if v[0] == "XOM"), None)
        r.log("period snapshot %s: %s | basis %s identities %s bytes %d | XOM %s" % (snap_info["key"], json.dumps({k: snap_info[k] for k in ("written", "reason")}), snap.get("basis"), snap.get("n_identities"), sbytes, sx))
        if snap.get("basis") != 2 or not sx or sx[3] < 5 or sx[5] < 5:
            bad.append("the period snapshot is not on the new basis or lacks Exxon's two sides: basis %s, XOM %s" % (snap.get("basis"), sx))

        r.kv(version=book.get("version"), book_bytes=str(n_bytes), managers=str(len(m)), read_x1000=",".join(scaled) or "none", baupost_13f_value=money(m["BAUPOST"]["tearsheet"]["value_usd"]),
             successions_cohort=str(len(b["successions"]["cohort"])), tickers=str(len(tk["by_ticker"])), xom_holders="%s (prior %s)" % (x.get("n_holders"), x.get("n_prior_holders")), invariant_failures=str(len(bad)))
        if bad:
            for z in bad:
                r.fail(z)
            raise SystemExit(1)
        r.ok("LIVE: 13f-book %s - Baupost reads %s and adds up to its cover page; %d successions are one identity each; Exxon has its row in the ticker table (%s holders, %s before)" % (
            book.get("version"), money(m["BAUPOST"]["tearsheet"]["value_usd"]), len(b["successions"]["cohort"]), x.get("n_holders"), x.get("n_prior_holders")))


if __name__ == "__main__":
    main()
    sys.exit(0)
