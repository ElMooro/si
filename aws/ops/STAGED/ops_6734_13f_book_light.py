"""ops 6734 - 13F book light (re-runnable): invoke justhodl-13f-book once and report what the live book says - version,
security-master coverage, learned ETF classes, the review lists (unclassified ETFs, unmapped, 'other'), rotation, stance,
contenders, boards, manager mix, history - plus the day's invocation log lines. First used for 1.1.3 (Pfd sector).

Preconditions:
  1. the function carries today's code (LastModified after the commit);
  2. invoke the book synchronously (a root run under the trigger cap) - up to three passes while the security master
     still has never-checked identities (budget 100 OpenFIGI requests per run), reporting coverage after each;
  3. read data/13f-book.json and print, from the live data: master coverage + refresh stats, the ETFs the reviewed
     table does not know yet (to be added), the largest unmapped identities, the rotation table for both scopes, the
     stance boards (all / stocks / etfs), the contenders, the most widely held ETFs, two managers' asset mix, the
     period snapshot and timing; embed the smart-money block as JSON for offline reading.
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

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
BOOK = "justhodl-13f-book"
OUT, MASTER = "data/13f-book.json", "data/13f-security-master.json"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=330)
MAX_PASSES = 3


def money(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "-"
    a = abs(v)
    s = "-" if v < 0 else ""
    if a >= 1e9:
        return "%s$%.2fB" % (s, a / 1e9)
    if a >= 1e6:
        return "%s$%.0fM" % (s, a / 1e6)
    if a >= 1e3:
        return "%s$%.0fK" % (s, a / 1e3)
    return "%s$%.0f" % (s, a)


def row_line(r):
    v = r.get("votes") or {}
    return "%-7s %-6s %-40s holders %d (prior %d) votes +%d/-%d net %+d w %s value %s who+ %s who- %s%s" % (
        r.get("ticker") or "-", (r.get("kind") or "")[:6], (r.get("issuer") or "")[:40], r.get("n_holders") or 0, r.get("n_prior_holders") or 0,
        v.get("bull", 0), v.get("bear", 0), v.get("net", 0), v.get("weighted_net"), money(r.get("value_usd")),
        ",".join(r.get("new", []) + r.get("inc", [])) or "-", ",".join(r.get("absent", []) + r.get("dec", [])) or "-",
        " RECLASS?" if r.get("reclass_candidate") else "")


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6734_13f_book_light") as r:
        r.heading("ops 6734 - 13F book light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. deployed code")
        cf = lam.get_function_configuration(FunctionName=BOOK)
        r.log("%s: modified %s, memory %s, timeout %s, state %s, sha %s" % (BOOK, (cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("CodeSha256") or "")[:12]))
        r.log("code modified %s (a release pushed minutes ago may not have landed yet - compare with the version below)" % (cf.get("LastModified") or "")[:19])
        try:
            logs = boto3.client("logs", region_name=REGION, config=CFG)
            day0 = int(datetime(2026, 11, 4, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
            kw = {"logGroupName": "/aws/lambda/" + BOOK, "startTime": day0, "filterPattern": '"13f-book:"', "limit": 50}
            seen = 0
            while True:
                ev = logs.filter_log_events(**kw)
                for e in ev.get("events", []):
                    seen += 1
                    r.log("invocation %s: %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).strftime("%H:%M:%S"), e["message"].strip()[:220]))
                if not ev.get("nextToken") or seen > 200:
                    break
                kw["nextToken"] = ev["nextToken"]
            r.log("invocations logged today (UTC): %d" % seen)
        except Exception as exc:  # noqa: BLE001
            r.warn("log read not permitted (%s)" % type(exc).__name__)

        r.section("2. passes (security master fills by reported value, 100 requests per run)")
        resp = None
        for i in range(1, MAX_PASSES + 1):
            t0 = time.time()
            resp = lam.invoke(FunctionName=BOOK, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6727_first_light"}).encode())
            payload = json.loads(resp["Payload"].read() or b"{}")
            if resp.get("FunctionError"):
                r.fail("pass %d: FunctionError %s: %s" % (i, resp.get("FunctionError"), json.dumps(payload)[:600]))
                raise SystemExit(1)
            book = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
            sm = (book.get("book") or {}).get("security_master") or {}
            cov, rf = sm.get("coverage") or {}, sm.get("refresh") or {}
            lr = ((b0 := book.get("book") or {}).get("review") or {}).get("learned") or {}
            r.log("pass %d: %.0fs wall, handler %s | version %s contract %s | master %s/%s resolved (%.1f%% of value) typed-only %s (%s%% of value) no_match %s ambiguous %s error %s pending %s | refresh: due %s sent US %s any %s isin %s requests %s keyed %s changed %s partial %s %.0fs%s | learned: %s" % (
                i, time.time() - t0, json.dumps(payload), book.get("version"), book.get("contract"), cov.get("resolved"), cov.get("identities"), cov.get("resolved_value_pct") or 0.0,
                cov.get("typed_only"), cov.get("typed_only_value_pct"), cov.get("no_match"), cov.get("ambiguous"), cov.get("error"), cov.get("pending"), rf.get("due"), rf.get("sent_us"), rf.get("sent_any"), rf.get("sent_isin"),
                rf.get("requests"), rf.get("keyed"), rf.get("changed"), rf.get("partial"), rf.get("seconds") or 0.0, " DEADLINE" if rf.get("deadline_hit") else "", json.dumps(lr.get("last_run"))))
            if not cov.get("pending") and not rf.get("partial") and not rf.get("deadline_hit"):
                break
        if book.get("contract") != "13f-book.v2" or (book.get("book") or {}).get("errors"):
            r.fail("the live book is not v2 or carries artifact errors: %s %s" % (book.get("contract"), (book.get("book") or {}).get("errors")))
            raise SystemExit(1)
        b = book["book"]
        h = s3.head_object(Bucket=LIVE, Key=MASTER)
        r.log("master object: %s bytes, modified %s" % (h["ContentLength"], h["LastModified"].isoformat(timespec="seconds")))
        r.log("book object: %s bytes, run %ss, errors %s, trigger %s" % (len(json.dumps(book)), book.get("run_seconds"), len(b.get("errors") or []), json.dumps(book.get("trigger"))))

        r.section("3. review lists (feed the reviewed ETF table from these)")
        rv = b.get("review") or {}
        r.log("class sources over held fund-like identities: %s | learned object: %s rows, last run %s" % (json.dumps(rv.get("class_sources")), (rv.get("learned") or {}).get("n_rows"), json.dumps((rv.get("learned") or {}).get("last_run"))))
        r.log("unclassified ETFs: %d = %s (table %s, %s tickers)" % (rv.get("n_unclassified_etfs") or 0, money(rv.get("unclassified_value_usd")), rv.get("etf_table_reviewed"), rv.get("etf_table_size")))
        for u in (rv.get("unclassified_etfs") or [])[:80]:
            r.log("  %-7s %-45s %-16s holders %s value %s cusip %s fmp %s" % (u.get("ticker"), (u.get("name") or "")[:45], u.get("security_type"), u.get("n_holders"), money(u.get("value_usd")), u.get("cusip"), u.get("fmp_asset_class")))
        r.log("'other' (non-equity-sector shares): %d held; largest with their OpenFIGI sector:" % (rv.get("n_other") or 0))
        for o in (rv.get("other_top") or [])[:30]:
            r.log("  %-24s %-10s %-36s type %-22s sector %-8s holders %s value %s" % ((o.get("ticker") or "-")[:24], o.get("cusip"), (o.get("issuer") or "")[:36], o.get("security_type"), o.get("sector"), o.get("n_holders"), money(o.get("value_usd"))))
        try:
            master = json.loads(s3.get_object(Bucket=LIVE, Key=MASTER)["Body"].read())
            rows = master.get("by_cusip") or {}
            by = {}
            for c, row in rows.items():
                k = ("CINS" if c[:1].isalpha() else "CUSIP", row.get("status"), row.get("query"), bool(row.get("type_agreed")))
                by[k] = by.get(k, 0) + 1
            r.log("master rows by (shape, status, query, type_agreed): %s" % json.dumps(sorted(((" ".join(map(str, k)), v) for k, v in by.items()), key=lambda kv: -kv[1])))
        except Exception as exc:  # noqa: BLE001
            r.warn("master read failed: %s" % type(exc).__name__)
        r.log("unmapped identities held now: %d; largest:" % (rv.get("n_unmapped") or 0))
        for u in (rv.get("unmapped_top") or [])[:25]:
            r.log("  %-9s %-10s %-45s %-10s holders %s value %s" % (u.get("cusip"), (u.get("class") or "")[:10], (u.get("issuer") or "")[:45], u.get("master"), u.get("n_holders"), money(u.get("value_usd"))))

        for scope in ("cohort", "concentrated"):
            r.section("4. rotation - %s (%s)" % (scope, ", ".join(((b.get("scopes") or {}).get(scope) or {}).get("members") or [])))
            rot = (b.get("rotation") or {}).get(scope) or {}
            r.log("total now %s prior %s | identities %s | unmapped %.2f%% of value" % (money(rot.get("value_usd")), money(rot.get("value_prior_usd")), rot.get("n_identities"), rot.get("unmapped_value_pct") or 0.0))
            for x in rot.get("buckets") or []:
                p = x.get("pairs") or {}
                r.log("  %-18s now %9s (%5.2f%%) prior %9s (%5.2f%%) dpp %+6.2f vs prior %s | mgrs %2s ids %5s<-%-5s | new %4s absent %4s more %4s fewer %4s net %+5d %-13s | %s" % (
                    x["bucket"], money(x.get("value_usd")), x.get("share_pct") or 0.0, money(x.get("value_prior_usd")), x.get("share_prior_pct") or 0.0, x.get("share_delta_pp") or 0.0,
                    x.get("value_change_pct"), x.get("n_managers"), x.get("n_identities"), x.get("n_identities_prior"), p.get("new"), p.get("absent"), p.get("more"), p.get("fewer"), x.get("net_pairs") or 0, x.get("stance"),
                    ", ".join("%s %s/%s" % (t.get("name"), t.get("n_holders"), money(t.get("value_usd"))) for t in (x.get("top") or [])[:4])))

        for scope in ("cohort", "concentrated"):
            st_all = (b.get("stance") or {}).get(scope) or {}
            for univ in ("all", "stocks", "etfs"):
                st = st_all.get(univ) or {}
                r.section("5. stance - %s / %s: counts %s | bullish %s bearish %s contested %s" % (scope, univ, json.dumps(st.get("counts")), st.get("n_bullish"), st.get("n_bearish"), st.get("n_contested")))
                for lab in ("bullish", "bearish", "contested"):
                    rows = st.get(lab) or []
                    r.log("%s (%d shown):" % (lab.upper(), len(rows)))
                    for x in rows:
                        r.log("  " + row_line(x))

        for scope in ("cohort", "concentrated"):
            ct_all = (b.get("contenders") or {}).get(scope) or {}
            for univ in ("all", "stocks", "etfs"):
                ct = ct_all.get(univ) or {}
                r.section("6. contenders - %s / %s: rising %s (new to cohort %s) fading %s (left %s)" % (scope, univ, ct.get("n_rising"), ct.get("n_new_to_cohort"), ct.get("n_fading"), ct.get("n_left_cohort")))
                for lab in ("rising", "fading"):
                    for x in ct.get(lab) or []:
                        r.log("  %-6s %s%s" % (lab.upper(), row_line(x), " NEW-TO-COHORT" if x.get("new_to_cohort") else " LEFT-COHORT" if x.get("left_cohort") else ""))

        r.section("7. boards - cohort / etfs")
        bd = ((b.get("boards") or {}).get("cohort") or {}).get("etfs") or {}
        r.log("ETF identities %s, held %s, value %s" % (bd.get("n_identities"), bd.get("n_held"), money(bd.get("value_usd"))))
        for lab in ("widely_held", "largest_value", "newly_present", "dropped", "increased", "decreased"):
            r.log("%s:" % lab)
            for x in (bd.get(lab) or [])[:10]:
                r.log("  " + row_line(x))
        bd = ((b.get("boards") or {}).get("cohort") or {}).get("stocks") or {}
        r.log("STOCKS identities %s, held %s, value %s; widely held:" % (bd.get("n_identities"), bd.get("n_held"), money(bd.get("value_usd"))))
        for x in (bd.get("widely_held") or [])[:10]:
            r.log("  " + row_line(x))

        r.section("8. manager mix + history")
        for fk in ("BERKSHIRE", "ELLIOTT", "BRIDGEWATER", "SCION"):
            m = (b.get("managers") or {}).get(fk) or {}
            mix = m.get("mix") or {}
            r.log("%s %s: %s | unmapped %.2f%%" % (fk, m.get("current_period"), " | ".join("%s %.1f%%<-%.1f%% net%+d" % (x["bucket"], x.get("share_pct") or 0.0, x.get("share_prior_pct") or 0.0, x.get("net_pairs") or 0) for x in mix.get("buckets") or []), mix.get("unmapped_value_pct") or 0.0))
            top = (m.get("top") or [])[:5]
            r.log("  top: %s" % ", ".join("%s(%s %s)" % (x.get("ticker") or "-", x.get("kind"), (x.get("issuer") or "")[:18]) for x in top))
        hs = b.get("history") or {}
        r.log("history: periods %s | snapshot %s" % (json.dumps(hs.get("periods")), json.dumps(hs.get("snapshot"))))
        r.log("engines block: %s" % ", ".join("%s=%s" % (e["engine"].replace("justhodl-", ""), e["tier"]) for e in book.get("engines") or []))

        r.section("9. smart-money block (JSON, for offline reading)")
        block = {"security_master": b.get("security_master"), "review": rv, "rotation": b.get("rotation"), "stance": b.get("stance"), "contenders": b.get("contenders"),
                 "scopes": b.get("scopes"), "history": hs, "generated_at": book.get("generated_at"), "run_seconds": book.get("run_seconds")}
        r.log("```json\n%s\n```" % json.dumps(block, separators=(",", ":"), default=str))
        cov = (b.get("security_master") or {}).get("coverage") or {}
        r.kv(version=book.get("version"), master_resolved="%s/%s" % (cov.get("resolved"), cov.get("identities")), resolved_value_pct=str(cov.get("resolved_value_pct")),
             unclassified_etfs=str(rv.get("n_unclassified_etfs")), unmapped=str(rv.get("n_unmapped")), run_seconds=str(book.get("run_seconds")))
        r.ok("LIGHT: book %s live with a %s/%s-resolved master (%s%% of value, %s typed-only); %s ETFs still unclassified (%s)" % (
            book.get("version"), cov.get("resolved"), cov.get("identities"), cov.get("resolved_value_pct"), cov.get("typed_only"), rv.get("n_unclassified_etfs"), money(rv.get("unclassified_value_usd"))))


if __name__ == "__main__":
    main()
    sys.exit(0)
