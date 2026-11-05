"""ops 6755 - READ-ONLY check-in on the 13F book's automatic runs of 2026-11-05 (the self-scheduled follow-up to the
smart-money release: trigger trig_01ANcw4Sa8yPv1uKXK1UzYqu, 16:52 UTC).

Expected today: the native research publication at ~15:40 UTC handed off to justhodl-13f-book (trigger.kind
holdings_research_published; the 16:21 reconcile may hand off too) and the Scheduler justhodl-13f-book-daily ran at
16:37 UTC (trigger.kind scheduled). Reports, from the live objects and CloudWatch only (nothing is invoked):
  1. data/13f-book.json: HEAD (bytes, modified), version (1.3.0 since the strength desk shipped the per-ticker table on
     2026-11-04 - the check-in prompt's 1.1.3 predates it), contract, generated_at, run_seconds, the trigger block,
     security-master coverage (resolved / typed-only / unmapped % of value), the learned-classes stats, the unclassified
     ETF count / value, the rotation headline buckets, the tickers block (data/13f-tickers.json);
  2. data/13f-etf-classes-learned.json: n_rows (grows ~300 per run until the bank's ~1,296 products are banked) and
     the last run;
  3. every "13f-book:" CloudWatch line of 2026-11-05 UTC, paginated with nextToken, so each invocation is listed with
     its trigger; the Scheduler state; the research engine's hand-off log lines ("[13f] book hand-off") of the day;
  4. chain_guard refusals, if any, from the same log group.
Nothing is written but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
BOOK, RESEARCH, SCHED = "justhodl-13f-book", "justhodl-13f-positions", "justhodl-13f-book-daily"
OUT, LEARNED, TICKERS = "data/13f-book.json", "data/13f-etf-classes-learned.json", "data/13f-tickers.json"
DAY = datetime(2026, 11, 5, 0, 0, tzinfo=timezone.utc)
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def money(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "-"
    a, s = abs(v), "-" if v < 0 else ""
    return "%s$%.2fB" % (s, a / 1e9) if a >= 1e9 else "%s$%.0fM" % (s, a / 1e6) if a >= 1e6 else "%s$%.0fK" % (s, a / 1e3) if a >= 1e3 else "%s$%.0f" % (s, a)


def log_lines(logs, group, pattern, start_ms, limit=400):
    out, kw = [], {"logGroupName": group, "startTime": start_ms, "filterPattern": pattern, "limit": 100}
    while True:
        try:
            ev = logs.filter_log_events(**kw)
        except Exception as e:  # noqa: BLE001
            return out, "log read failed: %s" % type(e).__name__
        for e in ev.get("events", []):
            out.append((datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).strftime("%H:%M:%S"), (e.get("message") or "").strip()))
        if not ev.get("nextToken") or len(out) >= limit:
            return out, None
        kw["nextToken"] = ev["nextToken"]


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    sch = boto3.client("scheduler", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    problems = []
    with report("ops_6755_13f_book_auto_runs_checkin") as r:
        r.heading("ops 6755 - 13F book automatic runs of 2026-11-05 (read-only check-in) %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. the live book")
        h = s3.head_object(Bucket=LIVE, Key=OUT)
        book = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
        b = book.get("book") or {}
        r.log("%s: %s bytes, modified %s | version %s contract %s | generated %s | run %ss | trigger %s | errors %s" % (
            OUT, h["ContentLength"], h["LastModified"].isoformat(timespec="seconds"), book.get("version"), book.get("contract"), book.get("generated_at"), book.get("run_seconds"),
            json.dumps(book.get("trigger")), len(b.get("errors") or [])))
        sm = b.get("security_master") or {}
        cov, rf = sm.get("coverage") or {}, sm.get("refresh") or {}
        r.log("master: %s/%s resolved = %.2f%% of value | typed-only %s (%s%% of value) | no_match %s ambiguous %s error %s pending %s | unmapped %.2f%% of value | refresh: due %s requests %s changed %s partial %s deadline %s" % (
            cov.get("resolved"), cov.get("identities"), cov.get("resolved_value_pct") or 0.0, cov.get("typed_only"), cov.get("typed_only_value_pct"), cov.get("no_match"), cov.get("ambiguous"), cov.get("error"), cov.get("pending"),
            cov.get("unmapped_value_pct") or 0.0, rf.get("due"), rf.get("requests"), rf.get("changed"), rf.get("partial"), rf.get("deadline_hit")))
        rv = b.get("review") or {}
        lr = rv.get("learned") or {}
        r.log("review: class sources %s | learned rows %s last run %s | unclassified ETFs %s = %s (table %s, %s tickers) | unmapped held %s | other %s" % (
            json.dumps(rv.get("class_sources")), lr.get("n_rows"), json.dumps(lr.get("last_run")), rv.get("n_unclassified_etfs"), money(rv.get("unclassified_value_usd")), rv.get("etf_table_reviewed"), rv.get("etf_table_size"), rv.get("n_unmapped"), rv.get("n_other")))
        for scope in ("cohort", "concentrated"):
            rot = (b.get("rotation") or {}).get(scope) or {}
            r.log("rotation %s: now %s prior %s identities %s | %s" % (scope, money(rot.get("value_usd")), money(rot.get("value_prior_usd")), rot.get("n_identities"),
                  "; ".join("%s %s (%s%%, %s)" % (x.get("bucket") or x.get("klass") or x.get("class"), money(x.get("value_usd")), x.get("share_pct"), x.get("word") or x.get("stance")) for x in (rot.get("buckets") or [])[:8])))
        tk = b.get("tickers") or {}
        r.log("tickers block: %s" % json.dumps(tk)[:400])
        season = b.get("season") or {}
        r.log("season: %s" % json.dumps({k: season.get(k) for k in ("period", "next_period", "deadline", "filed", "n_filed", "n_cohort", "detector")})[:500])
        if book.get("contract") != "13f-book.v2":
            problems.append("contract %s" % book.get("contract"))
        gen = book.get("generated_at") or ""
        if not gen.startswith("2026-11-05"):
            problems.append("the live book was not generated today (generated_at %s)" % gen)
        try:
            ht = s3.head_object(Bucket=LIVE, Key=TICKERS)
            r.log("%s: %s bytes, modified %s" % (TICKERS, ht["ContentLength"], ht["LastModified"].isoformat(timespec="seconds")))
        except Exception as e:  # noqa: BLE001
            problems.append("%s unreadable: %s" % (TICKERS, type(e).__name__))

        r.section("2. learned ETF classes")
        try:
            hl = s3.head_object(Bucket=LIVE, Key=LEARNED)
            learned = json.loads(s3.get_object(Bucket=LIVE, Key=LEARNED)["Body"].read())
            rows = learned.get("rows") or learned.get("by_ticker") or learned.get("classes") or {}
            n = learned.get("n_rows") or (len(rows) if hasattr(rows, "__len__") else None)
            r.log("%s: %s bytes, modified %s | n_rows %s | last run %s | keys %s" % (LEARNED, hl["ContentLength"], hl["LastModified"].isoformat(timespec="seconds"), n, json.dumps(learned.get("last_run"))[:300], sorted(learned.keys())[:10]))
        except Exception as e:  # noqa: BLE001
            r.warn("learned classes unreadable: %s" % type(e).__name__)

        r.section("3. today's invocations (CloudWatch, paginated) and the Scheduler")
        day0 = int(DAY.timestamp() * 1000)
        lines, err = log_lines(logs, "/aws/lambda/" + BOOK, '"13f-book:"', day0)
        if err:
            r.warn(err)
        for t, m in lines:
            r.log("  %s %s" % (t, m[:240]))
        kinds = {}
        for t, m in lines:
            mm = re.search(r"trigger[\"']?[:=]\s*[\"']?(\w+)", m)
            if mm:
                kinds[mm.group(1)] = kinds.get(mm.group(1), 0) + 1
        r.log("book lines today: %d | trigger kinds seen: %s" % (len(lines), json.dumps(kinds)))
        starts, _ = log_lines(logs, "/aws/lambda/" + BOOK, '"START RequestId"', day0, limit=50)
        r.log("book START lines today: %d -> %s" % (len(starts), ", ".join(t for t, _ in starts)))
        hand, err2 = log_lines(logs, "/aws/lambda/" + RESEARCH, '"book hand-off"', day0, limit=50)
        if err2:
            r.warn(err2)
        for t, m in hand:
            r.log("  research %s %s" % (t, m[:240]))
        r.log("research hand-off lines today: %d" % len(hand))
        ref, _ = log_lines(logs, "/aws/lambda/" + BOOK, '"chain_guard"', day0, limit=50)
        ref2, _ = log_lines(logs, "/aws/lambda/" + RESEARCH, '"chain_guard"', day0, limit=50)
        refusals = [(t, m) for t, m in ref + ref2 if re.search(r"refus|deny|denied|cap", m, re.I)]
        for t, m in refusals[:20]:
            r.log("  chain_guard %s %s" % (t, m[:240]))
        r.log("chain_guard refusal-shaped lines today: %d" % len(refusals))
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule %s: %s %s tz %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone")))
            if live.get("State") != "ENABLED":
                problems.append("schedule %s is %s" % (SCHED, live.get("State")))
        except Exception as e:  # noqa: BLE001
            problems.append("schedule %s unreadable: %s" % (SCHED, type(e).__name__))
        cf = lam.get_function_configuration(FunctionName=BOOK)
        r.log("%s: modified %s state %s sha %s" % (BOOK, (cf.get("LastModified") or "")[:19], cf.get("State"), (cf.get("CodeSha256") or "")[:12]))
        n_starts = len(starts)
        sched_seen = any("scheduled" in m for _, m in lines) or any(t >= "16:37" for t, _ in starts)
        hand_seen = any("holdings_research_published" in m for _, m in lines) or bool(hand)
        if n_starts == 0:
            problems.append("no book invocation logged today")
        if not sched_seen:
            problems.append("no scheduled (16:37 UTC) run seen")
        if not hand_seen:
            problems.append("no hand-off from the native research seen")
        r.kv(version=str(book.get("version")), generated=str(book.get("generated_at")), trigger=str((book.get("trigger") or {}).get("kind")), starts=str(n_starts), scheduled=str(sched_seen), handoff=str(hand_seen),
             resolved_pct=str(cov.get("resolved_value_pct")), learned_rows=str(lr.get("n_rows")), unclassified=str(rv.get("n_unclassified_etfs")), problems=str(len(problems)))
        if problems:
            r.fail("CHECK-IN FOUND PROBLEMS: %s" % "; ".join(problems))
            raise SystemExit(1)
        r.ok("13F book automatic runs verified for 2026-11-05: %d invocations (scheduled %s, hand-off %s), live book %s generated %s by trigger %s" % (
            n_starts, sched_seen, hand_seen, book.get("version"), book.get("generated_at"), (book.get("trigger") or {}).get("kind")))


if __name__ == "__main__":
    main()
    sys.exit(0)
