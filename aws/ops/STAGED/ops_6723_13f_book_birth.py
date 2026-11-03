"""ops 6723 - justhodl-13f-book birth: declare, first light, read-back (Khalid 2026-11-03: "Ship it").

After deploy-lambdas ships justhodl-13f-book v1.0.0 (its config creates the Scheduler schedule justhodl-13f-book-daily,
16:37 UTC):
  1. function + schedule exist and match the config;
  2. deployed schedule manifest := committed (additions-only, live-verified) - the reconciler's source of truth;
  3. first light: one async invoke, wait for data/13f-book.json to appear/refresh, read it back: managers, cohort,
     boards, season, clone-alpha join, engines block; every section the page renders must be present;
  4. the complete book JSON is embedded in this report so the page can be exercised offline against the real feed
     before it is pushed (the sandbox cannot reach justhodl.ai).
Direct lane (STAGED): idempotent, no serial twin, invokes only the book engine (a reader).
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED = "justhodl-13f-book", "justhodl-13f-book-daily"
OUT = "data/13f-book.json"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def head(s3, key):
    try:
        h = s3.head_object(Bucket=LIVE, Key=key)
        return h["LastModified"], h["ContentLength"]
    except Exception:  # noqa: BLE001
        return None, None


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    with report("ops_6723_13f_book_birth") as r:
        r.heading("ops 6723 - justhodl-13f-book: declare, first light, read-back %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. function + schedule")
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - dispatch deploy-lambdas function=%s, then run again" % (FN, type(e).__name__, FN))
            raise SystemExit(1)
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s, env keys %s" % (
            FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19],
            sorted((cf.get("Environment") or {}).get("Variables", {}).keys())))
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-13f-book/config.json").read_text(encoding="utf-8"))["eventbridge_scheduler"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
            if live.get("ScheduleExpression") != cfg["cron"] or fn_of((live.get("Target") or {}).get("Arn")) != FN:
                r.fail("live schedule differs from config (%s vs %s)" % (live.get("ScheduleExpression"), cfg["cron"]))
                raise SystemExit(1)
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)

        r.section("2. deployed schedule manifest := committed (additions-only)")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        r.log("S3 %d entries, repo %d; added %s removed %d changed %d" % (len(a), len(b), added, len(removed), len(changed)))
        if removed or changed:
            r.fail("not additions-only: removed %s changed %s" % (removed[:5], changed[:5]))
            raise SystemExit(1)
        for kind, name in added:
            e = b[(kind, name)]
            if kind != "scheduler":
                r.fail("unexpected added entry %s %s" % (kind, name))
                raise SystemExit(1)
            lv = sch.get_schedule(Name=name, GroupName=e.get("group") or "default")
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of((lv.get("Target") or {}).get("Arn")) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s does not match live" % name)
                raise SystemExit(1)
        if added:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.ok("manifest uploaded (+%d), backup backups/schedule-manifest/%s/" % (len(added), stamp))
        else:
            r.ok("manifest already equal")

        r.section("3. first light")
        prev_mod, prev_len = head(s3, OUT)
        r.log("before: %s %s bytes" % (prev_mod.isoformat(timespec="seconds") if prev_mod else "absent", prev_len))
        resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("async invoke %s at %s" % (resp.get("StatusCode"), datetime.now(timezone.utc).strftime("%H:%M:%S")))
        book, mod, size = None, None, None
        t_end = time.time() + 6 * 60
        while time.time() < t_end:
            time.sleep(10)
            mod, size = head(s3, OUT)
            if mod and mod != prev_mod:
                break
        if not (mod and mod != prev_mod):
            r.fail("no new %s within 6 min - read CloudWatch /aws/lambda/%s" % (OUT, FN))
            raise SystemExit(1)
        book = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
        bk = book.get("book") or {}
        m = bk.get("managers") or {}
        r.log("book: %d bytes, generated_at %s, contract %s, run %ss, errors %s" % (size, book.get("generated_at"), book.get("contract"), book.get("run_seconds"), bk.get("errors")))
        r.log("required period %s | cohort %d %s | concentrated %s | hedge books %s | cohort equity $%.1fB | counts %s" % (
            bk.get("required_period"), len(bk.get("cohort") or []), bk.get("cohort"), bk.get("concentrated"), bk.get("hedge_books"),
            (bk.get("cohort_equity_value_usd") or 0) / 1e9, bk.get("cohort_counts")))
        for fk in sorted(m):
            x = m[fk]
            r.log("  %s %s %s | eq %s opt %s | $%.2fB (%s%%) | top10 %s | new %s abs %s up %s dn %s | clone %s %s" % (
                fk, x.get("status"), x.get("flags"), x.get("n_equity_positions"), x.get("n_option_rows"), (x.get("total_value_usd") or 0) / 1e9,
                x.get("value_change_pct"), x.get("top10_pct_of_book"), x.get("n_newly_present_equity"), x.get("n_absent_equity"), x.get("n_increased_equity"),
                x.get("n_decreased_equity"), (x.get("clone") or {}).get("skill_score"), (x.get("clone") or {}).get("label")))
        for scope in ("cohort", "concentrated"):
            bd = (bk.get("boards") or {}).get(scope) or {}
            r.log("boards.%s: identities %s | %s" % (scope, bd.get("n_identities"), "; ".join("%s %d" % (k, len(bd.get(k) or [])) for k in ("widely_held", "largest_value", "newly_present", "dropped", "increased", "decreased"))))
            r.log("  newly_present top3: %s" % "; ".join("%s (%s) n%d%s" % (x["issuer"], x["cusip"], x["n_new"], " RECLASS?" if x.get("reclass_candidate") else "") for x in (bd.get("newly_present") or [])[:3]))
            r.log("  dropped top3: %s" % "; ".join("%s (%s) a%d%s%s" % (x["issuer"], x["cusip"], x["n_absent"], " NO-HOLDER" if x.get("no_current_holder") else "", " RECLASS?" if x.get("reclass_candidate") else "") for x in (bd.get("dropped") or [])[:3]))
        se = book.get("season") or {}
        r.log("season: next %s deadline %s days %s filed %s/%s detector %s new %s" % (se.get("next_period"), se.get("deadline"), se.get("days_to_deadline"), se.get("n_filed_for_next"), se.get("tracked"),
                                                                                    (se.get("detector") or {}).get("generated_at"), len((se.get("detector") or {}).get("new_filings") or [])))
        ca = book.get("clone_alpha") or {}
        r.log("clone-alpha: as_of %s %s ranked %d top %s" % (ca.get("as_of"), ca.get("status"), len(ca.get("ranked") or []), [(x["fund_key"], x.get("skill_score")) for x in (ca.get("ranked") or [])[:4]]))
        r.log("price-divergence: %s | ai brief: %s %s" % ((book.get("price_divergence") or {}).get("state"), (book.get("ai_brief") or {}).get("portfolio_action"), (book.get("ai_brief") or {}).get("model")))
        for e in book.get("engines") or []:
            r.log("engine %s: tier %s newest %s h | %s" % (e["engine"], e.get("tier"), e.get("newest_age_h"), "; ".join("%s %s" % (o["key"].split("/")[-1], o.get("tier")) for o in e.get("objects") or [])))
        missing = [k for k in ("book", "season", "clone_alpha", "price_divergence", "ai_brief", "engines", "limitations", "quality") if k not in book]
        if missing or not m or not bk.get("cohort"):
            r.fail("book incomplete: missing %s managers %d cohort %s" % (missing, len(m), bk.get("cohort")))
            raise SystemExit(1)
        r.kv(managers=len(m), cohort=len(bk.get("cohort") or []), bytes=size, run_seconds=book.get("run_seconds"), errors=len(bk.get("errors") or []))

        r.section("4. complete book JSON (for the offline page check)")
        text = json.dumps(book, separators=(",", ":"), ensure_ascii=False, default=str)
        r.log("payload bytes: %d" % len(text))
        r.lines.append("")
        r.lines.append("```json")
        r.lines.append(text)
        r.lines.append("```")
        r.lines.append("")
        r.ok("ops 6723 complete - the book is live at %s; next: push 13f.html and verify it in headless Chrome (ops 6724)" % OUT)


if __name__ == "__main__":
    main()
    sys.exit(0)
