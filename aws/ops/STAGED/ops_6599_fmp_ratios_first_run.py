"""ops 6599 - after deploy-lambdas creates justhodl-fmp-ratios: declare its schedule in the deployed manifest (additions-
only, live definition verified, ops 6594 pattern), run the book refresh once now so the chart's Valuation / Financials
desks and the fleet's fmp_book stop serving the 2026-09-15 harvest today, verify the new book, and (read-only) read
why the TradingView bar bank did not advance past 2026-10-19 at this morning's 02:30 UTC universe refresh (the chart's
AAPL still shows Monday's bar on Wednesday morning).
"""
from datetime import datetime, timedelta, timezone
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
FN = "justhodl-fmp-ratios"
SCHED = "justhodl-fmp-ratios-daily"
BOOK = "data/fmp-ratios.json"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def insights(logs, group, query, hours, limit=40):
    now = datetime.now(timezone.utc)
    try:
        qid = logs.start_query(logGroupName=group, startTime=int((now - timedelta(hours=hours)).timestamp()), endTime=int(now.timestamp()), queryString=query, limit=limit)["queryId"]
    except Exception as e:  # noqa: BLE001
        return "start:%s" % type(e).__name__, []
    for _ in range(40):
        time.sleep(2)
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return r["status"], [{f["field"]: f["value"] for f in row} for row in r.get("results", [])]
    return "Running", []


def main():
    s3, lam, sch, logs = c("s3"), c("lambda"), c("scheduler"), c("logs")
    with report("ops_6599_fmp_ratios_first_run") as r:
        r.heading("ops 6599 - justhodl-fmp-ratios: declare, first run, verify; tv-bars refresh diagnosis")
        r.section("1. the function and its schedule exist (deploy-lambdas)")
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - run again after deploy-lambdas" % (FN, type(e).__name__))
            raise SystemExit(1)
        r.log("%s: runtime %s, memory %s, timeout %s, modified %s, state %s" % (FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), (cf.get("LastModified") or "")[:19], cf.get("State")))
        env_keys = sorted((cf.get("Environment") or {}).get("Variables", {}).keys())
        r.log("env keys: %s (values never printed)" % env_keys)
        if "FMP_KEY" not in env_keys:
            r.warn("FMP_KEY not in the function's env - equity_enrich falls back to SSM /justhodl/fmp/api-key")
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
        except Exception as e:  # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)

        r.section("2. deployed manifest := committed (additions-only, live-verified)")
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
            ok = lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of((lv.get("Target") or {}).get("Arn")) == fn_of(e["targets"][0]["arn"])
            if not ok:
                r.fail("declared %s does not match live (%s %s -> %s)" % (name, lv.get("ScheduleExpression"), lv.get("State"), fn_of((lv.get("Target") or {}).get("Arn"))))
                raise SystemExit(1)
        if added:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            if s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8") != repo_text:
                r.fail("read-back differs")
                raise SystemExit(1)
            r.ok("manifest uploaded (+%d), backup backups/schedule-manifest/%s/" % (len(added), stamp))
        else:
            r.ok("manifest already equal")

        r.section("3. first run now")
        try:
            prev = json.loads(s3.get_object(Bucket=LIVE, Key=BOOK)["Body"].read())
        except Exception:  # noqa: BLE001
            prev = {}
        prev_gen = prev.get("generated_at")
        r.log("book before: generated_at %s, %d tickers" % (prev_gen, len(prev.get("tickers") or {})))
        resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("async invoke status %s at %s" % (resp.get("StatusCode"), datetime.now(timezone.utc).strftime("%H:%M:%S")))
        new = None
        deadline = time.time() + 15 * 60
        while time.time() < deadline:
            time.sleep(30)
            try:
                h = s3.head_object(Bucket=LIVE, Key=BOOK)
                cand = json.loads(s3.get_object(Bucket=LIVE, Key=BOOK)["Body"].read())
                if cand.get("generated_at") != prev_gen:
                    new = (cand, h)
                    break
            except Exception:  # noqa: BLE001
                pass
        if not new:
            status, rows = insights(logs, "/aws/lambda/" + FN, 'fields @timestamp, @message | sort @timestamp desc | limit 30', 1, limit=30)
            r.log("log tail (%s): %s" % (status, json.dumps([x.get("@message", "")[:220] for x in rows])[:4000]))
            r.fail("the book was not rewritten within 15 min - see the log tail above (the 22:38 UTC schedule stands)")
            raise SystemExit(1)
        cand, h = new
        tk = cand.get("tickers") or {}
        fresh = sum(1 for v in tk.values() if isinstance(v, dict) and not v.get("stale_from"))
        stale = len(tk) - fresh
        aapl = tk.get("AAPL") or {}
        r.log("book after: generated_at %s, engine %s, %d bytes, %d tickers (%d fresh, %d kept from %s); universe %s; errors %d; key_status %s" % (
            cand.get("generated_at"), cand.get("engine"), h.get("ContentLength"), len(tk), fresh, stale, cand.get("prev_generated_at"), json.dumps(cand.get("universe")), len(cand.get("errors") or {}), json.dumps(cand.get("key_status"))))
        r.log("AAPL: as_of %s price %s pe %s ev_ebitda %s next_earnings %s financials years %s" % (aapl.get("as_of"), aapl.get("price"), aapl.get("pe"), aapl.get("ev_ebitda"), aapl.get("next_earnings"), [f.get("year") for f in (aapl.get("financials") or [])][-3:]))
        r.log("error sample: %s" % json.dumps(dict(list((cand.get("errors") or {}).items())[:15])))
        r.kv(step="fmp-ratios first run", before="%s / %d tickers" % (prev_gen, len(prev.get("tickers") or {})), after="%s / %d tickers (%d fresh)" % (cand.get("generated_at"), len(tk), fresh), note="schema %s" % cand.get("schema_version"))
        if cand.get("schema_version") != "fmp-ratios.v1" or fresh < 40:
            r.fail("book shape or freshness wrong (schema %s, fresh %d)" % (cand.get("schema_version"), fresh))
            raise SystemExit(1)
        # the shared reader sees it the way every engine will
        sys.path.insert(0, str(ROOT / "aws" / "shared"))
        import fmp_book  # noqa: E402
        fmp_book._CACHE["book"] = None
        rr = fmp_book.row("AAPL") or {}
        r.log("fmp_book.row('AAPL') via the shared reader: ticker %s as_of %s pe %s; as_of() %s; key_status() %s" % (rr.get("ticker"), rr.get("as_of"), rr.get("pe"), fmp_book.as_of(), fmp_book.key_status()))

        r.section("4. why the TradingView bar bank stopped at 2026-10-19 (read-only)")
        status, rows = insights(logs, "/aws/lambda/justhodl-tv-bars", 'fields @timestamp, @message | filter @message like /refresh|shard|fanned|Task timed out|Traceback|denied|Denied|refused|REPORT/ | sort @timestamp desc | limit 40', 14, limit=40)
        r.log("tv-bars log (14 h, %s): %s" % (status, json.dumps([(x.get("@timestamp", "")[11:19], x.get("@message", "")[:200].replace("\n", " ")) for x in rows])[:5000]))
        idx = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/tv-bars/universe/_index.json")["Body"].read())
        syms = idx.get("symbols") or {}
        from collections import Counter
        last_c = Counter((v.get("last") or "")[:10] for v in syms.values())
        asof_c = Counter((v.get("as_of") or "")[:10] for v in syms.values())
        r.log("index: updated_at %s n_symbols %s/%s; failures %d; last-bar dates %s; as_of dates %s" % (idx.get("updated_at"), idx.get("n_symbols"), len(syms), len(idx.get("failures") or {}), json.dumps(last_c.most_common(6)), json.dumps(asof_c.most_common(6))))
        recent_fail = sorted(((v.get("at") or ""), k, (v.get("err") or "")[:90]) for k, v in (idx.get("failures") or {}).items())[-8:]
        r.log("most recent failures: %s" % json.dumps(recent_fail))
        try:
            p = c("ssm")
            for name in ("/justhodl/tradingview/sessionid", "/justhodl/tradingview/auth_token"):
                md = p.get_parameter(Name=name, WithDecryption=False)["Parameter"]
                r.log("SSM %s: version %s last modified %s (value never read)" % (name, md.get("Version"), str(md.get("LastModifiedDate"))[:19]))
        except Exception as e:  # noqa: BLE001
            r.warn("ssm metadata: %s" % type(e).__name__)
        r.ok("fmp-ratios declared + refreshed (%d fresh rows); tv-bars diagnosis read" % fresh)


if __name__ == "__main__":
    main()
