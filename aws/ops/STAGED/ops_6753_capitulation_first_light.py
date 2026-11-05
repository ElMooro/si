"""ops 6753 - the capitulation desk's first light (Khalid 2026-11-05: "Capitulation · panic volume at the lows which is
right on chart.html under capitulation. audit it and see if we can improve it. maybe add etfs, crypto, metal, bonds and
other asset class too").

After deploy-lambdas ships justhodl-bottom 1.3.0 (the capitulation module + the crypto bank's Coinbase fallback):
  1. the live function runs the committed source (the release receipt data/ops/releases/justhodl-bottom.json names the
     commit; its source inventory carries capitulation.py) and the Scheduler schedule is untouched;
  2. invoke justhodl-bottom once (RequestResponse, up to 900 s; the trigger cap counts it as a root run) and read back:
       - data/capitulation-desk.json: contract, engine version, session, universe scored by desk, the regime / 5-session
         sum / percentile / clusters, prints per desk (60 sessions and the latest window), the top prints with their
         tape, context, Wyckoff link, follow-through and evidence, the follow-through base rate, the benchmarks' prints,
         scan errors, degraded inputs;
       - data/bottom.json: version 1.3.0, the slim `capitulation` pointer block, board_all intact (the katlin / fortress /
         alert consumers' contract), the crypto lane's status line from the engine log;
       - data/bottom/history/<session>.json.gz carries the capitulation summary;
  3. the crypto bank: how many symbols the lane scored, which source each took (polygon / coinbase), the errors left.
Re-runnable. Direct lane (STAGED). Writes nothing but the artifacts the engine owns and this report.
"""
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter
import gzip
import json
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN, SCHED = "justhodl-bottom", "justhodl-bottom-daily"
OUT, CAPIT, HIST = "data/bottom.json", "data/capitulation-desk.json", "data/bottom/history/"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=930)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def pct(v, d=1):
    return "-" if v is None else ("%+." + str(d) + "f%%") % v


def head_commit():
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(ROOT), capture_output=True, text=True, check=True).stdout.strip()
    except Exception:  # noqa: BLE001
        return None


def main():
    s3, lam, sch, logs = c("s3"), c("lambda"), c("scheduler"), c("logs")
    with report("ops_6753_capitulation_first_light") as r:
        r.heading("ops 6753 - capitulation desk first light (justhodl-bottom 1.3.0) %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. the deployed function")
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-bottom/config.json").read_text(encoding="utf-8"))
        cf = lam.get_function_configuration(FunctionName=FN)
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s, CodeSha256 %s" % (FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19], cf.get("CodeSha256")))
        if cf.get("Runtime") != cfg["runtime"] or cf.get("MemorySize") != cfg["memory"] or cf.get("Timeout") != cfg["timeout"]:
            r.fail("live configuration differs from config.json")
            raise SystemExit(1)
        try:
            rec = json.loads(s3.get_object(Bucket=LIVE, Key="data/ops/releases/%s.json" % FN)["Body"].read())
        except Exception as e:  # noqa: BLE001
            rec = {}
            r.warn("release receipt unreadable: %s" % type(e).__name__)
        inv = rec.get("source") or rec.get("source_inventory") or {}
        r.log("release receipt: commit %s deployed %s code_sha256 %s; source files %s" % (rec.get("commit"), rec.get("deployed_at"), rec.get("code_sha256"), sorted(inv.keys())[:8] if isinstance(inv, dict) else inv))
        head = head_commit()
        r.log("this checkout: %s" % head)
        if rec.get("code_sha256") and rec.get("code_sha256") != cf.get("CodeSha256"):
            r.fail("the live CodeSha256 is not the receipt's: AWS is not running the proven zip")
            raise SystemExit(1)
        src = (ROOT / "aws/lambdas/justhodl-bottom/source/lambda_function.py").read_text(encoding="utf-8")
        want = src.split('VERSION = "', 1)[1].split('"', 1)[0]
        if isinstance(inv, dict) and inv and "capitulation.py" not in inv:
            r.fail("the deployed zip has no capitulation.py: deploy-lambdas has not shipped 1.3.0 yet - wait for the deploy, then run again")
            raise SystemExit(1)
        live = sch.get_schedule(Name=SCHED, GroupName="default")
        r.log("schedule %s: %s %s (config %s)" % (SCHED, live.get("ScheduleExpression"), live.get("State"), cfg["eventbridge_scheduler"]["cron"]))
        if live.get("ScheduleExpression") != cfg["eventbridge_scheduler"]["cron"] or live.get("State") != "ENABLED":
            r.fail("schedule drifted")
            raise SystemExit(1)

        r.section("2. invoke once (RequestResponse)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6753_first_light"}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        r.log("invoke: status %s, function error %s, %.0fs, payload %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(body)[:600]))
        if resp.get("FunctionError"):
            r.fail("the engine raised: %s" % json.dumps(body)[:800])
            raise SystemExit(1)

        r.section("3. data/capitulation-desk.json")
        obj = s3.get_object(Bucket=LIVE, Key=CAPIT)
        doc = json.loads(obj["Body"].read())
        br, counts = doc.get("breadth") or {}, doc.get("counts") or {}
        r.log("written %s | %d bytes | contract %s | engine %s %s | module %s | session %s | window %s | generated %s" % (
            obj["LastModified"].isoformat(timespec="seconds"), obj["ContentLength"], doc.get("contract"), doc.get("engine"), doc.get("engine_version"), doc.get("version"), doc.get("session"), doc.get("window"), doc.get("generated_at")))
        if doc.get("contract") != "capitulation.v1" or doc.get("engine_version") != want:
            r.fail("artifact contract/version mismatch: %s %s (want %s)" % (doc.get("contract"), doc.get("engine_version"), want))
            raise SystemExit(1)
        r.log("READ: %s" % doc.get("read"))
        r.log("breadth: scored %s by desk %s | last session %s | sum_5 %s (%s%% of scored) percentile %s regime %s | window %s sessions, median sum_5 %s p90 %s max %s | clusters in window %s" % (
            br.get("n_scored"), json.dumps(br.get("scored_by_desk")), json.dumps(br.get("last_session"))[:300], br.get("sum_5"), br.get("sum_5_pct_of_scored"), br.get("sum_5_percentile"), br.get("regime"),
            br.get("window_sessions"), br.get("window_median_sum_5"), br.get("window_p90_sum_5"), br.get("window_max_sum_5"), br.get("n_clusters_window")))
        for cl in (br.get("clusters") or [])[-6:]:
            r.log("  cluster %s: %s prints (%s%%) %s" % (cl.get("date"), cl.get("total"), cl.get("pct_of_scored"), json.dumps(cl.get("by_desk"))))
        for d in doc.get("desk_order") or []:
            r.log("  desk %-12s %s" % (d, json.dumps(counts.get(d))))
        r.log("prints: 60 sessions %s | latest window (since %s) %s | follow-through %s" % (doc.get("n_prints_60"), doc.get("latest_cut"), len(doc.get("latest") or []), json.dumps(doc.get("follow_through"))[:700]))
        lat = doc.get("latest") or []
        ctx = Counter(p.get("context") for p in lat)
        wyk = Counter((p.get("wyckoff") or {}).get("state") or "none" for p in lat)
        r.log("latest: contexts %s | wyckoff states %s | desks %s" % (json.dumps(ctx), json.dumps(wyk), json.dumps(Counter(p.get("desk") for p in lat))))
        for p in lat[:30]:
            a = p.get("after") or {}
            r.log("  %-6s %-28s %-12s %s ret %s vol %sx rng %sx loc %s %s score %s dd20 %s wy %s | since %s bounce %s under %s held %s volafter %s now %s | fleet %s 13f %s" % (
                p.get("ticker"), (p.get("name") or "")[:28], p.get("desk"), p.get("date"), pct(p.get("ret_pct")), p.get("vol_x"), p.get("range_x"), p.get("loc"), p.get("context"), p.get("score"), pct(p.get("dd20_pct")),
                json.dumps(p.get("wyckoff"))[:60], a.get("sessions_since"), pct(a.get("bounce_pct")), pct(a.get("undercut_pct")), a.get("held"), a.get("vol_after_x"), pct(a.get("now_vs_print_pct")), p.get("n_confirm"), p.get("f13_net_usd_m")))
        for d in doc.get("desk_order") or []:
            rows = (doc.get("boards") or {}).get(d) or []
            if d == "stocks" or not rows:
                continue
            r.log("  board %s (%d rows, 60 sessions): %s" % (d, len(rows), "; ".join("%s %s %s %sx s%s" % (x.get("ticker"), x.get("date"), pct(x.get("ret_pct")), x.get("vol_x"), x.get("score")) for x in rows[:12])))
        bench = doc.get("benchmarks") or {}
        r.log("benchmarks: %s" % "; ".join("%s last %s prints250 %d%s" % (k, v.get("last"), len(v.get("prints") or []), (" latest " + (v["prints"][0].get("date") or "")) if v.get("prints") else "") for k, v in bench.items()))
        r.log("scan errors %s | degraded %s | feeds_asof %s" % (doc.get("scan_errors"), doc.get("degraded"), json.dumps(doc.get("feeds_asof"))[:400]))
        bad = [p for p in lat if p.get("vol_x") is None or p.get("ret_pct") is None or p.get("ret_pct") > 0]
        if bad:
            r.fail("%d latest prints carry impossible tape (null volume / positive return)" % len(bad))
            raise SystemExit(1)

        r.section("4. data/bottom.json (the pointer block, the consumers' contract)")
        obj = s3.get_object(Bucket=LIVE, Key=OUT)
        bot = json.loads(obj["Body"].read())
        slim = bot.get("capitulation") or {}
        r.log("bottom.json: version %s session %s generated %s %d bytes elapsed %ss | board %d board_all %d | degraded %s" % (
            bot.get("version"), bot.get("session"), bot.get("generated_at"), obj["ContentLength"], bot.get("elapsed_s"), len(bot.get("board") or []), len(bot.get("board_all") or []), bot.get("degraded")))
        r.log("pointer block: key %s regime %s sum_5 %s pct %s n_scored %s n_prints_60 %s latest %s | read: %s" % (
            slim.get("key"), slim.get("regime"), slim.get("sum_5"), slim.get("sum_5_percentile"), slim.get("n_scored"), slim.get("n_prints_60"), slim.get("n_latest"), slim.get("read")))
        if bot.get("version") != want or slim.get("key") != CAPIT or "boards" in slim:
            r.fail("bottom.json pointer block wrong: version %s key %s" % (bot.get("version"), slim.get("key")))
            raise SystemExit(1)
        sys.path.insert(0, str(ROOT / "aws" / "shared"))
        from bottom_context import context_rows  # noqa: E402
        ctx_rows = context_rows(bot)
        r.log("bottom_context.context_rows: %d rows (the katlin / fortress / alert consumers' contract holds)" % len(ctx_rows))
        for line in (bot.get("log") or []):
            if "crypto lane" in line or "capitulation" in line or "wrote" in line:
                r.log("  engine log: %s" % line[:300])
        snap_key = HIST + str(bot.get("session")) + ".json.gz"
        try:
            snap = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=snap_key)["Body"].read()))
            r.log("history %s: capitulation %s" % (snap_key, json.dumps(snap.get("capitulation"))[:300]))
        except Exception as e:  # noqa: BLE001
            r.warn("history snapshot unreadable: %s" % type(e).__name__)

        r.section("5. the crypto bank")
        srcs = Counter()
        for sym in ("BTC", "ETH", "SOL", "XRP", "DOGE", "AVAX", "LINK", "PEPE", "HYPE", "TAO"):
            try:
                d = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key="data/warm/katlin/crypto-bars/%s.json.gz" % sym)["Body"].read()))
                rows = d.get("rows") or []
                srcs[d.get("last_source") or "?"] += 1
                r.log("  %-5s rows %d last %s banked %s last_source %s" % (sym, len(rows), rows[-1][0] if rows else None, d.get("banked_at"), d.get("last_source")))
            except Exception as e:  # noqa: BLE001
                r.log("  %-5s unreadable %s" % (sym, type(e).__name__))
        r.log("sampled last_source: %s" % json.dumps(srcs))
        r.kv(session=str(doc.get("session")), regime=str(br.get("regime")), sum_5=str(br.get("sum_5")), scored=str(br.get("n_scored")), prints_60=str(doc.get("n_prints_60")), latest=str(len(lat)), bytes=str(obj["ContentLength"]))
        r.ok("ops 6753 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
