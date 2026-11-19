"""ops 6844 - justhodl-campaign-scan: birth and first light (re-runnable).

Khalid 2026-11-19: "accumulation, bottom, distribution engines should be built for the icons there it should show all
the corresponding stocks".  After deploy-lambdas ships the engine:
  1. the function exists with config.json's runtime / memory / timeout / env; the Scheduler schedule
     justhodl-campaign-scan-daily (04:02 UTC) exists, is ENABLED and targets it;
  2. deployed schedule manifest := committed (additions-only, the added entry live-verified) - the reconciler disables
     what the manifest does not declare;
  3. the engine once (RequestResponse; a first-light invoke through ops, the precedent of every engine birth), then
     data/campaign-scan.json read back: its counts add up, every row stands inside its window, each ticker once a board;
  4. an independent re-scan on the runner: the engine's own port run here over the real banked bars of a sample of the
     published rows must print the same mark on the same date;
  5. the artifact embedded (gzip + base64) so the desk is verified offline against it before the browser proof.
Writes nothing but the schedule manifest (additions-only, with a backup) and what the engine owns.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import re
import sys
import time
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-campaign-scan" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED, OUT = "justhodl-campaign-scan", "justhodl-campaign-scan-daily", "data/campaign-scan.json"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=900, max_pool_connections=32)
ONCE = Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=900)      # an invoke is never repeated by the client
WINDOW = {"CLIMAX": LF.CLIMAX_SESSIONS, "BOTTOM": LF.BOTTOM_SESSIONS, "ACCUM": LF.ACCUM_SESSIONS, "DIST": LF.DIST_SESSIONS}
STATUS = {"bottom": {"HELD", "UNDERCUT", "BROKEN"}, "accumulation": {"ABOVE", "BASE", "LOST"}, "distribution": {"BELOW", "RANGE", "RECLAIMED"}}


def c(name, cfg=CFG):
    return boto3.client(name, region_name=REGION, config=cfg)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    lam_once = c("lambda", ONCE)
    with report("ops_6844_campaign_scan_first_light") as r:
        r.heading("ops 6844 - justhodl-campaign-scan: birth and first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. function + schedule")
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-campaign-scan/config.json").read_text(encoding="utf-8"))
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - wait for deploy-lambdas, then run again" % (FN, type(e).__name__))
            raise SystemExit(1)
        env = (cf.get("Environment") or {}).get("Variables", {})
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s, handler %s, env %s" % (
            FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19], cf.get("Handler"), json.dumps({k: env[k] for k in sorted(env) if k in cfg["env"]})))
        if cf.get("Runtime") != cfg["runtime"] or cf.get("MemorySize") != cfg["memory"] or cf.get("Timeout") != cfg["timeout"] or cf.get("Handler") != cfg["handler"]:
            r.fail("live configuration differs from config.json")
            raise SystemExit(1)
        diff = {k: (env.get(k), v) for k, v in cfg["env"].items() if env.get(k) != v}
        if diff:
            r.fail("live environment differs from config.json: %s" % json.dumps(diff))
            raise SystemExit(1)
        sc = cfg["eventbridge_scheduler"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
        except Exception as e:  # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)
        r.log("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
        if live.get("ScheduleExpression") != sc["cron"] or fn_of((live.get("Target") or {}).get("Arn")) != FN or live.get("State") != "ENABLED":
            r.fail("live schedule differs from config (%s vs %s)" % (live.get("ScheduleExpression"), sc["cron"]))
            raise SystemExit(1)
        r.ok("function and schedule match config.json (one run a day)")

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

        r.section("3. first light: the engine once -> data/campaign-scan.json")
        t0 = time.time()
        resp = lam_once.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6844_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:1500]))
            raise SystemExit(1)
        obj = s3.get_object(Bucket=LIVE, Key=OUT)
        body = obj["Body"].read()
        out = json.loads(body)
        r.log("%.0fs wall | handler %s | written %s | %d bytes (gzip %d) | version %s contract %s | run %ss | errors %s | trigger %s | content-type %s cache %s" % (
            time.time() - t0, json.dumps(payload), obj["LastModified"].isoformat(timespec="seconds"), len(body), len(gzip.compress(body)), out.get("version"), out.get("contract"), out.get("run_seconds"),
            out.get("n_errors"), json.dumps(out.get("trigger")), obj.get("ContentType"), obj.get("CacheControl")))
        r.log("universe: %s" % json.dumps(out["universe"]))
        bad = []
        for name in LF.BOARDS:
            rows, cn = out["boards"][name], out["counts"][name]
            r.log("%s: %s" % (name, json.dumps(cn)))
            if len({x["ticker"] for x in rows}) != len(rows):
                bad.append("%s lists a ticker twice" % name)
            if cn["n"] != len(rows) or cn["stock"]["n"] + cn["etf"]["n"] != len(rows):
                bad.append("%s counts do not add up" % name)
            for kind in ("stock", "etf"):
                sub = [x for x in rows if x["kind"] == kind]
                if sum(cn[kind]["by_status"].values()) != len(sub) or {k for k in cn[kind]["by_status"]} - STATUS[name]:
                    bad.append("%s %s statuses" % (name, kind))
            late = [x["ticker"] for x in rows if x["ago"] > WINDOW[x["mark"]] or x["ago"] < 0]
            if late:
                bad.append("%s rows outside their window: %s" % (name, late[:5]))
            if sum(v[name] for v in out["by_sector"].values()) != cn["stock"]["n"]:
                bad.append("%s sector table" % name)
            got = sum((p["climax"] + p["bottom"]) if name == "bottom" else p[name] for p in out["prints"])
            if got != len(rows):
                bad.append("%s prints by session sum to %d, board %d" % (name, got, len(rows)))
        if out["universe"]["scanned"] != out["universe"]["scanned_stock"] + out["universe"]["scanned_etf"] or sum(v["scanned"] for v in out["by_sector"].values()) != out["universe"]["scanned_stock"]:
            bad.append("universe counts")
        if out.get("as_of") != json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/polygon-full/eod/manifest.json")["Body"].read()).get("last_session"):
            bad.append("as_of is not the bank's last session")
        (r.fail if bad else r.ok)("invariants: %s" % ("; ".join(bad) if bad else "every board adds up, one row a ticker, every row inside its window, as of the bank's last session"))
        if bad:
            raise SystemExit(1)
        try:      # what the page will fetch: the same key, same origin
            req = urllib.request.Request("https://justhodl.ai/%s?nocache=%d" % (OUT, int(time.time())), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
            with urllib.request.urlopen(req, timeout=60) as rs:
                edge = json.loads(rs.read().decode("utf-8"))
            same = edge.get("generated_at") == out.get("generated_at")
            (r.ok if same else r.warn)("edge: https://justhodl.ai/%s serves contract %s generated %s - %s" % (OUT, edge.get("contract"), edge.get("generated_at"), "this run" if same else "not this run yet (edge cache)"))
        except Exception as e:  # noqa: BLE001
            r.warn("edge: https://justhodl.ai/%s not readable from the runner (%s: %s) - the browser proof reads it" % (OUT, type(e).__name__, str(e)[:120]))

        r.section("4. independent re-scan of a sample on the runner")
        sample, seen = [], set()
        for name in LF.BOARDS:
            rows = [x for x in out["boards"][name] if x["source"] != "harvest"]
            for x in sorted(rows, key=lambda x: -(x.get("mcap") or 0))[:14] + rows[:6]:
                if (name, x["ticker"]) not in seen:
                    seen.add((name, x["ticker"]))
                    sample.append((name, x))
        want = {x["ticker"] for _, x in sample}
        rec, _ = LF.recorded_splits(s3, out["universe"]["bank_first_session"], out["as_of"], want)
        hv = LF.harvest_index(LF.optional(s3, LF.BOTTOM_KEY)[0])
        book = {LF.canon(b_["symbol"]): b_ for b_ in LF.bank_enrich.rows(fields=LF.BOOK_FIELDS)}
        uni, _ = LF.select_universe(list(book.values()))
        bench = LF.clean(LF.read_bars(s3, "SPY"))
        order = {x["date"]: i for i, x in enumerate(bench)}
        order["__last__"] = len(bench) - 1
        cache, miss = {}, []
        for name, x in sample:
            t = x["ticker"]
            if t not in cache:
                cache[t] = LF.one_ticker(s3, uni[t], rec.get(t), hv.get(t), order, LF.listings_of(uni), lambda _t: None)[1]
            mine = cache[t].get(name)
            same = mine and (mine["mark"], mine["date"], mine["status"], mine["last"]) == (x["mark"], x["date"], x["status"], x["last"])
            if not same:
                miss.append("%s %s: published %s %s %s, re-scan %s" % (name, t, x["mark"], x["date"], x["status"], json.dumps({k: mine[k] for k in ("mark", "date", "status")}) if mine else None))
        r.log("re-scanned %d rows of %d tickers: %s" % (len(sample), len(cache), ", ".join("%s %s %s %s" % (n[:5], x["ticker"], x["mark"], x["date"]) for n, x in sample[:18])))
        (r.fail if miss else r.ok)("re-scan: %s" % ("; ".join(miss[:8]) if miss else "%d published rows reproduced on the runner - same mark, date, status and last close" % len(sample)))
        if miss:
            raise SystemExit(1)

        r.section("5. the boards")
        for name in LF.BOARDS:
            rows = [x for x in out["boards"][name] if x["kind"] == "stock"]
            for x in sorted(rows, key=lambda x: -(x.get("mcap") or 0))[:8]:
                r.log("  %-12s %-6s %-26s %-7s %s  ago %3d  %-9s since %+6.1f%%  $%.0fB" % (name, x["ticker"], (x.get("name") or "")[:26], x["mark"], x["date"], x["ago"], x["status"], x.get("since_pct") or 0, (x.get("mcap") or 0) / 1e9))
        r.log("changes: %s" % json.dumps(out.get("changes"))[:600])
        blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN campaign-scan")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        cn = out["counts"]
        r.kv(as_of=str(out["as_of"]), scanned=str(out["universe"]["scanned"]), accumulation=str(cn["accumulation"]["n"]), bottom=str(cn["bottom"]["n"]), distribution=str(cn["distribution"]["n"]),
             run_seconds=str(out.get("run_seconds")), bytes=str(len(body)))
        r.ok("FIRST LIGHT: %s scanned as of %s in %ss - accumulation %d, bottom %d, distribution %d" % (out["universe"]["scanned"], out["as_of"], out.get("run_seconds"), cn["accumulation"]["n"], cn["bottom"]["n"], cn["distribution"]["n"]))


if __name__ == "__main__":
    main()
    sys.exit(0)
