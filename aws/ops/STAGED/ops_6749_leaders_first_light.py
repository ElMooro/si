"""ops 6749 - justhodl-leaders birth + first light, and the Polygon bank 1.2.0 priming the industry funds' flow history
(Khalid 2026-11-04: "build a new engine called leaders and track how leaders on each industry are doing compared to
sp500 and whether they are in accumulation or distribution. Is smart money buying or selling them. Buying inflow and
outflows. The industry etfs inflows and outflows, how are they compared to their 250, 200, 100, 50, 20 moving average.
Which are seeing golden cross and death cross. Also which are seeing double tops and double bottoms. Climax, tight
price. I want to be able to sort them ascending and descending.").

After deploy-lambdas ships the release commit:
  1. justhodl-leaders exists with the config's runtime / memory / timeout / env; the Scheduler schedule
     justhodl-leaders-daily (03:50 UTC) exists, is ENABLED and targets it;
  2. deployed schedule manifest := committed (additions-only, every added entry live-verified) - the reconciler disables
     what the manifest does not declare, so the declaration goes up before the next reconcile;
  3. the Polygon bank 1.2.0 once, the ETF Global fund-flow dataset only (async, polled through its manifest): the
     catalog's funds are pulled first under `composite_ticker`; every catalog fund's file is then read back and must name
     that fund in every row (the identity gate), with its last observation dated;
  4. the leaders engine once (RequestResponse), data/leaders.json read back: invariants, the continuity section (every
     cut / stitch / refusal), flow coverage by source, a field-coverage table for everything the request names, the
     boards, the strongest and weakest industries and leaders;
  5. the artifact embedded gzip+base64 in a second report file (the page is verified offline against it before the
     headless-Chrome proof on the site, ops 6750).
Re-runnable: step 3 is skipped when the funds were already primed under the current epoch today. Direct lane (STAGED).
Writes nothing but the schedule manifest (additions-only, with a backup) and what the two engines own.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
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
from ops_report import report  # noqa: E402
import industry_etf_catalog as catalog  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED, OUT = "justhodl-leaders", "justhodl-leaders-daily", "data/leaders.json"
BANK, BANK_ROOT, FLOWS = "justhodl-polygon-bank", "data/warm/polygon-bank/", "etf-global-fund-flows"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=330, max_pool_connections=32)
ONCE = Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=330)      # an invoke is never repeated by the client
MA = ("d20", "d50", "d100", "d200", "d250")
H = ("d", "w", "m", "q", "h", "y")


def c(name, cfg=CFG):
    return boto3.client(name, region_name=REGION, config=cfg)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def money(v):
    if v is None:
        return "-"
    a, s = abs(float(v)), "-" if v < 0 else "+" if v > 0 else ""
    return "%s$%.2fB" % (s, a / 1e9) if a >= 1e9 else "%s$%.0fM" % (s, a / 1e6) if a >= 1e6 else "%s$%.0fK" % (s, a / 1e3) if a >= 1e3 else "%s$%.0f" % (s, a)


def pct(v, d=1):
    return "-" if v is None else ("%+." + str(d) + "f%%") % v


def jget(s3, key):
    raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def at(row, path):
    cur = row
    for k in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def line(r):
    vs, ma, cr, ad = r.get("vs") or {}, r.get("ma") or {}, r.get("cross") or {}, r.get("ad") or {}
    return "%-6s %-22s #%s/%s %-34s str %5s | vs 1W %7s 1M %7s 3M %7s 6M %7s 1Y %8s | d20 %6s d50 %6s d100 %6s d200 %6s d250 %6s %s %s/%s | x %s%s | AD %6s %-19s | smart %s | %s" % (
        r["ticker"], (r.get("name") or "")[:22], r.get("rank"), r.get("rank_us"), (r.get("industry") or "")[:34], r.get("strength"), pct(vs.get("w")), pct(vs.get("m")), pct(vs.get("q")), pct(vs.get("h")), pct(vs.get("y")),
        ma.get("d20"), ma.get("d50"), ma.get("d100"), ma.get("d200"), ma.get("d250"), ma.get("stack"), ma.get("above"), ma.get("of"),
        cr.get("regime"), (" %s %sd" % (cr.get("type"), cr.get("ago"))) if cr.get("type") else "", ad.get("score"), ad.get("label"), (r.get("smart") or {}).get("verdict"), ", ".join(r.get("signals") or []))


def prime_bank(r, s3, lam_once, funds):
    """The bank's flow dataset once; returns the manifest's per-dataset row. Skipped when today's run already primed it."""
    spec = json.loads((ROOT / "aws/lambdas/justhodl-polygon-bank/source/polygon-bank-spec.json").read_text(encoding="utf-8"))
    epoch = next(d for d in spec["datasets"] if d["id"] == FLOWS)["epoch"]
    cf = c("lambda").get_function_configuration(FunctionName=BANK)
    r.log("%s: modified %s, state %s, timeout %s" % (BANK, (cf.get("LastModified") or "")[:19], cf.get("State"), cf.get("Timeout")))

    def read():
        man = jget(s3, BANK_ROOT + "manifest.json")
        return man, (man.get("per_dataset") or {}).get(FLOWS) or {}

    man, per = read()
    today = datetime.now(timezone.utc).date().isoformat()
    r.log("before: bank %s spec %s updated %s | flows epoch %s (spec %s) priority %s/%s at %s | objects %s lap %s cursor %s mismatch %s emptied %s" % (
        man.get("version"), man.get("spec_version"), man.get("updated_at"), per.get("epoch"), epoch, per.get("priority_last_run"), per.get("priority_size"), per.get("priority_last_at"),
        per.get("objects"), per.get("lap"), per.get("cursor"), per.get("mismatch"), per.get("emptied")))
    primed = man.get("version", "0") >= "1.2.0" and per.get("epoch") == epoch and str(per.get("priority_last_at") or "")[:10] == today and (per.get("priority_last_run") or 0) >= len(funds)
    if primed:
        r.ok("already primed today under epoch %s (%s funds) - the bank is not started again" % (epoch, per.get("priority_last_run")))
        return man, per
    t0 = datetime.now(timezone.utc)
    resp = lam_once.invoke(FunctionName=BANK, InvocationType="Event", Payload=json.dumps({"only": [FLOWS], "trigger": "ops_6749_prime_industry_funds"}).encode())
    r.log("async invoke %s at %s (only %s; its budget is 780 s)" % (resp.get("StatusCode"), t0.strftime("%H:%M:%S"), FLOWS))
    t_end = time.time() + 17 * 60
    done = False
    while time.time() < t_end:
        time.sleep(20)
        try:
            man, per = read()
        except Exception:  # noqa: BLE001
            continue
        fin = str((man.get("run") or {}).get("finished_at") or "")
        if fin and fin > t0.isoformat():
            done = True
            break
    if not done:
        r.fail("the bank did not finish within 17 min (manifest updated %s) - read CloudWatch /aws/lambda/%s" % (man.get("updated_at"), BANK))
        raise SystemExit(1)
    run = man.get("run") or {}
    r.log("after %.0fs: bank %s spec %s | run calls %s retries %s http %s puts %s unchanged %s bytes %s seconds %s | flows run %s" % (
        (datetime.now(timezone.utc) - t0).total_seconds(), man.get("version"), man.get("spec_version"), run.get("calls"), run.get("retries"), json.dumps(run.get("http")), run.get("puts"), run.get("unchanged"),
        run.get("put_bytes"), run.get("seconds"), json.dumps((run.get("datasets") or {}).get(FLOWS))))
    r.log("flows dataset: %s" % json.dumps(per))
    if man.get("version", "0") < "1.2.0":
        r.fail("the live bank is %s - deploy-lambdas has not shipped 1.2.0 yet" % man.get("version"))
        raise SystemExit(1)
    return man, per


def audit_fund_files(r, s3, funds, last_session):
    """Every catalog fund's banked flow file: who its rows name, how many, and the last observation."""
    clean, foreign, absent, empty, stale = [], [], [], [], []
    for t in funds:
        key = BANK_ROOT + FLOWS + "/" + t + ".json.gz"
        try:
            obj = s3.get_object(Bucket=LIVE, Key=key)
        except Exception:  # noqa: BLE001
            absent.append(t)
            continue
        raw = gzip.decompress(obj["Body"].read())
        doc = json.loads(raw)
        rows = doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None) or []
        names = Counter(str(x.get("composite_ticker")) for x in rows if isinstance(x, dict))
        if not rows:
            empty.append(t)
            continue
        if set(names) != {t}:
            foreign.append("%s(%s)" % (t, ",".join("%s:%d" % kv for kv in names.most_common(3))))
            continue
        eff = sorted(str(x.get("effective_date") or "") for x in rows)
        proc = sorted(str(x.get("processed_date") or "") for x in rows)
        age = (datetime.fromisoformat(last_session) - datetime.fromisoformat(eff[-1][:10])).days if eff[-1] else None
        (clean if age is not None and age <= 7 else stale).append((t, len(rows), eff[0][:10], eff[-1][:10], proc[-1][:10], len(raw), (obj.get("Metadata") or {}).get("epoch")))
    r.log("catalog funds %d | this fund's rows and current %d | this fund's rows, last observation older than 7 days %d | no rows %d | other funds' rows %d | no file %d" % (
        len(funds), len(clean), len(stale), len(empty), len(foreign), len(absent)))
    for name, group in (("current", clean), ("NOT CURRENT", stale)):
        for t, n, e0, e1, p1, nb, ep in group:
            r.log("  %-11s %-5s %5d rows  effective %s..%s  processed %s  %7d bytes  epoch %s" % (name, t, n, e0, e1, p1, nb, ep))
    if empty:
        r.log("  no rows at the provider: %s" % " ".join(empty))
    if foreign:
        r.log("  STILL OTHER FUNDS' ROWS: %s" % " ".join(foreign))
    if absent:
        r.log("  no file: %s" % " ".join(absent))
    return clean, stale, empty, foreign, absent


def coverage(r, name, group, fields):
    n = len(group)
    r.log("%s (%d rows):" % (name, n))
    for label, fn in fields:
        k = sum(1 for x in group if fn(x) not in (None, "", [], {}))
        r.log("  %-46s %4d / %-4d %5.1f%%" % (label, k, n, 100.0 * k / n if n else 0.0))


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    lam_once = c("lambda", ONCE)
    with report("ops_6749_leaders_first_light") as r:
        r.heading("ops 6749 - justhodl-leaders: birth, the bank's industry-fund flows, first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. function + schedule")
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-leaders/config.json").read_text(encoding="utf-8"))
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
        for other in ("justhodl-strength",):
            oc = lam.get_function_configuration(FunctionName=other)
            r.log("%s: modified %s, state %s (1.0.6 ships in the same deploy; its next scheduled run is 03:40 UTC)" % (other, (oc.get("LastModified") or "")[:19], oc.get("State")))
        r.ok("function and schedule match config.json")

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

        r.section("3. the Polygon bank 1.2.0: the industry funds' ETF Global flow history, under the fund's own name")
        funds = sorted(catalog.ALL_ETFS)
        bank_manifest = jget(s3, "data/warm/polygon-full/eod/manifest.json")
        last_session = str(bank_manifest.get("last_session") or bank_manifest.get("as_of") or datetime.now(timezone.utc).date().isoformat())[:10]
        man, per = prime_bank(r, s3, lam_once, funds)
        clean, stale, empty, foreign, absent = audit_fund_files(r, s3, funds, last_session)
        if foreign:
            r.fail("%d catalog funds still hold other funds' rows after the priming run" % len(foreign))
            raise SystemExit(1)
        (r.ok if len(clean) >= 0.8 * len(funds) else r.warn)("identity gate: %d of %d catalog funds banked under their own name and current; %d not current, %d without rows, %d without a file; mismatches refused so far %s" % (
            len(clean), len(funds), len(stale), len(empty), len(absent), per.get("mismatch")))

        r.section("4. first light: justhodl-leaders once -> data/leaders.json")
        t0 = time.time()
        resp = lam_once.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6749_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:1500]))
            raise SystemExit(1)
        obj = s3.get_object(Bucket=LIVE, Key=OUT)
        body = obj["Body"].read()
        out = json.loads(body)
        rows, etfs, inds = out["rows"], out["etfs"], out["industries"]
        by = {x["ticker"]: x for x in rows}
        r.log("%.0fs wall | handler %s | written %s | %d bytes (gzip %d) | version %s contract %s | run %ss | errors %s | trigger %s | content-type %s cache %s" % (
            time.time() - t0, json.dumps(payload), obj["LastModified"].isoformat(timespec="seconds"), len(body), len(gzip.compress(body)), out.get("version"), out.get("contract"), out.get("run_seconds"), out.get("n_errors"),
            json.dumps(out.get("trigger")), obj.get("ContentType"), obj.get("CacheControl")))
        try:      # what the page will fetch: the same key, same origin
            req = urllib.request.Request("https://justhodl.ai/%s?nocache=%d" % (OUT, int(time.time())), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
            with urllib.request.urlopen(req, timeout=60) as rs:
                edge = json.loads(rs.read().decode("utf-8"))
            same = edge.get("generated_at") == out.get("generated_at")
            (r.ok if same else r.warn)("edge: https://justhodl.ai/%s serves contract %s generated %s - %s" % (OUT, edge.get("contract"), edge.get("generated_at"), "this run" if same else "not this run yet (edge cache)"))
        except Exception as e:  # noqa: BLE001
            r.warn("edge: https://justhodl.ai/%s not readable from the runner (%s: %s) - ops 6750 reads it in a browser" % (OUT, type(e).__name__, str(e)[:120]))
        u = out["universe"]
        r.log("as of %s | bank %s" % (out.get("as_of"), json.dumps(out.get("bank"))))
        r.log("leaders %d (top-%d %d, ADRs %d) | industries %d | sectors %d | funds %d | unbanked %s | skipped for time %s | stats %s" % (
            u["n_leaders"], u["top_n"], u["n_top"], u["n_adr"], u["n_industries"], u["n_sectors"], u["n_etfs"], u["unbanked"], u["skipped_for_time"], json.dumps(u["stats"])))

        bad = []
        if out.get("contract") != "leaders.v1" or out.get("engine") != FN:
            bad.append("contract %s engine %s" % (out.get("contract"), out.get("engine")))
        if len(rows) < 500 or len(inds) < 120 or len(etfs) < 100:
            bad.append("thin universe: %d leaders / %d industries / %d funds" % (len(rows), len(inds), len(etfs)))
        if out.get("as_of") != last_session:
            bad.append("as_of %s is not the bank's last session %s" % (out.get("as_of"), last_session))
        if len(by) != len(rows) or len({x["ticker"] for x in etfs}) != len(etfs) or len({x["industry"] for x in inds}) != len(inds):
            bad.append("duplicate rows")
        per_ind = Counter(x["industry"] for x in rows if x["rank"] <= u["top_n"])
        if max(per_ind.values()) > u["top_n"]:
            bad.append("an industry carries more than %d top leaders: %s" % (u["top_n"], [k for k, v in per_ind.items() if v > u["top_n"]]))
        gaps = {}
        for name, group in (("leader", rows), ("fund", etfs)):
            for x in group:
                h, vs, bh = x.get("h") or {}, x.get("vs") or {}, out["benchmark"]["h"]
                for k in H:
                    if h.get(k) is not None and vs.get(k) is not None and bh.get(k) is not None and abs((h[k] - bh[k]) - vs[k]) > 0.02:
                        gaps.setdefault("%s %s%s" % (name, x["ticker"], " (thin)" if x.get("thin") else ""), []).append("%s %+.2f" % (k, vs[k] - (h[k] - bh[k])))
                ma = x.get("ma") or {}
                for k in MA:
                    s, d = ma.get("s" + k[1:]), ma.get(k)
                    if s and d is not None and x.get("px") and abs((x["px"] / s - 1) * 100 - d) > 0.02:
                        bad.append("%s %s: %s %s is not px %s over the average %s" % (name, x["ticker"], k, d, x["px"], s))
        # excess is measured on the sessions both the instrument and SPY printed; a row that missed a session inside a
        # window reads a slightly different SPY leg than the benchmark's own - expected for the thin funds, a defect in bulk
        r.log("rows whose excess is not their return minus the benchmark's (a missed session inside the window): %d %s" % (len(gaps), json.dumps(gaps)[:1500]))
        if len(gaps) > 0.02 * (len(rows) + len(etfs)):
            bad.append("%d rows carry an excess return that is not their return minus the benchmark's" % len(gaps))
        boards = out["boards"]
        for scope, group in (("leaders", rows), ("etfs", etfs)):
            have = {x["ticker"] for x in group}
            for k, lst in boards[scope].items():
                if len(set(lst)) != len(lst) or not set(lst) <= have:
                    bad.append("board %s.%s names a row that is not there, or twice" % (scope, k))
        for k, key in (("double_top", "dtop"), ("double_bottom", "dbot")):
            for t in boards["leaders"][k]:
                if (by[t].get(key) or {}).get("state") not in ("FORMING", "CONFIRMED"):
                    bad.append("%s is on the %s board with state %s" % (t, k, (by[t].get(key) or {}).get("state")))
        for t in boards["leaders"]["golden_cross"] + boards["leaders"]["death_cross"]:
            cr = by[t].get("cross") or {}
            if not cr.get("fresh") or cr.get("ago") is None or cr["ago"] > 20:
                bad.append("%s is on a cross board with a cross %s sessions old" % (t, cr.get("ago")))
        late = [x for x in out["sources"] if x.get("tier") not in ("fresh", None)]
        r.log("sources: %s%s" % (json.dumps(Counter(x.get("tier") for x in out["sources"]).most_common()), (" | not fresh: %s" % json.dumps([{k: x.get(k) for k in ("engine", "key", "tier", "age_h", "last_modified")} for x in late])) if late else ""))
        if out.get("n_errors"):
            bad.append("engine errors: %s" % json.dumps(out.get("errors")[:8]))
        for x in bad[:40]:
            r.fail(x)
        if bad:
            raise SystemExit(1)
        r.ok("invariants hold: %d leaders, %d industries, %d funds as of %s; excess returns and average distances recompute; boards name live rows only" % (len(rows), len(inds), len(etfs), out["as_of"]))

        r.section("5. continuity: every cut, stitch and refusal")
        hs = out["history"]
        allrows = {x["ticker"]: x for x in rows + etfs}

        def hz(t):
            x = allrows.get(t) or {}
            h, ma = x.get("h") or {}, x.get("ma") or {}
            return "sessions %s from %s | h 1M %s 3M %s 6M %s 1Y %s | d200 %s d250 %s | cross %s | jumps %s" % (
                (x.get("history") or {}).get("sessions"), (x.get("history") or {}).get("from"), h.get("m"), h.get("q"), h.get("h"), h.get("y"), ma.get("d200"), ma.get("d250"),
                ((x.get("cross") or {}).get("regime")), json.dumps((x.get("history") or {}).get("jumps")))
        r.log("history %s" % json.dumps({k: v for k, v in hs.items() if k not in ("cut", "stitched", "refused", "short", "note")}))
        for t, cut in hs["cut"].items():
            r.log("CUT      %-6s %s | %s" % (t, json.dumps(cut), hz(t)))
        for t, st in hs["stitched"].items():
            r.log("STITCHED %-6s %s | %s" % (t, json.dumps(st), hz(t)))
        for t, why in hs["refused"].items():
            r.log("REFUSED  %-6s %s | %s" % (t, why, hz(t)))
        r.log("short histories (under 253 sessions, %d): %s" % (len(hs["short"]), " ".join("%s:%s" % (t, (allrows[t].get("history") or {}).get("sessions")) for t in hs["short"])))
        r.log("one-session moves beyond x1.8 / x0.56 left as printed: %s" % json.dumps({x["ticker"]: x["history"]["jumps"] for x in rows + etfs if (x.get("history") or {}).get("jumps")}))
        fisv = (hs["cut"].get("FISV") or {}).get("reason")
        (r.fail if fisv == "separation" else r.ok)("FISV: cut %s, stitched %s (a crash beside an unrelated listing is not a separation)" % (json.dumps(hs["cut"].get("FISV")), json.dumps(hs["stitched"].get("FISV"))))
        ext = sorted([x for x in rows if (x.get("h") or {}).get("y") is not None], key=lambda x: -abs(x["h"]["y"]))[:14]
        r.log("largest one-year moves: %s" % ", ".join("%s %+.0f%%" % (x["ticker"], x["h"]["y"]) for x in ext))
        r.log("splits rebased: %s" % json.dumps(out["splits"]))

        r.section("6. what the request names, and how much of it is there")
        lead_fields = [("vs S&P 500 1D", lambda x: at(x, "vs.d")), ("vs S&P 500 1W", lambda x: at(x, "vs.w")), ("vs S&P 500 1M", lambda x: at(x, "vs.m")), ("vs S&P 500 3M", lambda x: at(x, "vs.q")),
                       ("vs S&P 500 6M", lambda x: at(x, "vs.h")), ("vs S&P 500 1Y", lambda x: at(x, "vs.y")), ("strength rank", lambda x: x.get("strength")),
                       ("accumulation / distribution label", lambda x: at(x, "ad.label")), ("A/D score 21 sessions ago", lambda x: at(x, "ad.score_21")),
                       ("signed dollar volume 5 sessions", lambda x: at(x, "mf.w.net")), ("signed dollar volume 21 sessions", lambda x: at(x, "mf.m.net")), ("signed dollar volume 63 sessions", lambda x: at(x, "mf.q.net")),
                       ("smart money verdict (not NO READ)", lambda x: None if at(x, "smart.verdict") in (None, "NO READ") else 1), ("13F cohort read", lambda x: x.get("f13")), ("insider transactions (90 days)", lambda x: x.get("insider")),
                       ("price vs 20-session average", lambda x: at(x, "ma.d20")), ("price vs 50-session average", lambda x: at(x, "ma.d50")), ("price vs 100-session average", lambda x: at(x, "ma.d100")),
                       ("price vs 200-session average", lambda x: at(x, "ma.d200")), ("price vs 250-session average", lambda x: at(x, "ma.d250")),
                       ("50/200 regime (golden or death side)", lambda x: at(x, "cross.regime")), ("a dated 50/200 cross in the lookback", lambda x: at(x, "cross.type")),
                       ("double top (any state)", lambda x: x.get("dtop")), ("double bottom (any state)", lambda x: x.get("dbot")), ("climax in the last 30 sessions", lambda x: x.get("climax")),
                       ("tight-base read", lambda x: at(x, "tight.tier")), ("industry fund attached", lambda x: at(x, "etf.ticker") or x.get("etf")), ("52-week position", lambda x: x.get("pos_52w"))]
        coverage(r, "LEADERS", rows, lead_fields)
        fund_fields = [("reported flow, current", lambda x: 1 if at(x, "flow.current") else None), ("flow 1 observation", lambda x: at(x, "flow.w1.usd")), ("flow 5 observations", lambda x: at(x, "flow.w5.usd")),
                       ("flow 21 observations", lambda x: at(x, "flow.w21.usd")), ("flow 63 observations", lambda x: at(x, "flow.w63.usd")), ("flow as % of assets (21)", lambda x: at(x, "flow.w21.pct_aum")),
                       ("vs S&P 500 1M", lambda x: at(x, "vs.m")), ("vs S&P 500 1Y", lambda x: at(x, "vs.y")),
                       ("price vs 20-session average", lambda x: at(x, "ma.d20")), ("price vs 50-session average", lambda x: at(x, "ma.d50")), ("price vs 100-session average", lambda x: at(x, "ma.d100")),
                       ("price vs 200-session average", lambda x: at(x, "ma.d200")), ("price vs 250-session average", lambda x: at(x, "ma.d250")), ("50/200 regime", lambda x: at(x, "cross.regime")),
                       ("accumulation / distribution label", lambda x: at(x, "ad.label")), ("double top (any state)", lambda x: x.get("dtop")), ("double bottom (any state)", lambda x: x.get("dbot")),
                       ("climax in the last 30 sessions", lambda x: x.get("climax")), ("tight-base read", lambda x: at(x, "tight.tier")), ("13F cohort read", lambda x: x.get("f13"))]
        coverage(r, "INDUSTRY FUNDS", etfs, fund_fields)
        ind_fields = [("ranked against the S&P 500", lambda x: x.get("rank")), ("cap-weighted excess 1M", lambda x: at(x, "vs.m")), ("A/D average", lambda x: at(x, "ad.score")),
                      ("own-fund flow (not the sector proxy)", lambda x: 1 if at(x, "etf_flow.funds") and not at(x, "etf_flow.proxy") else None), ("any fund flow incl. sector proxy", lambda x: at(x, "etf_flow.w21.usd")),
                      ("13F industry row", lambda x: x.get("f13")), ("smart-money read", lambda x: x.get("smart"))]
        coverage(r, "INDUSTRIES", inds, ind_fields)
        fl = out["flows"]
        r.log("flows %s" % json.dumps({k: v for k, v in fl.items() if k != "no_flow"}))
        r.log("no current flow (%d): %s" % (len(fl["no_flow"]), " ".join(fl["no_flow"])))
        (r.ok if fl["n_current"] >= 0.8 * fl["n_funds"] else r.warn)("fund flows current for %d of %d industry funds (by source %s)" % (fl["n_current"], fl["n_funds"], json.dumps(fl["by_source"])))

        r.section("7. boards")
        bm = out["benchmark"]
        r.log("SPY %s as of %s | h %s | ma %s | cross %s | A/D %s" % (bm["px"], bm["as_of"], json.dumps(bm["h"]), json.dumps(bm["ma"]), json.dumps(bm["cross"]), json.dumps(bm["ad"])))
        r.log("summary %s" % json.dumps(out["summary"]))
        for scope in ("leaders", "etfs"):
            r.log("%s boards: %s" % (scope, json.dumps({k: len(v) for k, v in boards[scope].items()})))
        b = boards["leaders"]
        for name in ("golden_cross", "death_cross"):
            r.log("%s (%d): %s" % (name.upper(), len(b[name]), ", ".join("%s(%s, %sd)" % (t, by[t]["cross"]["date"], by[t]["cross"]["ago"]) for t in b[name])))
        for name in ("golden_near", "death_near"):
            r.log("%s (%d): %s" % (name.upper(), len(b[name]), ", ".join("%s(gap %s%%)" % (t, by[t]["cross"]["gap_pct"]) for t in b[name])))
        for name, key in (("double_top", "dtop"), ("double_bottom", "dbot")):
            r.log("%s (%d): %s" % (name.upper(), len(b[name]), ", ".join("%s(%s%s)" % (t, by[t][key]["state"], " target met" if by[t][key].get("target_hit") else "") for t in b[name])))
            r.log("%s states: %s" % (name.upper(), json.dumps(Counter((x.get(key) or {}).get("state") for x in rows).most_common())))
        for name in ("buying_climax", "selling_climax"):
            r.log("%s (%d): %s" % (name.upper(), len(b[name]), ", ".join("%s(%s)" % (t, by[t]["climax"].get("date")) for t in b[name])))
        r.log("TIGHT (%d): %s" % (len(b["tight"]), ", ".join("%s(%s, base %s, range %s%%)" % (t, by[t]["tight"]["tier"], by[t]["tight"]["base_len"], by[t]["tight"]["base_range_pct"]) for t in b["tight"][:40])))
        for name in ("accumulation", "distribution", "smart_buying", "smart_selling", "rs_new_highs", "rs_new_lows"):
            r.log("%s (%d): %s" % (name.upper(), len(b[name]), " ".join(b[name][:60])))
        r.log("A/D labels: leaders %s | funds %s" % (json.dumps(Counter((x.get("ad") or {}).get("label") for x in rows).most_common()), json.dumps(Counter((x.get("ad") or {}).get("label") for x in etfs).most_common())))
        r.log("smart money: %s | 13F %s | insiders %s" % (json.dumps(Counter((x.get("smart") or {}).get("verdict") for x in rows).most_common()), json.dumps(out["f13"]), json.dumps(out["insiders"])))
        r.log("fund boards: %s" % json.dumps({k: v for k, v in boards["etfs"].items() if v and k not in ("bull_stack", "bear_stack")}))

        r.section("8. industries and leaders")
        ranked = [x for x in inds if x.get("rank")]
        for name, group in (("STRONGEST 15 INDUSTRIES", ranked[:15]), ("WEAKEST 15 INDUSTRIES", ranked[-15:])):
            r.log("%s:" % name)
            for x in group:
                vs, br, ef, f13 = x.get("vs") or {}, x["breadth"], x["etf_flow"], x.get("f13") or {}
                r.log("  #%-3s %-40s %-22s str %5s | vs 1W %7s 1M %7s 3M %7s 6M %7s | >50 %s/%s >200 %s/%s | AD %6s %s | $flow21 %s | 13F ind net %s | funds %s %s 21obs %s (%s%%) | %s" % (
                    x["rank"], x["industry"][:40], (x.get("sector") or "")[:22], x.get("strength"), pct(vs.get("w")), pct(vs.get("m")), pct(vs.get("q")), pct(vs.get("h")), br["above_50"], br["n"], br["above_200"], br["n"],
                    x["ad"]["score"], x["ad"]["label"], money(x.get("mf21_usd")), money(f13.get("net_usd")), ",".join(ef["funds"]) or "-", "(sector)" if ef["proxy"] else "", money((ef.get("w21") or {}).get("usd")),
                    (ef.get("w21") or {}).get("pct_aum"), " ".join(x["leaders"])))
        r.log("FUNDS by 21-observation flow as a share of assets (top and bottom 15):")
        cur = sorted([x for x in etfs if (x.get("flow") or {}).get("current") and ((x["flow"].get("w21") or {}).get("pct_aum")) is not None], key=lambda x: -x["flow"]["w21"]["pct_aum"])
        for x in cur[:15] + cur[-15:]:
            w, ma = x["flow"], x.get("ma") or {}
            r.log("  %-5s %-9s %-22s aum %9s | 1 %9s 5 %9s (%6s%%) 21 %9s (%6s%%) | px vs 20 %6s 50 %6s 100 %6s 200 %6s 250 %6s %s | %s | AD %s | vs 1M %s" % (
                x["ticker"], x["role"], w["source"], money(w.get("aum_usd")), money((w.get("w1") or {}).get("usd")), money((w.get("w5") or {}).get("usd")), (w.get("w5") or {}).get("pct_aum"),
                money((w.get("w21") or {}).get("usd")), (w.get("w21") or {}).get("pct_aum"), ma.get("d20"), ma.get("d50"), ma.get("d100"), ma.get("d200"), ma.get("d250"), ma.get("stack"),
                ((x.get("cross") or {}).get("regime")), (x.get("ad") or {}).get("label"), pct((x.get("vs") or {}).get("m"))))
        r.log("STRONGEST 20 LEADERS:")
        for x in rows[:20]:
            r.log("  " + line(x))
        r.log("WEAKEST 20 LEADERS:")
        for x in [x for x in rows if x.get("strength") is not None][-20:]:
            r.log("  " + line(x))
        r.log("changes %s" % json.dumps(out["changes"])[:600])
        r.log("sources %s" % json.dumps(out["sources"]))

        # the published artifact for offline page work: sparks kept for the first 80 rows / 30 funds only
        keep = json.loads(body)
        for k, n in (("rows", 80), ("etfs", 30)):
            for i, x in enumerate(keep[k]):
                if i >= n:
                    x.pop("spark", None)
                    x.pop("rs_spark", None)
        blob = base64.b64encode(gzip.compress(json.dumps(keep, separators=(",", ":")).encode(), compresslevel=9)).decode("ascii")
        art = ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6749_leaders_first_light_artifact.md"
        art.write_text("# ops 6749 - data/leaders.json as published (gzip + base64 of the JSON; sparks kept for the first 80 rows / 30 funds)\n\n<!-- ARTIFACT_GZ_B64 -->\n" +
                       "\n".join(blob[i:i + 120] for i in range(0, len(blob), 120)) + "\n<!-- /ARTIFACT_GZ_B64 -->\n", encoding="utf-8")
        r.log("artifact embedded: %s (%d base64 chars)" % (art.name, len(blob)))
        r.kv(as_of=str(out["as_of"]), leaders=str(len(rows)), industries=str(len(inds)), funds=str(len(etfs)), bytes=str(len(body)), run_s=str(out["run_seconds"]), errors=str(out["n_errors"]),
             flows_current="%d/%d" % (fl["n_current"], fl["n_funds"]), bank_clean="%d/%d" % (len(clean), len(funds)), golden=str(len(b["golden_cross"])), death=str(len(b["death_cross"])),
             dtop=str(len(b["double_top"])), dbot=str(len(b["double_bottom"])), climax="%d/%d" % (len(b["buying_climax"]), len(b["selling_climax"])), tight=str(len(b["tight"])),
             cut=str(hs["n_cut"]), stitched=str(hs["n_stitched"]))
        r.ok("LEADERS LIVE: %d leaders / %d industries / %d funds as of %s; flows current %d/%d; schedule %s %s" % (len(rows), len(inds), len(etfs), out["as_of"], fl["n_current"], fl["n_funds"], SCHED, sc["cron"]))


if __name__ == "__main__":
    main()
    sys.exit(0)
