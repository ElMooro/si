"""ops 4637 — GLOBAL LIQUIDITY TREND REVERSAL engine.

Khalid: same playbook as blackswan, for his TradingView global-
liquidity list. New engine justhodl-liquidity-reversal v1.0.0:
full v1.8.1 resolution ladder (shared warm-cache pool with
blackswan — every series resolved tonight is free), plus a
reversal analytics core: 1M-vs-3M momentum sign divergence with
|z(1M)|>=1 force, turn-age, MA50 cross recency, and a Reversal
Gauge on breadth of fresh turns. Pre-dumps list names first.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-liquidity-reversal"
B = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
ARN = "arn:aws:lambda:us-east-1:857687956942:function:" + FN
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4637"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def s3j(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def main():
    misses = 0
    with report("4637_liquidity_reversal") as r:
        r.heading("ops 4637 — liquidity trend-reversal engine")

        r.section("pre-dump: liquidity list candidates")
        wl = s3j("data/tv-watchlists.json") or {}
        names = [str((it or {}).get("name"))
                 for it in (wl.get("lists") or [])
                 if isinstance(it, dict)]
        liq = [(str(it.get("name")), it.get("n"))
               for it in (wl.get("lists") or [])
               if isinstance(it, dict)
               and "liquid" in str(it.get("name", "")).lower()]
        r.kv(n_lists=len(names), liquidity_candidates=json.dumps(
            liq[:6]))
        misses += contract(r, "list-exists", bool(liq),
                           "liquidity list present: %s"
                           % (liq[:3] or "NONE"))

        r.section("deploy-settle + schedule")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if "justhodl-liquidity-reversal v1.0.0" in src:
                    settled = True
                    break
            except Exception as e:
                r.log("settle %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "v1.0.0 live")
        if not settled:
            sys.exit(1)
        try:
            sch.get_schedule(Name=FN)
        except Exception:
            try:
                sch.create_schedule(
                    Name=FN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": ARN, "RoleArn": ROLE})
                r.log("hourly schedule created")
            except Exception as e:
                r.warn("schedule: %s" % str(e)[:90])

        r.section("run + reversal truth")
        lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        pl = s3j("data/liquidity-reversal.json") or {}
        g = pl.get("gauge") or {}
        br = g.get("breadth") or {}
        rows = pl.get("rows") or []
        r.kv(list=pl.get("list_name"), members=pl.get("n_members"),
             resolved=pl.get("n_resolved"),
             trend_capable=br.get("n_trend_capable"),
             gauge=g.get("value"), label=g.get("label"),
             turning_up=br.get("turning_up_pct"),
             turning_down=br.get("turning_down_pct"),
             top_turns=json.dumps(g.get("top_fresh_turns")
                                  or [])[:240])
        shown = 0
        for x in rows:
            if not x.get("reversal_state"):
                continue
            r.log("%-26s %-11s 1m=%-7s 3m=%-7s z=%-5s age=%-4s %s"
                  % (str(x.get("symbol"))[:26],
                     x.get("reversal_state"),
                     x.get("ret_1m"), x.get("ret_3m"),
                     x.get("z_1m"),
                     x.get("turn_age_days"),
                     str(x.get("name"))[:28]))
            shown += 1
            if shown >= 18:
                break
        nm = pl.get("n_members") or 0
        nr = pl.get("n_resolved") or 0
        misses += contract(r, "list-found",
                           bool(pl.get("list_name")),
                           "list '%s' (%s members)"
                           % (pl.get("list_name"), nm))
        misses += contract(r, "resolution",
                           nr >= 40 and (nm == 0
                                         or nr >= 0.55 * nm),
                           "%d/%d resolved (shared cache pool "
                           "with blackswan)" % (nr, nm))
        misses += contract(r, "trend-capable",
                           (br.get("n_trend_capable") or 0) >= 30,
                           "%s rows carry reversal analytics"
                           % br.get("n_trend_capable"))
        misses += contract(r, "gauge",
                           g.get("label") in ("REVERSING_UP",
                                              "REVERSING_DOWN",
                                              "TRENDING", "MIXED")
                           and isinstance(g.get("value"),
                                          (int, float)),
                           "gauge %s (%s)" % (g.get("value"),
                                              g.get("label")))

        r.section("edge")
        page_ok = pay_ok = False
        for att in range(8):
            try:
                pg = http_get("https://justhodl.ai/"
                              "liquidity-reversal.html?cb=%d"
                              % time.time()).decode("utf-8",
                                                    "replace")
                page_ok = "TREND REVERSAL" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/"
                    "liquidity-reversal.json?cb=%d"
                    % time.time()))
                pay_ok = bool((jd.get("gauge") or {}).get("label"))
                if page_ok and pay_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", page_ok and pay_ok,
                           "page + payload at the edge")

        r.section("verdict")
        if misses:
            r.fail("liquidity-reversal: %d red (pre-dump above is "
                   "repair evidence)" % misses)
            sys.exit(1)
        r.ok("LIQUIDITY REVERSAL LIVE — list '%s': %s/%s resolved, "
             "%s trend-capable, gauge %s (%s) · "
             "https://justhodl.ai/liquidity-reversal.html"
             % (pl.get("list_name"), nr, nm,
                br.get("n_trend_capable"), g.get("value"),
                g.get("label")))


if __name__ == "__main__":
    main()
