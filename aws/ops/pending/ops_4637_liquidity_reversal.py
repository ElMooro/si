"""ops 4637 — GLOBAL LIQUIDITY TREND REVERSAL engine.

Khalid: "now the same way let's build a global liquidity trend
reversal, that list is on my tradingview too." Doctrine #1.
Forked from blackswan v1.8.1 (full resolver ladder + SHARED warm
caches), new analytics: cadence-scaled dual-window OLS slopes,
MA-cross confirmation, doctrine polarity map, TREND + REVERSAL
dials. Pre-dumps liquid* list-name candidates (4623 lesson),
settles engine + signal v2.1.4, schedules hourly, invokes,
contracts dials + resolution + canary + edge.
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

LFN = "justhodl-liquidity-reversal"
PFN = "justhodl-physical-econ"
B = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
LARN = "arn:aws:lambda:us-east-1:857687956942:function:" + LFN
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


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
        r.heading("ops 4637 — global liquidity trend reversal")

        r.section("pre-dump: liquidity list candidates")
        wl = s3j("data/tv-watchlists.json") or {}
        names = [str((it or {}).get("name"))
                 for it in (wl.get("lists") or [])
                 if isinstance(it, dict)]
        liq = [n for n in names if n and (
            "liquid" in n.lower() or "gli" in n.lower())]
        r.kv(n_lists=len(names), liquidity_candidates=liq[:8])
        misses += contract(r, "list-exists", bool(liq),
                           "liquidity list present: %s"
                           % (liq[:3] or "NONE"))

        r.section("deploy-settle + env + schedule")
        ok_l = ok_p = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=LFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-liquidity-reversal v1.0.0" in src:
                    ok_l = True
                    break
            except Exception as e:
                r.log("settle %d: %s" % (att + 1, str(e)[:60]))
            time.sleep(30)
        for att in range(6):
            try:
                gf = lam.get_function(FunctionName=PFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v2.1.4" in src:
                    ok_p = True
                    break
            except Exception:
                pass
            time.sleep(30)
        misses += contract(r, "deploy", ok_l and ok_p,
                           "liquidity-reversal v1.0.0 + signal "
                           "v2.1.4")
        if not (ok_l and ok_p):
            sys.exit(1)
        try:
            tek = ssm.get_parameter(
                Name="/justhodl/te_api",
                WithDecryption=True)["Parameter"]["Value"]
            cfg = lam.get_function_configuration(FunctionName=LFN)
            ev = (cfg.get("Environment") or {}).get(
                "Variables") or {}
            need = {"TE_API_KEY": tek}
            if any(ev.get(k) != v for k, v in need.items()):
                ev.update(need)
                lam.update_function_configuration(
                    FunctionName=LFN,
                    Environment={"Variables": ev})
                for _ in range(20):
                    st = lam.get_function_configuration(
                        FunctionName=LFN)
                    if st.get("LastUpdateStatus") == "Successful":
                        break
                    time.sleep(5)
            r.ok("  [env] TE key present")
        except Exception as e:
            r.warn("env: %s" % str(e)[:90])
        try:
            sch.get_schedule(Name=LFN)
        except Exception:
            try:
                sch.create_schedule(
                    Name=LFN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": LARN, "RoleArn": ROLE})
            except Exception as e:
                r.warn("schedule: %s" % str(e)[:90])

        r.section("run + dials truth")
        lam.invoke(FunctionName=LFN, InvocationType="RequestResponse")
        pl = s3j("data/liquidity-reversal.json") or {}
        L = pl.get("liquidity") or {}
        rows = {x["symbol"]: x for x in pl.get("rows") or []}
        r.kv(list=pl.get("list_name"), members=pl.get("n_members"),
             resolved=pl.get("n_resolved"),
             statistical=pl.get("n_with_history"),
             trend=L.get("trend_score"),
             trend_label=L.get("trend_label"),
             reversal=L.get("reversal_score"),
             reversal_label=L.get("reversal_label"),
             n_polarity=L.get("n_polarity_rows"),
             confirmed=L.get("n_confirmed"))
        r.log("top reversals: %s"
              % json.dumps(L.get("top_reversals") or [])[:300])
        for sym in list(rows)[:14]:
            x = rows[sym]
            if x.get("trend_state"):
                r.log("%-26s trend=%-4s rev=%-13s conf=%-9s "
                      "slope=%s"
                      % (sym[:26], x.get("trend_state"),
                         x.get("reversal"),
                         x.get("reversal_conf"),
                         x.get("slope_now_pct")))
        misses += contract(r, "list-found",
                           bool(pl.get("list_name")),
                           "list '%s' (%s members)"
                           % (pl.get("list_name"),
                              pl.get("n_members")))
        nm = pl.get("n_members") or 0
        nr = pl.get("n_resolved") or 0
        misses += contract(r, "resolution",
                           nr >= 10 and (nm == 0 or nr >= 0.5 * nm),
                           "%d/%d resolved (shared caches)"
                           % (nr, nm))
        n_tr = sum(1 for x in (pl.get("rows") or [])
                   if x.get("trend_state"))
        misses += contract(r, "trend-coverage",
                           n_tr >= max(8, int(0.3 * max(nr, 1))),
                           "%d rows carry trend/reversal states"
                           % n_tr)
        misses += contract(r, "dials",
                           L.get("trend_label") in
                           ("EASING", "TIGHTENING", "MIXED")
                           and isinstance(L.get("trend_score"),
                                          (int, float)),
                           "TREND %s (%s) · REVERSAL %s (%s) on "
                           "%s polarity rows"
                           % (L.get("trend_score"),
                              L.get("trend_label"),
                              L.get("reversal_score"),
                              L.get("reversal_label"),
                              L.get("n_polarity_rows")))

        r.section("canary + edge")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pe = s3j("data/physical-economy.json") or {}
        cb = (pe.get("canaries") or {}).get(
            "liquidity_reversal") or {}
        misses += contract(r, "canary",
                           cb.get("trend") == L.get("trend_label"),
                           "physical board carries %s"
                           % json.dumps(cb)[:130])
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
                    "liquidity-reversal.json?cb=%d" % time.time()))
                pay_ok = bool((jd.get("liquidity")
                               or {}).get("trend_label"))
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
             "TREND %s (%s), REVERSAL %s (%s), %s confirmed turns "
             "· https://justhodl.ai/liquidity-reversal.html"
             % (pl.get("list_name"), nr, nm,
                L.get("trend_score"), L.get("trend_label"),
                L.get("reversal_score"), L.get("reversal_label"),
                L.get("n_confirmed")))


if __name__ == "__main__":
    main()
