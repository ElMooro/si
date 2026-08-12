"""ops 4624 — blackswan regate (vault list-index fix): Khalid's TV list as a canary strip.

Khalid: "my tradingview blackswan list have many many more metrics
you can add." Correct institutional mapping for a list with that
name: tail-risk ALARMS, not expansion legs. New engine
justhodl-blackswan-watch joins tv-watchlists (name prefers 'swan',
the ops-4062 lesson) x TV vault x symbol-dictionary into per-symbol
|z| move + range-extreme states and one composite alarm; physical
v2.1.2 carries it as an observed canary. This op PRE-DUMPS the raw
shapes (list names, swan members, one vault entry) so even a red run
teaches the next patch, then settles, invokes, contracts, edge.
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

BFN = "justhodl-blackswan-watch"
PFN = "justhodl-physical-econ"
B = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
BARN = "arn:aws:lambda:us-east-1:857687956942:function:" + BFN
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4623"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def s3j(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def shape(o, depth=0):
    if depth > 2:
        return type(o).__name__
    if isinstance(o, dict):
        return {k: shape(v, depth + 1)
                for k, v in list(o.items())[:6]}
    if isinstance(o, list):
        return ["len=%d" % len(o),
                shape(o[0], depth + 1) if o else "empty"]
    return (str(o)[:40] if isinstance(o, str) else o)


def main():
    misses = 0
    with report("4624_blackswan_regate") as r:
        r.heading("ops 4623 — blackswan canary strip")

        r.section("pre-dump: raw shapes (evidence for any repair)")
        wl = s3j("data/tv-watchlists.json")
        names = []
        if isinstance(wl, dict):
            for k in ("watchlists", "lists", "data"):
                v = wl.get(k)
                if isinstance(v, dict):
                    names += list(v.keys())
                if isinstance(v, list):
                    names += [str((it or {}).get("name")
                                  or (it or {}).get("title"))
                              for it in v if isinstance(it, dict)]
            if not names:
                names = list(wl.keys())[:60]
        swanish = [n for n in names
                   if n and ("swan" in n.lower()
                             or "black" in n.lower())]
        r.log("watchlists container top-level: %s"
              % json.dumps(shape(wl))[:300])
        r.log("n_list_names=%d · swan/black candidates: %s"
              % (len(names), swanish[:6]))
        vault = s3j("data/tradingview.json")
        r.log("vault top-level shape: %s"
              % json.dumps(shape(vault))[:400])
        misses += contract(r, "swan-list-exists", bool(swanish),
                           "a swan/black list is present: %s"
                           % (swanish[:3] or "NONE"))

        r.section("deploy-settle + schedule")
        ok_b = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=BFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-blackswan-watch v1.0.1" in src:
                    ok_b = True
                    break
            except Exception as e:
                r.log("settle %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(30)
        ok_p = False
        for att in range(6):
            try:
                gf = lam.get_function(FunctionName=PFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v2.1.2" in src:
                    ok_p = True
                    break
            except Exception:
                pass
            time.sleep(30)
        misses += contract(r, "deploy", ok_b and ok_p,
                           "blackswan v1.0.1 + signal v2.1.2")
        if not (ok_b and ok_p):
            sys.exit(1)
        try:
            sch.get_schedule(Name=BFN)
        except Exception:
            try:
                sch.create_schedule(
                    Name=BFN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": BARN, "RoleArn": ROLE})
            except Exception as e:
                r.warn("schedule: %s" % str(e)[:100])

        r.section("engine run + per-symbol truth")
        inv = lam.invoke(FunctionName=BFN,
                         InvocationType="RequestResponse")
        bb = {}
        try:
            bb = json.loads(json.loads(
                inv["Payload"].read().decode()).get("body") or "{}")
        except Exception:
            pass
        r.kv(engine=json.dumps(bb)[:200])
        pl = s3j("data/blackswan-watch.json") or {}
        strip = pl.get("strip") or {}
        r.kv(list=pl.get("list_name"), members=pl.get("n_members"),
             resolved=pl.get("n_resolved"),
             with_history=pl.get("n_with_history"),
             alarm=strip.get("alarm"),
             top=json.dumps(strip.get("top_movers") or [])[:200])
        for x in (pl.get("rows") or [])[:22]:
            r.log("%-14s %-9s z=%-5s dod=%-7s range=%-9s %s"
                  % (x.get("symbol"), x.get("move_state"),
                     x.get("move_z"), x.get("dod_pct"),
                     x.get("range_state"),
                     str(x.get("name"))[:40]))
        misses += contract(r, "list-found",
                           bool(pl.get("list_name")),
                           "swan list resolved: %s (%s members)"
                           % (pl.get("list_name"),
                              pl.get("n_members")))
        nm = pl.get("n_members") or 0
        nr = pl.get("n_resolved") or 0
        misses += contract(r, "resolution",
                           nr >= 10 and (nm == 0 or nr >= 0.4 * nm),
                           "%d/%d symbols resolved from the vault"
                           % (nr, nm))
        misses += contract(r, "alarm",
                           strip.get("alarm") in ("CALM", "AMBER",
                                                  "RED"),
                           "strip alarm %s (red=%s amber=%s "
                           "extremes=%s)"
                           % (strip.get("alarm"), strip.get("n_red"),
                              strip.get("n_amber"),
                              strip.get("n_range_extreme")))

        r.section("physical canary + edge")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pe = s3j("data/physical-economy.json") or {}
        cans = pe.get("canaries") or {}
        misses += contract(r, "canary",
                           "blackswan_strip" in cans,
                           "blackswan_strip on the board: %s"
                           % json.dumps(cans.get("blackswan_strip")
                                        or {})[:140])
        page_ok = pay_ok = False
        for att in range(8):
            try:
                pg = http_get("https://justhodl.ai/"
                              "blackswan-watch.html?cb=%d"
                              % time.time()).decode("utf-8",
                                                    "replace")
                page_ok = "TAIL-RISK ALARM STRIP" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/blackswan-watch.json"
                    "?cb=%d" % time.time()))
                pay_ok = bool(jd.get("list_name"))
                if page_ok and pay_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", page_ok and pay_ok,
                           "page + payload at the edge")

        r.section("verdict")
        if misses:
            r.fail("blackswan: %d red (pre-dump above is the repair "
                   "evidence)" % misses)
            sys.exit(1)
        r.ok("BLACKSWAN STRIP LIVE — list '%s': %s/%s resolved, "
             "alarm %s, on the physical canary board · "
             "https://justhodl.ai/blackswan-watch.html"
             % (pl.get("list_name"), nr, nm, strip.get("alarm")))


if __name__ == "__main__":
    main()
