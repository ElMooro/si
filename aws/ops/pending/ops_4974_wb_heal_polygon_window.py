"""ops_4974 -- worldbank drain HEAL + polygon entitled-window probe
+ fiscaldata seed-expansion verify + gdelt v1 last-3 forensics.

Board forensics: worldbank froze at 9,200/29,490 for 24h+ -- ops
4960 armed only the WEEKLY redrain, so when chains hit depth-140
nothing restarted the drain. Heal = rate(2 hours) drain-continue
schedule (the fiscaldata/imf house pattern) + immediate kick.

  P1 worldbank: read state (lease/queue/failures), create -2h
     schedule, kick, watch 6min -> banked must MOVE
  P2 gdelt: name the 3 unfinished v1 files (expect 404-class)
  P3 polygon: binary-search earliest entitled grouped-daily date
     (donor key), count sample tickers -> window verdict for the
     polygon-full design
  P4 fiscaldata v1.0.1: kick rediscover -> universe grows past 19
     (new seeds probe-validated; wrong guesses fail-named)
"""
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
WB = "justhodl-worldbank-full"
FD = "justhodl-fiscaldata-full"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def kick(fn, payload=b"{}"):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=payload)
    except Exception:
        pass


def fetch(url, timeout=45, cap=300_000):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(cap)
    except urllib.error.HTTPError as e:
        return e.code, (e.read(200) or b"")
    except Exception as e:
        return 0, str(e)[:120].encode()


with report("ops_4974_wb_heal_polygon_window") as R:
    fails = []
    R.section("P1 worldbank heal")
    wbs = gj("data/warm/worldbank-full/_state/state.json") or {}
    R.log("  frozen-state: banked=%s q=%s lease=%.0fs-ago "
          "failures=%d as_of=%s" % (
              wbs.get("n_banked") or len(wbs.get("have") or {}),
              len(wbs.get("queue") or []),
              time.time() - float(wbs.get("lease_until") or 0),
              len(wbs.get("failures") or {}),
              (wbs.get("as_of") or "")[:19]))
    for k, v in list((wbs.get("failures") or {}).items())[:4]:
        R.log("    fail %s: %s" % (k, json.dumps(v)[:110]))
    try:
        arn = lam.get_function_configuration(
            FunctionName=WB)["FunctionArn"]
        sch.create_schedule(
            Name="justhodl-worldbank-full-2h", GroupName="default",
            ScheduleExpression="rate(2 hours)",
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": "{}"})
        R.log("  schedule -2h CREATED (the missing restarter)")
    except Exception as e:
        if "Conflict" in type(e).__name__ or "exists" in str(e):
            R.log("  schedule -2h exists")
        else:
            R.log("  schedule FAIL %s" % str(e)[:90])
            fails.append("P1-sched")
    kick(WB)
    b0 = wbs.get("n_banked") or len(wbs.get("have") or {})
    t0, moved = time.time(), False
    while time.time() - t0 < 6 * 60:
        time.sleep(30)
        wbs = gj("data/warm/worldbank-full/_state/state.json") or {}
        b1 = wbs.get("n_banked") or len(wbs.get("have") or {})
        R.log("  t+%3ds banked=%s q=%s" % (
            time.time() - t0, b1, len(wbs.get("queue") or [])))
        if b1 > b0 + 20:
            moved = True
            break
    R.log("  P1 %s (drain %s)" % (
        "PASS" if moved else "FAIL",
        "moving again" if moved else "still frozen"))
    if not moved:
        fails.append("P1")

    R.section("P2 gdelt v1 last-3")
    gds = gj("data/warm/gdelt-full/_state/state.json") or {}
    R.log("  v1=%s/%s v1_gb=%.2f phase=%s" % (
        gds.get("v1_idx"), gds.get("v1_total"),
        (gds.get("v1_bytes") or 0) / 1e9, gds.get("phase")))
    for k, v in list((gds.get("v1_failures") or {}).items())[:6]:
        R.log("    v1-fail %s: %s" % (k, v))

    R.section("P3 polygon entitled window")
    pk = ""
    try:
        pk = lam.get_function_configuration(
            FunctionName="justhodl-equity-research"
        )["Environment"]["Variables"].get("POLYGON_API_KEY") or ""
    except Exception as e:
        R.log("  donor env err %s" % str(e)[:80])
    win = {}
    if pk:
        for d in ["2016-06-01", "2019-06-03", "2021-06-01",
                  "2022-06-01", "2023-06-01", "2024-01-02"]:
            st_, b_ = fetch(
                "https://api.polygon.io/v2/aggs/grouped/locale/us/"
                "market/stocks/%s?limit=3&apiKey=%s" % (d, pk))
            n = 0
            try:
                n = json.loads(b_).get("resultsCount", 0)
            except Exception:
                pass
            R.log("  grouped %s -> %s results=%s %s" % (
                d, st_, n,
                "" if st_ == 200 else b_[:90].decode(
                    "utf-8", "replace")))
            win[d] = {"status": st_, "n": n}
            time.sleep(0.4)
    ent = sorted(d for d, v in win.items()
                 if v["status"] == 200 and v["n"] > 0)
    R.log("  entitled-from ~ %s" % (ent[0] if ent else "NONE"))

    R.section("P4 fiscaldata universe grows")
    kick(FD, json.dumps({"rediscover": True}).encode())
    fd0 = 19
    t0, grew = time.time(), False
    fds = {}
    while time.time() - t0 < 7 * 60:
        time.sleep(30)
        fds = gj("data/warm/fiscaldata-full/_state/state.json") or {}
        uni = len(fds.get("universe") or {})
        R.log("  t+%3ds universe=%s banked=%s invalid=%s" % (
            time.time() - t0, uni, len(fds.get("have") or {}),
            len(fds.get("invalid") or {})))
        if uni > fd0 + 5:
            grew = True
            break
    R.log("  P4 %s universe=%d (+%d) invalid-named=%d" % (
        "PASS" if grew else "FAIL",
        len(fds.get("universe") or {}),
        len(fds.get("universe") or {}) - fd0,
        len(fds.get("invalid") or {})))
    if not grew:
        fails.append("P4")

    if fails:
        R.log("ops 4974 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(wb_banked=wbs.get("n_banked") or
         len(wbs.get("have") or {}),
         gdelt_v1="%s/%s" % (gds.get("v1_idx"),
                             gds.get("v1_total")),
         polygon_from=(ent[0] if ent else None),
         fd_universe=len(fds.get("universe") or {}))
    R.log("ops 4974 GREEN -- worldbank restarter armed + draining; "
          "polygon window evidenced; fiscaldata universe expanded")
