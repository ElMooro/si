"""ops 3513 — equity-FTD graded alpha family LIVE (queue #5 closed).

justhodl-equity-ftd v1.0.0 extends ignition's SEC CNS fetch into a
dedicated family: 6 half-month files of per-symbol history, $-value
fails from the native PRICE column (3512-proven), PEAK-DAY days-to-
cover vs FMP 20d volume, spike vs trailing-file mean (>=3 priors),
ETF/fund exclusion, hard liquidity floors (px>=$5, avg vol>=500k,
>=$5M fails, spike>=3x, peak-day DTC>=0.5), and schema-v2
"ftd-squeeze" UP [21,63] signals vs SPY through the fleet grader
(shared signals_emit.log_signal: regime snapshot + dedupe + suppress
honored). Cheap unchanged-skip when SEC hasn't published. Twice-weekly
Scheduler (Mon+Thu 15:10 UTC). alpha-families.html gains the 7th card.

Gates:
  L1 CI battery: synthetic zip -> $27M exact, peak-day 400k, spike
     5.0x, ETF excluded, penny blocked by floors, <3-priors blocked
  L2 live force run: doc has >=4 files, universe ~13-14k, candidates
     with volumes >=100, top_dollars[0] in the recon cohort
     (GOOG/GOOGL/AMD/XOM sanity), EVERY emitted signal re-verified
     against every floor from the doc itself, qualifiers printed
  L3 Scheduler exists (cron 10 15 ? * MON,THU *) -> this engine
  L4 families page: b-ftd card + equity-ftd feed wired + script parses
"""
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report                      # noqa: E402
from _lambda_deploy_helpers import deploy_lambda   # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-equity-ftd"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
iam = boto3.client("iam")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3513"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3513_equity_ftd_family") as rep:
    out = {"ops": 3513, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:540]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:500]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3513 — equity-FTD family (SEC CNS)")

    try:
        sys.path.insert(0, str(REPO / "aws/shared"))
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "ftd", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)

        def mkzip(rows):
            txt = ("SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|"
                   "DESCRIPTION|PRICE\n" + "\n".join(rows))
            b = io.BytesIO()
            zf = zipfile.ZipFile(b, "w")
            zf.writestr("f.txt", txt)
            zf.close()
            return b.getvalue()

        A = mkzip(["20260701|C1|SQZ|200000|SQUEEZE CORP|30.00",
                   "20260702|C1|SQZ|400000|SQUEEZE CORP|30.00",
                   "20260703|C1|SQZ|300000|SQUEEZE CORP|30.00",
                   "20260701|C2|BIGX|500000|ISHARES BIG ETF|40.0",
                   "20260701|C3|LOWP|900000|PENNY CO|1.20",
                   "badline", "20260701|C4|NOQ|abc|X|1"])
        per, meta = m.parse_file(A)
        t1 = (per["SQZ"]["q"] == 900000
              and max(per["SQZ"]["days"].values()) == 400000
              and abs(per["SQZ"]["usd"] - 27_000_000) < 1e-6
              and meta["n_rows"] == 5)

        def prior(q):
            return {"tag": "x", "per": {
                "SQZ": {"q": q, "usd": q * 30, "days": {"d": q},
                        "desc": "SQUEEZE CORP"},
                "BIGX": {"q": 500000, "usd": 2e7, "days": {"d": 500000},
                         "desc": "ISHARES BIG ETF"},
                "LOWP": {"q": 900000, "usd": 1e6, "days": {"d": 900000},
                         "desc": "PENNY CO"}}, "meta": {}}

        files = [{"tag": "202607a", "per": per, "meta": meta},
                 prior(180000), prior(180000), prior(180000)]
        t2 = abs(m.spike_map(files)["SQZ"] - 5.0) < 1e-9

        def vol(s):
            return ((600_000, 30.0) if s == "SQZ"
                    else (5_000_000, 40.0) if s == "BIGX"
                    else (9_000_000, 1.2))

        rows, quals = m.evaluate(files, vol)
        t3 = ("BIGX" not in [r["t"] for r in rows]
              and len(quals) == 1 and quals[0]["t"] == "SQZ"
              and quals[0]["dtc_peak"] == 0.67
              and quals[0]["spike"] == 5.0 and quals[0]["usd"] == 27.0)
        _, q2 = m.evaluate([files[0], prior(180000)], vol)
        t4 = q2 == []
        gate("L1_ci_battery", all([t1, t2, t3, t4]),
             {"parse": t1, "spike": t2, "floors_etf": t3,
              "few_priors": t4})
    except Exception as e:  # noqa: BLE001
        gate("L1_ci_battery", False, str(e)[:340])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY},
        timeout=600, memory=512,
        description="Equity FTD family v1.0.0 (ops 3513)",
        create_function_url=False, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN,
               Payload=json.dumps({"force": True}).encode())
    time.sleep(2)
    try:
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
                                        Key="data/equity-ftd.json")
                         ["Body"].read())
        F = doc.get("floors") or {}
        quals = doc.get("qualifiers") or []
        floors_ok = all(
            (r.get("px") or 0) >= F["price"]
            and (r.get("avg20") or 0) >= F["avg20"]
            and (r.get("usd") or 0) * 1e6 >= F["usd"]
            and (r.get("spike") or 0) >= F["spike"]
            and (r.get("dtc_peak") or 0) >= F["dtc_peak"]
            for r in quals)
        withvol = sum(1 for r in (doc.get("top_dollars") or [])
                      + (doc.get("top_spikes") or [])
                      if r.get("avg20"))
        top1 = ((doc.get("top_dollars") or [{}])[0].get("t"))
        gate("L2_live_run",
             len(doc.get("files") or []) >= 4
             and (doc.get("universe_n") or 0) > 10000
             and (doc.get("n_candidates") or 0) >= 100
             and withvol >= 100 and floors_ok
             and top1 in {"GOOG", "GOOGL", "AMD", "XOM", "PG", "MS",
                          "QCOM", "UNP"},
             {"files": doc.get("files"),
              "universe_n": doc.get("universe_n"),
              "n_candidates": doc.get("n_candidates"),
              "top_dollars_3": [(r["t"], r["usd"]) for r in
                                (doc.get("top_dollars") or [])[:3]],
              "top_spikes_3": [(r["t"], r["spike"], r["dtc_peak"])
                               for r in
                               (doc.get("top_spikes") or [])[:3]],
              "qualifiers": quals, "signals": doc.get("signals"),
              "logged": doc.get("logged"),
              "elapsed_s": doc.get("elapsed_s")})
    except Exception as e:  # noqa: BLE001
        gate("L2_live_run", False, str(e)[:340])

    try:
        role = iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
        fn_arn = lam.get_function(FunctionName=FN)["Configuration"][
            "FunctionArn"]
        name = "equity-ftd-sched"
        body = dict(
            Name=name, ScheduleExpression="cron(10 15 ? * MON,THU *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": fn_arn, "RoleArn": role,
                    "Input": json.dumps({})},
            State="ENABLED",
            Description="Equity FTD family — SEC publishes ~2wk lag")
        try:
            sch.create_schedule(**body)
        except sch.exceptions.ConflictException:
            sch.update_schedule(**body)
        d = sch.get_schedule(Name=name)
        gate("L3_schedule", d["ScheduleExpression"]
             == "cron(10 15 ? * MON,THU *)",
             {"name": name, "expr": d["ScheduleExpression"]})
    except Exception as e:  # noqa: BLE001
        gate("L3_schedule", False, str(e)[:280])

    got = b""
    for _ in range(15):
        try:
            got = fetch("https://justhodl.ai/alpha-families.html?cb=%d"
                        % int(time.time()))
            if b"b-ftd" in got:
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(20)
    scr = re.findall(rb"<script>([\s\S]*?)</script>", got)
    ok4 = False
    if scr:
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(scr[-1])
            pth = f.name
        ok4 = subprocess.run(["node", "--check", pth],
                             capture_output=True).returncode == 0
    gate("L4_families_page",
         b"b-ftd" in got and b"equity-ftd.json" in got
         and b"FTD Squeeze" in got and ok4,
         {"card": b"b-ftd" in got, "feed": b"equity-ftd.json" in got,
          "node": ok4})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3513.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
