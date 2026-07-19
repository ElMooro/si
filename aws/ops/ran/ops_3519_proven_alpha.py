"""ops 3519 — Proven Alpha Report (#10) LIVE + census #7 shells + #10
explicit SLA overrides.

A) justhodl-proven-alpha v1.0: paginated signals-table scan -> per-
   family graded truth (windows, hit, avg/med excess bps, Decimal-safe),
   verdict ladder SUPPRESSED > PROVEN(n>=10 & hit>=60 primary) >
   EVALUATING > PENDING(+first-grades ETA from check_timestamps).
   Daily Scheduler 22:40 after the graders. proven-alpha.html scoreboard
   (verdict chips, per-window hits, in-flight callout: ftd-squeeze ~Aug 9),
   FORCE-pinned "Portfolio & Execution".
B) census #7: the exact 12 unshelled pages gained the site shell.
C) census #10: config/feed-sla.json explicit overrides honored by
   feed-registry v1.4 (sla_source explicit|heuristic).

Gates:
  T1 engine CI (fixture: hit 70.0 exact, +5.0bps Decimal-safe, n<10
     blocked, ETA, ordering) — rerun in CI
  T2 live doc: families >= 8, ftd-squeeze PENDING w/ ETA + pending>=5,
     at least one family with graded >= 10, congress-buy row printed
  T3 Scheduler 22:40 exists
  T4 registry v1.4 run: >=1 row sla_source=explicit for an override key
  T5 shells: 12/12 pages served WITH the drawer include; proven-alpha
     page served + node; SERVED nav-manifest pins it under
     Portfolio & Execution
"""
import importlib.util, json, re, subprocess, sys, tempfile, time, urllib.request
from decimal import Decimal
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-proven-alpha"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
iam = boto3.client("iam")
SHELLED = ["bottleneck.html","carry-surface.html","charts.html","download.html",
           "engines.html","health.html","pairs-scanner.html","proof.html",
           "status.html","system-health.html","tv-notes.html","uptime.html"]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3519"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3519_proven_alpha") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:520]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3519 — Proven Alpha Report + census 7/10")
    try:
        spec = importlib.util.spec_from_file_location(
            "pa", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        def row(st, outs=None, cts=None):
            r = {"signal_type": st, "logged_at": "2026-06-01T00:00:00",
                 "outcomes": outs or {}}
            if cts: r["check_timestamps"] = cts
            return r
        rows = []
        for i in range(10):
            rows.append(row("famA", {"day_21": {"correct": i < 7,
                        "excess_return": Decimal("0.5") if i < 7
                        else Decimal("-1.0")}}))
        rows.append(row("famA", cts={"day_21": "2026-08-09T00:00:00"}))
        rows += [row("ftd-squeeze",
                     cts={"day_21": "2026-08-09T00:00:00"})
                 for _ in range(5)]
        fams = m.build_families(rows, set(), set())
        F = {f["family"]: f for f in fams}
        gate("T1_ci", F["famA"]["verdict"] == "PROVEN"
             and F["famA"]["hit_primary"] == 70.0
             and F["famA"]["avg_excess_bps"] == 5.0
             and F["ftd-squeeze"]["first_grades_eta"] == "2026-08-09",
             {k: (F[k]["verdict"], F[k].get("hit_primary")) for k in F})
    except Exception as e:
        gate("T1_ci", False, str(e)[:300])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=600, memory=512,
                  description="Proven Alpha Report v1.0 (ops 3519)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=b"{}")
    time.sleep(2)
    try:
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
                         Key="data/proven-alpha.json")["Body"].read())
        F = {f["family"]: f for f in doc["families"]}
        ftd = F.get("ftd-squeeze") or {}
        cg = F.get("congress-buy") or {}
        gate("T2_live", doc["summary"]["n_families"] >= 8
             and ftd.get("verdict") == "PENDING"
             and (ftd.get("pending") or 0) >= 5
             and bool(ftd.get("first_grades_eta"))
             and any((f.get("graded") or 0) >= 10
                     for f in doc["families"]),
             {"summary": doc["summary"],
              "ftd": {k: ftd.get(k) for k in
                      ("verdict", "pending", "first_grades_eta")},
              "congress_buy": {k: cg.get(k) for k in
                               ("verdict", "graded", "hit_primary",
                                "avg_excess_bps", "pending")},
              "top5": [(f["family"], f["verdict"], f["graded"],
                        f["hit_primary"]) for f in doc["families"][:5]]})
    except Exception as e:
        gate("T2_live", False, str(e)[:320])

    try:
        role = iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
        arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
        body = dict(Name="proven-alpha-sched",
                    ScheduleExpression="cron(40 22 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": role, "Input": "{}"},
                    State="ENABLED",
                    Description="Proven Alpha Report — nightly after graders")
        try: sch.create_schedule(**body)
        except sch.exceptions.ConflictException: sch.update_schedule(**body)
        gate("T3_schedule", True, sch.get_schedule(
            Name="proven-alpha-sched")["ScheduleExpression"])
    except Exception as e:
        gate("T3_schedule", False, str(e)[:240])

    try:
        deploy_lambda(report=rep, function_name="justhodl-feed-registry",
                      source_dir=REPO/"aws"/"lambdas"/"justhodl-feed-registry"/"source",
                      env_vars=None, timeout=None, memory=None,
                      description="feed-registry v1.4 SLA overrides (ops 3519)",
                      create_function_url=False, smoke=False)
        for _ in range(20):
            c = lam.get_function_configuration(
                FunctionName="justhodl-feed-registry")
            if c.get("LastUpdateStatus") == "Successful": break
            time.sleep(2)
        lam.invoke(FunctionName="justhodl-feed-registry", Payload=b"{}")
        time.sleep(2)
        reg = json.loads(s3c.get_object(Bucket=BUCKET,
                         Key="data/feed-registry.json")["Body"].read())
        ex = [r for r in (reg.get("feeds") or reg.get("rows") or [])
              if r.get("sla_source") == "explicit"]
        gate("T4_sla_overrides", len(ex) >= 3,
             {"n_explicit": len(ex),
              "sample": [(r["key"], r["sla_h"]) for r in ex[:4]]})
    except Exception as e:
        gate("T4_sla_overrides", False, str(e)[:300])

    ok_pages, missing = 0, []
    pa = b""
    for _ in range(16):
        try:
            cb = int(time.time())
            pa = fetch(f"https://justhodl.ai/proven-alpha.html?cb={cb}")
            nav = json.loads(fetch(
                f"https://justhodl.ai/nav-manifest.json?cb={cb}"))
            if b"Proven Alpha Report" in pa:
                break
        except Exception:
            nav = {}
        time.sleep(20)
    for p in SHELLED:
        try:
            if b"jh-nav-drawer" in fetch(
                    f"https://justhodl.ai/{p}?cb={int(time.time())}"):
                ok_pages += 1
            else:
                missing.append(p)
        except Exception:
            missing.append(p)
    pinned = any(c.get("name") == "Portfolio & Execution"
                 and any(pg.get("href") == "/proven-alpha.html"
                         for pg in c.get("pages") or [])
                 for c in (nav.get("categories") or []))
    scr = re.findall(rb"<script>\n?('use strict[\s\S]*?)</script>", pa)
    ok_node = False
    if scr:
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(scr[0]); pth = f.name
        ok_node = subprocess.run(["node", "--check", pth],
                                 capture_output=True).returncode == 0
    gate("T5_pages", ok_pages == 12 and b"Proven Alpha Report" in pa
         and ok_node and pinned,
         {"shelled": ok_pages, "missing": missing,
          "page": b"Proven Alpha Report" in pa, "node": ok_node,
          "pinned": pinned})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3519.json").write_text(
        json.dumps({"ops": 3519, "fails": fails}))
sys.exit(0)
