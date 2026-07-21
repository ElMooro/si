"""ops 3650 — NEW justhodl-portwatch (IMF PortWatch chokepoints, discovered
via World Monitor reverse-engineering; keyless public ArcGIS). Daily transit
7d vs 1y baseline + z + DISRUPTED/ELEVATED flags + IMF disruptions join.
Creates fn + Scheduler 12:10 UTC, invokes, gates data (Suez/Hormuz present,
>=5 chokepoints, z numeric), macro-leads row + MI zip. Also records the
reverse-engineering catalog verdicts."""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=600, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
FN = "justhodl-portwatch"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3650_ports_panel") as rep:
    rep.heading("ops 3650 — IMF PortWatch chokepoint engine")
    out = {"gates": {}, "catalog": {
        "already_ours": {"ecb_data_api": 16, "gdelt": 3, "aaii": 12,
                          "finnhub": 4, "fred/eia/yahoo/sec/treasury/bls":
                          "fleet-wide"},
        "new_adopted": "IMF PortWatch (this ops)",
        "queued": ["BIS stats API WS_CBS/LBS (eurodollar cross-border claims)",
                    "UN Comtrade (laggy monthly trade)"],
        "skipped_no_edge": ["MITRE ATT&CK", "OpenAQ/WAQI", "aviationstack",
                             "arxiv", "eur-lex raw", "ACLED (ToS/commercial)"]}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / FN / "source",
                      env_vars={}, timeout=300, memory=768,
                      description="portwatch v1.1 (+major-ports layer): IMF chokepoint transits, z + disruption flags"[:200],
                      create_function_url=False)
        arn = LAM.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
        cfg = dict(Name="justhodl-portwatch-daily",
                   ScheduleExpression="cron(10 12 * * ? *)",
                   FlexibleTimeWindow={"Mode": "OFF"},
                   Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                   State="ENABLED")
        try:
            SCH.create_schedule(**cfg)
        except Exception:
            SCH.update_schedule(**cfg)
        try:
            LAM.add_permission(FunctionName=FN, StatementId="sched-portwatch",
                               Action="lambda:InvokeFunction",
                               Principal="scheduler.amazonaws.com")
        except LAM.exceptions.ResourceConflictException:
            pass
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/portwatch.json")["Body"].read())
        cps = j.get("chokepoints") or []
        nm = " ".join(c["name"].lower() for c in cps)
        ok1 = (not err and len(cps) >= 5
               and ("suez" in nm) and ("hormuz" in nm)
               and all(isinstance(c.get("z"), (int, float)) for c in cps[:5]))
        gate("G1_data", ok1,
             f"err={err} layer={j.get('daily_layer')} rows={j.get('daily_rows')} "
             f"metric={j.get('metric_field')} n={len(cps)} worst={j.get('worst')} "
             f"pids={j.get('pids_seen')} span={j.get('date_span')} disrupted={j.get('n_disrupted')} sample={[(c['name'], c['z'], c['vs_baseline_pct'], c['status']) for c in cps[:5]]} "
             f"ports_n={len(j.get('ports') or [])} ports_metric={j.get('ports_metric')} ports_sample={[(pp['name'], pp['z'], pp['status']) for pp in (j.get('ports') or [])[:5]]} errors={j.get('errors')}")
        out["snapshot"] = {"worst": j.get("worst"),
                            "sample": [(c['name'], c['z'], c['status']) for c in cps[:8]],
                            "disruptions_n": len(j.get('disruptions') or [])}
    except Exception as e:
        gate("G1_data", False, str(e)[:380])

    # MI zip + macro-leads served
    try:
        cfg2 = LAM.get_function_configuration(FunctionName="justhodl-morning-intelligence")
        deploy_lambda(report=rep, function_name="justhodl-morning-intelligence",
                      source_dir=ROOT / "lambdas" / "justhodl-morning-intelligence" / "source",
                      env_vars=(cfg2.get("Environment") or {}).get("Variables") or {},
                      timeout=cfg2.get("Timeout", 300), memory=cfg2.get("MemorySize", 1024),
                      description=(cfg2.get("Description") or "MI")[:200],
                      create_function_url=False)
        loc = LAM.get_function(FunctionName="justhodl-morning-intelligence")["Code"]["Location"]
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        mi_ok = b"portwatch" in blob
    except Exception as e:
        mi_ok = False
    ok2 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            h = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/defcon.html?cb=" + str(int(time.time())),
                headers={"User-Agent": "Mozilla/5.0"}), timeout=30).read().decode("utf-8", "replace")
            det = f"panel={'jh-shipping' in h} ports_col={'MAJOR PORTS' in h} mi_zip={mi_ok}"
            if "Chokepoints" in h and "portwatch.json" in h and mi_ok:
                ok2 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    gate("G2_wires", ok2, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3650.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
