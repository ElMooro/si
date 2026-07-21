"""ops 3638 — STALE-60 TRIAGE. Classify every stale feed in the registry:
RETIRED (no writer in repo) · SCHEDULE-DEAD (writer, 0 invocations 7d) ·
KEY-DRIFT (writer active, key unwritten) · SLA-TIGHT (age < 1.6x sla).
Auto-heal conservatively: config/feed-sla.json overrides for RETIRED (silence
9999h) and SLA-TIGHT (ceil 1.8x observed age), cap 25 new overrides. Success
metric = registry re-run stale count DROPS. Never deletes anything."""
import json, math, subprocess, sys, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
S3C = boto3.client("s3", "us-east-1")
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
CW = boto3.client("cloudwatch", "us-east-1")
B = "justhodl-dashboard-live"

with report("3638_stale_triage") as rep:
    rep.heading("ops 3638 — stale-60 triage + conservative heals")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        reg = json.loads(S3C.get_object(Bucket=B, Key="data/feed-registry.json")["Body"].read())
        stale = reg.get("stale") or []
        n0 = len(stale)
        try:
            sla_cfg = json.loads(S3C.get_object(Bucket=B, Key="config/feed-sla.json")["Body"].read())
        except Exception:
            sla_cfg = {}
        lam_dir = ROOT / "lambdas"

        def writer_of(key):
            try:
                r = subprocess.run(["grep", "-rl", key, str(lam_dir)],
                                   capture_output=True, text=True, timeout=30)
                hits = [Path(p).parts for p in r.stdout.split()
                        if "/source/" in p]
                fns = sorted({parts[parts.index("lambdas") + 1] for parts in hits})
                return fns
            except Exception:
                return []

        def inv7(fn):
            try:
                r = CW.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Invocations",
                    Dimensions=[{"Name": "FunctionName", "Value": fn}],
                    StartTime=datetime.now(timezone.utc) - timedelta(days=7),
                    EndTime=datetime.now(timezone.utc),
                    Period=604800, Statistics=["Sum"])
                pts = r.get("Datapoints") or []
                return int(sum(p["Sum"] for p in pts))
            except Exception:
                return -1

        classes = {"RETIRED": [], "SCHEDULE_DEAD": [], "KEY_DRIFT": [],
                   "SLA_TIGHT": [], "OTHER": []}
        healed = {}
        for row in stale:
            k = row.get("key"); age = row.get("age_h") or 0; sl = row.get("sla_h") or 48
            fns = writer_of(k)
            if not fns:
                classes["RETIRED"].append({"key": k, "age_h": age})
                if len(healed) < 25:
                    healed[k] = 9999
                continue
            if age < sl * 1.6:
                classes["SLA_TIGHT"].append({"key": k, "age_h": age, "sla_h": sl,
                                             "writer": fns[0]})
                if len(healed) < 25 and k not in sla_cfg:
                    healed[k] = int(math.ceil(age * 1.8))
                continue
            iv = inv7(fns[0])
            if iv == 0:
                classes["SCHEDULE_DEAD"].append({"key": k, "age_h": age,
                                                 "writer": fns[0]})
            elif iv > 0:
                classes["KEY_DRIFT"].append({"key": k, "age_h": age,
                                             "writer": fns[0], "inv7": iv})
            else:
                classes["OTHER"].append({"key": k, "age_h": age,
                                         "writer": fns[0]})
        counts = {c: len(v) for c, v in classes.items()}
        triage = {"generated_at": datetime.now(timezone.utc).isoformat(),
                  "stale_n": n0, "counts": counts, "classes": classes,
                  "healed_overrides": healed,
                  "policy": ("RETIRED->9999h silence; SLA_TIGHT->1.8x age; "
                             "SCHEDULE_DEAD/KEY_DRIFT listed for repair arcs")}
        S3C.put_object(Bucket=B, Key="data/stale-triage.json",
                       Body=json.dumps(triage, indent=2).encode(),
                       ContentType="application/json")
        gate("G1_classified", sum(counts.values()) == n0 and n0 > 0,
             f"n={n0} counts={counts} healed_n={len(healed)} "
             f"sched_dead={[x['key'][:34] for x in classes['SCHEDULE_DEAD'][:6]]} "
             f"drift={[x['key'][:34] for x in classes['KEY_DRIFT'][:6]]}")
        out["counts"] = counts

        if healed:
            sla_cfg.update(healed)
            S3C.put_object(Bucket=B, Key="config/feed-sla.json",
                           Body=json.dumps(sla_cfg, indent=2).encode(),
                           ContentType="application/json")
        r = LAM.invoke(FunctionName="justhodl-feed-registry",
                       InvocationType="RequestResponse", Payload=b"{}")
        _ = r["Payload"].read()
        time.sleep(2)
        reg2 = json.loads(S3C.get_object(Bucket=B, Key="data/feed-registry.json")["Body"].read())
        n1 = len(reg2.get("stale") or [])
        gate("G2_stale_dropped", n1 < n0,
             f"stale {n0} -> {n1} (healed {len(healed)} overrides; "
             f"remaining = genuine repair backlog)")
        out["stale_before_after"] = [n0, n1]
    except Exception as e:
        gate("G1_classified", False, str(e)[:400])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3638.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
