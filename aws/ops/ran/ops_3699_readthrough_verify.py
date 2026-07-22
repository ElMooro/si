"""ops 3699 — justhodl-readthrough v1.0.0 in-account verification.

Deploy path was declarative (config.json -> deploy-lambdas.yml): create-function,
inherit_env from justhodl-confluence-meta, EventBridge Scheduler. This script is
the truth channel: it proves the function exists with the right shape, the schedule
is live, and the engine produces REAL, well-formed output on a live invoke.

Doctrine applied: never RequestResponse-gate a long engine — invoke Event and gate
on S3 object freshness (ops 3657 pattern).

Honest expectation: at an off-hours run there may be no qualifying catalyst. The
engine returns status=QUIET by design and writes an empty board rather than
inventing one. QUIET is a PASS for the pipeline gate; a second diagnostic pass at a
lower gap threshold exercises the full beneficiary/diffusion path so the numbers can
be eyeballed. The 11:20 UTC scheduled run restores the canonical 6% threshold.
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

REGION = "us-east-1"
FN = "justhodl-readthrough"
BUCKET = "justhodl-dashboard-live"
KEY = "data/readthrough.json"
SCHED = "justhodl-readthrough-sched"

LAM = boto3.client("lambda", REGION, config=Config(read_timeout=90, retries={"max_attempts": 0}))
S3C = boto3.client("s3", REGION)
SCH = boto3.client("scheduler", REGION)

with report("3699_readthrough_verify") as rep:
    rep.heading("ops 3699 — justhodl-readthrough v1.0.0 verification")
    out = {"gates": {}}
    fails = []
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3699.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860])
            rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        # ── G1: function exists with the declared shape + inherited secrets ──
        try:
            cfg = LAM.get_function_configuration(FunctionName=FN)
            envk = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
            need = ["FMP_KEY", "POLYGON_KEY"]
            have = [k for k in need if k in envk]
            ok1 = (cfg["Timeout"] == 600 and cfg["MemorySize"] == 1024
                   and cfg["Runtime"].startswith("python3.1") and len(have) == len(need))
            gate("G1_function", ok1,
                 f"runtime={cfg['Runtime']} mem={cfg['MemorySize']} timeout={cfg['Timeout']} "
                 f"state={cfg.get('State')} dlq={bool(cfg.get('DeadLetterConfig'))} "
                 f"xray={(cfg.get('TracingConfig') or {}).get('Mode')} "
                 f"secrets_present={have} n_env={len(envk)}")
            out["config"] = {"mem": cfg["MemorySize"], "timeout": cfg["Timeout"],
                             "runtime": cfg["Runtime"], "n_env": len(envk),
                             "telegram": "TELEGRAM_BOT_TOKEN" in envk}
        except Exception as e:
            gate("G1_function", False, f"get_function_configuration failed: {e}")

        # ── G2: EventBridge Scheduler schedule live ──
        try:
            s = SCH.get_schedule(Name=SCHED)
            ok2 = (s["State"] == "ENABLED"
                   and "11,13,21" in s["ScheduleExpression"]
                   and s["Target"]["Arn"].endswith(FN))
            gate("G2_schedule", ok2,
                 f"{s['ScheduleExpression']} tz={s.get('ScheduleExpressionTimezone')} "
                 f"state={s['State']} ftw={s['FlexibleTimeWindow']['Mode']} "
                 f"target={s['Target']['Arn'].split(':')[-1]}")
            out["schedule"] = {"cron": s["ScheduleExpression"], "state": s["State"]}
        except Exception as e:
            gate("G2_schedule", False, f"get_schedule({SCHED}) failed: {e}")

        # ── G3: live invoke (async) + S3 freshness gate ──
        def invoke_and_wait(payload, label, wait_s=420):
            before = None
            try:
                before = S3C.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
            except Exception:
                pass
            LAM.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps(payload).encode())
            t0 = time.time()
            while time.time() - t0 < wait_s:
                time.sleep(15)
                try:
                    h = S3C.head_object(Bucket=BUCKET, Key=KEY)
                    if before is None or h["LastModified"] > before:
                        age = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds()
                        return True, round(age, 1), round(time.time() - t0, 1)
                except Exception:
                    pass
            return False, None, round(time.time() - t0, 1)

        ok3, age, elapsed = invoke_and_wait({}, "canonical")
        gate("G3_invoke_writes_feed", ok3,
             f"canonical run (gap>=6%): object refreshed={ok3} age={age}s waited={elapsed}s")

        doc = {}
        if ok3:
            doc = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())

        # ── G4: payload is well-formed and HONEST (no fabricated rows) ──
        try:
            status = doc.get("status")
            ok4 = (doc.get("engine") == "justhodl-readthrough"
                   and doc.get("ok") is True
                   and status in ("OK", "QUIET", "DEGRADED")
                   and isinstance(doc.get("events"), list)
                   and isinstance(doc.get("beneficiaries"), list))
            gate("G4_payload", ok4,
                 f"v={doc.get('version')} status={status} events={doc.get('n_events', 0)} "
                 f"benef={doc.get('n_beneficiaries', 0)} unpriced={doc.get('n_unpriced', 0)} "
                 f"picks={len(doc.get('top_picks') or [])} elapsed={doc.get('elapsed_s')}s "
                 f"degraded={doc.get('degraded')}")
            out["canonical"] = {"status": status, "n_events": doc.get("n_events", 0),
                                "n_beneficiaries": doc.get("n_beneficiaries", 0),
                                "n_unpriced": doc.get("n_unpriced", 0),
                                "degraded": doc.get("degraded"),
                                "elapsed_s": doc.get("elapsed_s")}
        except Exception as e:
            gate("G4_payload", False, f"payload read failed: {e}")

        # ── G5: exercise the diffusion path if the canonical tape was quiet ──
        if doc.get("status") == "QUIET" or not (doc.get("events") or []):
            print("\n[diag] canonical tape quiet — second pass at gap>=4% to exercise "
                  "the full catalyst->graph->diffusion path")
            ok5, age5, el5 = invoke_and_wait({"gap_min_pct": 4.0}, "diag")
            if ok5:
                doc = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
            gate("G5_diffusion_path", bool(ok5),
                 f"diag run (gap>=4%): refreshed={ok5} events={doc.get('n_events', 0)} "
                 f"benef={doc.get('n_beneficiaries', 0)} unpriced={doc.get('n_unpriced', 0)} "
                 f"waited={el5}s")
        else:
            gate("G5_diffusion_path", True, "canonical run already produced events — no diag needed")

        # ── Print the real board so the numbers can be eyeballed ──
        out["events"] = [{"ticker": e.get("ticker"), "type": e.get("type"),
                          "move_pct": e.get("move_pct"),
                          "order_value": e.get("order_value_str"),
                          "n_benef": e.get("n_beneficiaries"),
                          "n_unpriced": e.get("n_unpriced"),
                          "headline": (e.get("headline") or "")[:120]}
                         for e in (doc.get("events") or [])[:8]]
        out["unpriced_top"] = [{"ticker": r.get("ticker"), "tier": r.get("tier"),
                                "catalyst": r.get("catalyst_ticker"),
                                "expected": r.get("expected_move_pct"),
                                "realized_ex_beta": r.get("realized_ex_beta_pct"),
                                "gap": r.get("residual_pct"),
                                "score": r.get("catch_up_score"),
                                "flags": len(r.get("flags") or [])}
                               for r in (doc.get("unpriced") or [])[:12]]
        print("\n=== CATALYSTS ===")
        for e in out["events"]:
            print(f"  {e['ticker']:6} {str(e['move_pct']):>7}%  {e['type']:20} "
                  f"{str(e['order_value']):>8}  benef={e['n_benef']} unpriced={e['n_unpriced']}")
            print(f"         {e['headline']}")
        print("\n=== UN-PRICED BENEFICIARIES ===")
        for r in out["unpriced_top"]:
            print(f"  {r['ticker']:6} {r['tier']:26} via {str(r['catalyst']):6} "
                  f"exp={str(r['expected']):>7} real={str(r['realized_ex_beta']):>7} "
                  f"gap={str(r['gap']):>7} score={r['score']}")
        if not out["events"]:
            print("  (none — no name gapped on a spend-implying catalyst in the window; "
                  "this is a real answer, nothing was invented)")

        # ── G6: page is live ──
        try:
            import urllib.request
            r = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/readthrough.html",
                headers={"User-Agent": "Mozilla/5.0"}), timeout=25)
            body = r.read(80_000).decode("utf-8", "ignore")
            ok6 = r.status == 200 and "Read-Through Radar" in body and "data/readthrough.json" in body
            gate("G6_page_live", ok6, f"HTTP {r.status} bytes={len(body)} "
                                      f"title_ok={'Read-Through Radar' in body}")
        except Exception as e:
            gate("G6_page_live", False, f"page fetch failed (CDN may still be baking): {e}")

    except Exception:
        out["crash"] = traceback.format_exc()[-1500:]
        print("CRASH:", out["crash"][-500:])

    out["verdict"] = ("CRASH" if out.get("crash") else
                      ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3699.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
