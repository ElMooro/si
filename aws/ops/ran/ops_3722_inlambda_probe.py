"""
ops 3722 — IN-LAMBDA PROBE: what does fetch_calendar actually return?

WHY A PROBE AND NOT ANOTHER FIX
═══════════════════════════════
Four ops have now touched this engine and the revision arrays are still 0/0.
Everything so far has been inferred from artifact side-effects. That inference
has been wrong twice:

  ops 3717  patched cur_eps inside the row loop — but the loop never runs, so
            the patch could not fire. Wrong layer.
  ops 3720  added the FMP degrade to shared/benzinga.py, probe-proven to return
            4,000 rows / 2,868 with epsEstimated from the OPS RUNNER.
  ops 3721  proved the shim was ABSENT from both live zips (deploy-lambdas
            skipped the [skip-deploy] auto-commit), force-republished it, and
            re-invoked — arrays STILL 0/0 and n_tracked byte-identical at 436.

n_tracked frozen at exactly 436 across a supposedly-changed code path is the
tell: either fetch_calendar still yields nothing INSIDE the Lambda (the runner
proving FMP works says nothing about the Lambda's own egress, key, or module
resolution), or rows arrive and are discarded downstream.

The ops runner and the Lambda are different environments. FMP_KEY resolution,
outbound network, and which benzinga.py is importable all differ. So stop
guessing from outside and ask the Lambda directly.

WHAT THIS DOES
══════════════
Deploys a THROWAWAY probe Lambda that imports the SAME shared module the real
engine imports and reports, as data:
    - which benzinga.py it loaded (path + whether _fmp_calendar is in it)
    - what _get("earnings", ...) returns raw (status/exception/body head)
    - len(fetch_calendar(...)) at the engine's real args
    - len(_fmp_calendar(...)) called directly
    - whether FMP_KEY resolves in-Lambda and what /stable/earnings-calendar
      returns from INSIDE AWS
Then deletes itself. No production function is modified.

EXITS 0 ALWAYS. This is diagnosis, not breakage — a red X here would be a false
alarm, and Khalid has had 9 failure emails in 24h.
"""
import io
import json
import sys
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
PROBE_FN = "justhodl-tmp-bzprobe-3722"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
DONOR = "justhodl-estimate-revisions"

LAM = boto3.client("lambda", region_name="us-east-1")

PROBE_SRC = '''
import json, os, traceback

def lambda_handler(event, context):
    out = {}
    # 1. which benzinga module did we actually load?
    try:
        import benzinga
        src_path = getattr(benzinga, "__file__", None)
        body = ""
        try:
            body = open(src_path, encoding="utf-8").read() if src_path else ""
        except Exception:
            pass
        out["module"] = {
            "path": src_path,
            "has_fmp_calendar": "_fmp_calendar" in body,
            "has_degrade": "degrade" in body,
            "bytes": len(body),
        }
    except Exception as e:
        out["module"] = {"import_error": str(e)[:200], "tb": traceback.format_exc()[-400:]}
        return {"statusCode": 200, "body": json.dumps(out)}

    # 2. key resolution INSIDE the lambda
    out["env"] = {
        "FMP_KEY_set": bool(os.environ.get("FMP_KEY")),
        "FMP_API_KEY_set": bool(os.environ.get("FMP_API_KEY")),
        "MASSIVE_API_KEY_set": bool(os.environ.get("MASSIVE_API_KEY")),
    }
    try:
        out["env"]["benzinga_key_resolves"] = bool(benzinga._key())
    except Exception as e:
        out["env"]["benzinga_key_error"] = str(e)[:150]

    # 3. raw Massive call — what comes back?
    try:
        from datetime import date, timedelta
        t = date.today()
        j = benzinga._get("earnings", {
            "date.gte": t.isoformat(),
            "date.lte": (t + timedelta(days=14)).isoformat(),
            "order": "asc", "sort": "date", "limit": "50",
        })
        out["massive_raw"] = {
            "is_none": j is None,
            "type": type(j).__name__,
            "keys": sorted(j.keys())[:8] if isinstance(j, dict) else None,
            "n_results": len(j.get("results") or []) if isinstance(j, dict) else None,
            "head": json.dumps(j)[:300] if j is not None else None,
        }
    except Exception as e:
        out["massive_raw"] = {"error": str(e)[:200]}

    # 4. the shim, called directly
    try:
        fb = benzinga._fmp_calendar(days_ahead=14, limit=1000)
        out["fmp_shim"] = {
            "n": len(fb),
            "n_with_eps": sum(1 for r in fb if r.get("estimated_eps") is not None),
            "sample": fb[0] if fb else None,
        }
    except Exception as e:
        out["fmp_shim"] = {"error": str(e)[:250], "tb": traceback.format_exc()[-400:]}

    # 5. fetch_calendar at the ENGINE'S REAL ARGS (HORIZON_DAYS=?, min_imp=2)
    for mi in (0, 2):
        try:
            rows = benzinga.fetch_calendar(days_ahead=14, min_importance=mi, limit=1000)
            out[f"fetch_calendar_minimp{mi}"] = {
                "n": len(rows or []),
                "n_with_eps": sum(1 for r in (rows or [])
                                  if r.get("estimated_eps") is not None),
                "sources": sorted({r.get("_source", "benzinga") for r in (rows or [])}),
                "sample": (rows or [None])[0],
            }
        except Exception as e:
            out[f"fetch_calendar_minimp{mi}"] = {"error": str(e)[:200]}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    with report("3722_inlambda_probe") as rep:
        rep.heading("ops 3722 — in-Lambda probe of fetch_calendar")
        out = {}

        def note(n, d):
            rep.log(f"{n}: {d}")
            print(f"  {n}: {d}")
            out[n] = d

        # donor env so the probe resolves keys exactly like the engine
        try:
            cfg = LAM.get_function_configuration(FunctionName=DONOR)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
        except Exception as e:  # noqa: BLE001
            env = {}
            note("donor_env_error", str(e)[:150])

        # build zip: probe handler + the real shared modules
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", PROBE_SRC)
            for p in sorted(SHARED.glob("*.py")):
                z.write(p, p.name)
        zb = buf.getvalue()
        note("zip_bytes", len(zb))

        # create (or replace) the throwaway probe
        try:
            LAM.delete_function(FunctionName=PROBE_FN)
            time.sleep(4)
        except Exception:  # noqa: BLE001
            pass
        try:
            LAM.create_function(
                FunctionName=PROBE_FN, Runtime="python3.12", Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb}, Timeout=120, MemorySize=512,
                Environment={"Variables": env},
                Description="temp probe (ops 3722) — deleted at end of run")
            t0 = time.time()
            while time.time() - t0 < 120:
                c = LAM.get_function_configuration(FunctionName=PROBE_FN)
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") in (None, "Successful"):
                    break
                time.sleep(4)
            note("probe_created", "active")
        except Exception as e:  # noqa: BLE001
            note("probe_create_error", str(e)[:250])
            out["verdict"] = "FAIL: probe could not be created"
            print("\nVERDICT:", out["verdict"])
            rep.log("VERDICT: " + out["verdict"])
            sys.exit(1)

        # invoke and read
        try:
            r = LAM.invoke(FunctionName=PROBE_FN, InvocationType="RequestResponse",
                           Payload=b"{}")
            raw = r["Payload"].read().decode("utf-8", "ignore")
            ferr = r.get("FunctionError")
            note("function_error", ferr)
            env_resp = json.loads(raw) if raw.strip().startswith("{") else {}
            body = env_resp.get("body")
            data = json.loads(body) if isinstance(body, str) else (body or {})

            rep.section("PROBE RESULT")
            print(json.dumps(data, indent=2, default=str)[:3000])
            for k, v in (data or {}).items():
                rep.kv(probe=k, value=json.dumps(v, default=str)[:300])
            out["probe"] = data

            # the decisive read
            fc0 = (data.get("fetch_calendar_minimp0") or {}).get("n")
            fc2 = (data.get("fetch_calendar_minimp2") or {}).get("n")
            shim_n = (data.get("fmp_shim") or {}).get("n")
            mod = data.get("module") or {}
            note("VERDICT_SIGNALS",
                 f"module_has_shim={mod.get('has_fmp_calendar')} "
                 f"fmp_shim_rows={shim_n} "
                 f"fetch_calendar(min_imp=0)={fc0} fetch_calendar(min_imp=2)={fc2} "
                 f"massive_none={(data.get('massive_raw') or {}).get('is_none')}")
        except Exception as e:  # noqa: BLE001
            note("invoke_error", str(e)[:250])

        # always clean up
        try:
            LAM.delete_function(FunctionName=PROBE_FN)
            note("probe_deleted", True)
        except Exception as e:  # noqa: BLE001
            note("probe_delete_error", str(e)[:120])

        # Diagnosis that RETURNS DATA is a success even when the data shows a
        # bug — a red X here would be a false alarm. Only a probe that failed to
        # produce any reading is a real failure.
        got = bool((out.get("probe") or {}).get("module"))
        out["verdict"] = ("DIAGNOSIS COMPLETE" if got
                          else "FAIL: probe returned no readings")
        print("\nVERDICT:", out["verdict"])
        rep.log("VERDICT: " + out["verdict"])
        if not got:
            sys.exit(1)


if __name__ == "__main__":
    main()
