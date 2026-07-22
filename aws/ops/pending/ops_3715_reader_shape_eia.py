"""
ops 3715 — readthrough reader shape fix + EIA re-gate

WHAT 3714 PROVED (both defects pinpointed, neither guessed)
═══════════════════════════════════════════════════════════

[A] forward-orders is HEALTHY. Artifact age 20.0h, schedule alive. The join
    failed on a SCHEMA-NAME MISMATCH:

        producer writes : top_25_by_score, all_results   (schema_version 3.0)
        reader tries    : by_ticker, results, rankings, top_picks, all, scored

    "all_results" is near-miss close to both "all" and "results" and matches
    neither. Confirmed from producer source: `"all_results": results` where
    results is a list of dicts each carrying "ticker" (line 515) — precisely
    the shape the reader already handles for list containers.

    FIX: extend the reader tuple. One line, in readthrough only. forward-orders
    is untouched — it is not broken.

    This is the SAME BUG CLASS as ops 3712 (gate read consensus_honesty, engine
    wrote consensus_coverage) and ops 3611 (portfolios vs benchmark_portfolios).
    Third occurrence. The durable countermeasure is the G0 key-contract gate:
    assert the producer key EXISTS before consuming it.

[B] EIA key was set SUCCESSFULLY at 3714. My gate was wrong, not the fix:
        FunctionError=None, statusCode=200, still_asking_for_key=False
    but the gate demanded the literal '"eia_key_present": true' in the raw
    payload, and that field lives inside a nested JSON `body` string, so the
    substring never matched. Re-gated here by PARSING the response.

CARRIED FORWARD (buildout-canary audit, do not re-research)
══════════════════════════════════════════════════════════
    interconnection queues -> BLOCKED ON CREDENTIAL. structural-pre-signals
      already uses an SEC EDGAR full-text proxy because ERCOT needs a numeric
      report ID with no search path and PJM needs a registered API key.
    book-to-bill / backlog -> justhodl-forward-orders (score_book_to_bill)
    patent velocity        -> justhodl-patent-velocity
    canary-grid            -> CRISIS polarity, 30 stress leads. A boom grid
                              must not reuse the name.
"""
import io
import json
import sys
import time
import zipfile
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

import boto3  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RT_SRC = ROOT / "lambdas" / "justhodl-readthrough" / "source" / "lambda_function.py"
RT_FN = "justhodl-readthrough"
EIA_FN = "eia-energy-agent"
BUCKET = "justhodl-dashboard-live"
KEY = "data/readthrough.json"

LAM = boto3.client("lambda", region_name="us-east-1")
S3C = boto3.client("s3", region_name="us-east-1")

OLD_TUPLE = '''    for k in ("by_ticker", "results", "rankings", "top_picks", "all", "scored"):'''
NEW_TUPLE = '''    for k in ("by_ticker", "results", "rankings", "top_picks", "all", "scored",
              "all_results", "top_25_by_score"):'''


def main():
    with report("3715_reader_shape_eia") as rep:
        rep.heading("ops 3715 — readthrough reader shape fix + EIA re-gate")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:32} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        # ── A. patch the reader ─────────────────────────────────────────────
        rep.section("A — readthrough reader shape")
        src = io.open(RT_SRC, encoding="utf-8").read()
        if OLD_TUPLE not in src and "all_results" in src:
            gate("A1_patch_applied", True, "already patched (idempotent re-run)")
        else:
            if OLD_TUPLE not in src:
                gate("A1_patch_applied", False,
                     "anchor not found — reader tuple changed shape, inspect manually")
            else:
                src = src.replace(OLD_TUPLE, NEW_TUPLE, 1)
                src = src.replace('VERSION = "1.2.1"', 'VERSION = "1.2.2"', 1)
                io.open(RT_SRC, "w", encoding="utf-8").write(src)
                gate("A1_patch_applied", 'all_results' in src and 'VERSION = "1.2.2"' in src,
                     "reader tuple += all_results, top_25_by_score; VERSION -> 1.2.2")

        if not fails:
            # env MUST be preserved — inherit live config, never blank it
            _cfg = LAM.get_function_configuration(FunctionName=RT_FN)
            _env = (_cfg.get("Environment") or {}).get("Variables") or {}
            deploy_lambda(report=rep, function_name=RT_FN,
                          source_dir=RT_SRC.parent,
                          env_vars=_env,
                          description="Catalyst read-through / diffusion engine "
                                      "(Opportunities). v1.2.2: reader accepts "
                                      "forward-orders all_results schema.",
                          timeout=600, memory=1024,
                          create_function_url=False, smoke=False)

            # settle: prove the NEW artifact is live before invoking (3701 lesson)
            t0 = time.time()
            live = False
            while time.time() - t0 < 300:
                try:
                    loc = LAM.get_function(FunctionName=RT_FN)["Code"]["Location"]
                    z = zipfile.ZipFile(
                        io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
                    s = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if 'VERSION = "1.2.2"' in s and "all_results" in s:
                        live = True
                        break
                except Exception as e:  # noqa: BLE001
                    print("   settle retry:", str(e)[:60])
                time.sleep(15)
            gate("A2_artifact_live", live,
                 f"v1.2.2 zip-proven live after {round(time.time()-t0)}s")

            if live:
                before = None
                try:
                    before = S3C.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
                except Exception:  # noqa: BLE001
                    pass
                LAM.invoke(FunctionName=RT_FN, InvocationType="Event", Payload=b"{}")
                t0 = time.time()
                fresh = False
                while time.time() - t0 < 420:
                    time.sleep(15)
                    try:
                        h = S3C.head_object(Bucket=BUCKET, Key=KEY)
                        if before is None or h["LastModified"] > before:
                            fresh = True
                            break
                    except Exception:  # noqa: BLE001
                        pass
                gate("A3_refreshed", fresh, f"artifact refreshed in {round(time.time()-t0)}s")

                d = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
                deg = d.get("degraded") or []
                still = [x for x in deg if "forward-orders" in str(x)]
                gate("A4_sidecar_joined", not still,
                     f"degraded={deg} (forward-orders entry must be GONE)")

                rows = d.get("beneficiaries") or []
                withf = sum(1 for x in rows
                            if (x.get("fundamentals") or {}).get("rpo_representative")
                            or (x.get("fundamentals") or {}).get("book_to_bill_spread_pct"))
                gate("A5_fwd_fields_present", withf > 0,
                     f"rows carrying forward-orders-derived fields: {withf}/{len(rows)}")

        # ── B. EIA re-gate (parse, do not substring) ────────────────────────
        rep.section("B — EIA re-gate")
        try:
            r = LAM.invoke(FunctionName=EIA_FN, InvocationType="RequestResponse",
                           Payload=b"{}")
            raw = r["Payload"].read().decode("utf-8", "ignore")
            ferr = r.get("FunctionError")
            env = json.loads(raw) if raw.strip().startswith("{") else {}
            body = env.get("body")
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except Exception:  # noqa: BLE001
                    body = {}
            body = body if isinstance(body, dict) else {}
            key_present = bool(body.get("eia_key_present"))
            steo = body.get("steo") or body.get("STEO") or {}
            steo_err = (steo or {}).get("error") if isinstance(steo, dict) else None
            gate("B1_eia_key_live", not ferr and key_present,
                 f"FunctionError={ferr} eia_key_present={key_present} "
                 f"steo_keys={list(steo)[:6] if isinstance(steo, dict) else type(steo).__name__} "
                 f"steo_error={str(steo_err)[:60]}")
        except Exception as e:  # noqa: BLE001
            gate("B1_eia_key_live", False, f"exception: {str(e)[:160]}")

        out["verdict"] = ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails))
        print("\nVERDICT:", out["verdict"])
        rep.log("VERDICT: " + out["verdict"])
        for _k, _v in out.items():
            if isinstance(_v, dict):
                rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:170])

        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
