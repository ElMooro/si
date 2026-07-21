"""ops 3622 (regate of 3621) — WIRING SWEEP (Khalid: wire the huge improvements into the
engines that need them). [A] shared _regime_snapshot() += spx_ma regime/
breadth200/narrow + vol-migration state/asia + KR flash — every emitted
signal fleet-wide now carries this context (transitive via importers).
[B] alert-sentinel: new KR 1-20 print → daily-report line + state period.
[C] best-setups: industry-boom {score,rank} join per setup + asia_flash_
tailwind on semi-linked industries when KR flash ≥15%."""
import base64, json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=600, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3622_wiring2") as rep:
    rep.heading("ops 3621 — regime-context + sentinel + best-setups wiring")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:620]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:580]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    def dep(fn, subdir, tmo, mem):
        cfg = LAM.get_function_configuration(FunctionName=fn)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=ROOT / "lambdas" / subdir / "source",
                      env_vars=env, timeout=max(tmo, cfg.get("Timeout", 120)),
                      memory=max(mem, cfg.get("MemorySize", 256)),
                      description=(cfg.get("Description") or fn)[:200],
                      create_function_url=False)

    # [A+C] best-setups (bundles shared → zip marker proves fleet propagation)
    try:
        dep("justhodl-best-setups", "justhodl-best-setups", 600, 1536)
        loc = LAM.get_function(FunctionName="justhodl-best-setups")["Code"]["Location"]
        import io, zipfile
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        mk = {m: (m.encode() in blob) for m in ("spx_regime", "kr_flash_yoy",
                                                "asia_flash_tailwind", "industry_boom")}
        r = LAM.invoke(FunctionName="justhodl-best-setups",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/best-setups.json")["Body"].read())
        ibm = ((j.get("industry_context") or {}).get("industry_boom") or {})
        n_boom = sum(1 for s0 in (j.get("setups") or j.get("top_setups") or
                                  j.get("all") or []) if s0.get("industry_boom"))
        rows = []
        for key in ("setups", "top_setups", "all", "structural_at_trough"):
            if isinstance(j.get(key), list):
                rows = j[key]; break
        n_boom = sum(1 for s0 in rows if isinstance(s0, dict) and s0.get("industry_boom"))
        n_tail = sum(1 for s0 in rows if isinstance(s0, dict) and s0.get("asia_flash_tailwind"))
        ok1 = all(mk.values()) and not err and (ibm.get("joined", 0) >= 5 or n_boom >= 5)
        gate("G1_shared_and_boom", ok1,
             f"zip_markers={mk} err={err} boom_meta={ibm} rows_key_scanned={len(rows)} "
             f"rows_boom={n_boom} rows_tailwind={n_tail}")
    except Exception as e:
        gate("G1_shared_and_boom", False, str(e)[:360])

    # [B] sentinel
    try:
        dep("justhodl-alert-sentinel", "justhodl-alert-sentinel", 180, 512)
        r = LAM.invoke(FunctionName="justhodl-alert-sentinel",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        diag = (pl.get("diagnostics") if isinstance(pl, dict) else None) or []
        st = json.loads(S3C.get_object(Bucket=B, Key="data/_alerts/last.json")["Body"].read())
        per = (st.get("snap") or {}).get("kr_flash_period")
        buf = st.get("buffer") or []
        kr_line = next((b0.get("line") for b0 in buf if "Korea 1-20" in str(b0.get("line"))), None)
        ok2 = (per == "2026-07-01..20") and not err
        gate("G2_sentinel_kr", ok2,
             f"err={err} diag={str(diag)[:120]} state_period={per} yoy={(st.get('snap') or {}).get('kr_flash_yoy')} "
             f"buffered_line={str(kr_line)[:140]} buf_n={len(buf)}")
    except Exception as e:
        gate("G2_sentinel_kr", False, str(e)[:360])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3622.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
