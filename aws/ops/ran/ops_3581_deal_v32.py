"""ops 3581 — deal-scanner v3.2.0 live-proof: options-flow confirmation join
(4 live options feeds wired) + SEC 8-K primary-document terms parser (filed
value vs PR spin). Gates on structure (fields present, page markers) — counts
are tape-dependent. Config heal inlined (workflow stomps to 512/300)."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3581)"}
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-deal-scanner"

with report("3581_deal_v32") as rep:
    rep.heading("ops 3581 — deal-scanner v3.2 (options confirm + filed terms)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:460]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:420]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 settle + markers + config heal (1024/900)
    ok1 = False; dl = time.time() + 660
    while time.time() < dl:
        try:
            cfg = LAM.get_function_configuration(FunctionName=FN)
            if cfg.get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                    src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if all(m in src for m in ('VERSION = "3.2.0"', "def load_options_flow",
                                          "def fetch_8k_terms", '"options_confirm"',
                                          "PR_LARGER")):
                    ok1 = True; break
        except Exception:
            pass
        time.sleep(12)
    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        if cfg.get("MemorySize") != 1024 or cfg.get("Timeout") != 900:
            LAM.update_function_configuration(FunctionName=FN, MemorySize=1024, Timeout=900)
            dl2 = time.time() + 180
            while time.time() < dl2:
                cfg = LAM.get_function_configuration(FunctionName=FN)
                if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("Timeout") == 900:
                    break
                time.sleep(6)
    except Exception:
        pass
    gate("G1_settled_v32", ok1 and cfg.get("Timeout") == 900 and cfg.get("MemorySize") == 1024,
         f"markers ok={ok1} mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')}")

    # G2 fresh feed — structural fields present, base rates persist
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    j = None; dl = time.time() + 560
    while time.time() < dl:
        try:
            cand = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-scanner.json")["Body"].read())
            if cand.get("version") == "3.2.0" and cand.get("generated_at", "") > t0.isoformat()[:19]:
                j = cand; break
        except Exception:
            pass
        time.sleep(15)
    if j:
        dls = j.get("deals") or [{}]
        cv = j.get("coverage") or {}
        struct = all(k in dls[0] for k in ("options_flow", "options_confirm", "filing"))
        gate("G2_feed_v32",
             struct and "n_options_confirmed" in cv and "n_filings_parsed" in cv
             and bool(j.get("base_rates")),
             f"deals={len(dls)} struct_ok={struct} options_confirmed={cv.get('n_options_confirmed')} "
             f"filings_parsed={cv.get('n_filings_parsed')} 8k_3d={cv.get('n_8k_item101_3d')} "
             f"base_rate_types={len(j.get('base_rates') or {})} signals={j['summary'].get('signals_logged')}")
        out["summary"] = {"n_deals": len(dls),
                          "options_confirmed": cv.get("n_options_confirmed"),
                          "filings_parsed": cv.get("n_filings_parsed"),
                          "filing_sample": next((d.get("filing") for d in dls if d.get("filing")), None)}
    else:
        gate("G2_feed_v32", False, "no fresh 3.2.0 feed within window")

    # G3 served page — static markers
    ok3 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/deal-scanner.html", headers=UA), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if all(m in html for m in ("CALL FLOW", "PR &gt; FILING", "TERMS ✓",
                                       "8-K filed", "live options fleet")):
                ok3 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G3_page_v32", ok3, "served: call-flow pill + filing verdict pills + filing context + note")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3581.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
