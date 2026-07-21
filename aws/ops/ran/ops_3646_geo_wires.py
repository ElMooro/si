"""ops 3646 — geo-risk WIRING (all five queued): [A] engine v1.1 gssi_cross
(news-flow vs global-sovereign CDS stress: NEWS_LEADS/PRICED_IN/CONFIRMED/
CALM per country) [B] sentinel geo-escalation buffer lines [C] MI +geo_risk
feed (zip) [D] defcon.html panel (bars+temp+escalation+cross chips)
[E] macro-leads geo row. Gates on data + served pages."""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=600, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3646_geo_wires") as rep:
    rep.heading("ops 3646 — geo-risk five-wire")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    def dep(fn, tmo, mem, desc):
        cfg = LAM.get_function_configuration(FunctionName=fn)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=ROOT / "lambdas" / fn / "source",
                      env_vars=env, timeout=max(tmo, cfg.get("Timeout", 120)),
                      memory=max(mem, cfg.get("MemorySize", 256)),
                      description=desc[:200], create_function_url=False)

    # [A] engine v1.1 + cross
    try:
        dep("justhodl-geopolitical-risk", 600, 1024, "geo-risk v1.1 + gssi_cross")
        r = LAM.invoke(FunctionName="justhodl-geopolitical-risk",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/geopolitical-risk.json")["Body"].read())
        gc = j.get("gssi_cross") or {}
        ok1 = (not err and j.get("version") == "1.1.0"
               and gc.get("mapped_n", 0) >= 8
               and isinstance(gc.get("rows"), list) and len(gc["rows"]) >= 8)
        states = {}
        for r0 in gc.get("rows") or []:
            states[r0["state"]] = states.get(r0["state"], 0) + 1
        gate("G1_cross", ok1,
             f"err={err} v={j.get('version')} mapped={gc.get('mapped_n')} states={states} "
             f"news_leads={[(x['country'], x['gap']) for x in (gc.get('news_leads') or [])[:4]]} "
             f"priced_in={[(x['country'], x['gap']) for x in (gc.get('priced_in') or [])[:3]]} "
             f"cross_err={gc.get('error')}")
        out["cross"] = gc
    except Exception as e:
        gate("G1_cross", False, str(e)[:360])

    # [B] sentinel
    try:
        dep("justhodl-alert-sentinel", 180, 512, "sentinel + geo escalation")
        r = LAM.invoke(FunctionName="justhodl-alert-sentinel",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        st = json.loads(S3C.get_object(Bucket=B, Key="data/_alerts/last.json")["Body"].read())
        snap = st.get("snap") or {}
        gate("G2_sentinel", (not err) and ("geo_escalating" in snap)
             and snap.get("geo_top"),
             f"err={err} geo_top={snap.get('geo_top')} temp={snap.get('geo_temp')} "
             f"escalating={snap.get('geo_escalating')}")
    except Exception as e:
        gate("G2_sentinel", False, str(e)[:340])

    # [C] MI zip marker
    try:
        dep("justhodl-morning-intelligence", 300, 1024, "MI + geo_risk feed")
        loc = LAM.get_function(FunctionName="justhodl-morning-intelligence")["Code"]["Location"]
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        gate("G3_mi", b"geo_risk" in blob and b"geopolitical-risk.json" in blob,
             f"zip geo_risk={b'geo_risk' in blob}")
    except Exception as e:
        gate("G3_mi", False, str(e)[:300])

    # [D+E] served pages
    ok4 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            h1 = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/defcon.html?cb=" + str(int(time.time())),
                headers={"User-Agent": "Mozilla/5.0"}), timeout=30).read().decode("utf-8", "replace")
            h2 = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/macro-leads.html?cb=" + str(int(time.time())),
                headers={"User-Agent": "Mozilla/5.0"}), timeout=30).read().decode("utf-8", "replace")
            mk = {"defcon_panel": "jh-georisk" in h1,
                  "defcon_cross": "news-leads" in h1,
                  "ml_row": "Geo stress" in h2,
                  "ml_fetch": "geopolitical-risk.json" in h2}
            det = str(mk)
            if all(mk.values()):
                ok4 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    gate("G4_pages", ok4, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3646.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
