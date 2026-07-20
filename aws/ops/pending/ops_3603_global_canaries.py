"""ops 3603 — Yahoo daily-depth fix (period1 epochs; range=max was serving
MONTHLY bars → 208% fake KOSPI vol, caught by 3602 gates) + GLOBAL VOL CANARY
GRID (10 world indices, stress breadth) + deep 'gb' breadth column. Crisis
gates now on REAL daily data: asia-spill 2008 + global breadth 2008/2020."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-fifx-vol-migration"

with report("3603_global_canaries") as rep:
    rep.heading("ops 3603 — global vol canaries + daily-depth fix")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:520]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:480]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    try:
        env = (LAM.get_function_configuration(FunctionName=FN)
               .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-fifx-vol-migration" / "source",
                      env_vars=env, timeout=300, memory=1024,
                      description="Vol migration barometer v1.4: Yahoo daily-depth fix + 10-index global vol canary grid with stress breadth (deep 'gb' history).",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_daily_depth", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol.json")["Body"].read())
            az = (j.get("legs") or {}).get("asia") or {}
            G = (j.get("legs") or {}).get("global") or {}
            gr = G.get("grid") or {}
            ks, hs = az.get("kospi") or {}, az.get("hang_seng") or {}
            m = j.get("migration") or {}
            ok1 = (j.get("version") == "1.4.0"
                   and (ks.get("n_history") or 0) + 20 >= 5000 if False else True)
            ksn = (gr.get("KOSPI") or {}).get("n_px") or 0
            hsn = (gr.get("HangSeng") or {}).get("n_px") or 0
            ok1 = (j.get("version") == "1.4.0" and ksn >= 5000 and hsn >= 8000
                   and isinstance(ks.get("z"), (int, float))
                   and 3 <= (ks.get("realized_20d_pct") or 0) <= 150)
            gate("G1_daily_depth", ok1,
                 f"KOSPI n_px={ksn} rlzd={ks.get('realized_20d_pct')}% z={ks.get('z')} "
                 f"({ks.get('pctile')}p) · HSI n_px={hsn} rlzd={hs.get('realized_20d_pct')}% "
                 f"z={hs.get('z')} · asia_spill={m.get('asia_spill')} state={m.get('asia_state')}")
            nz = [(nm, g.get("z")) for nm, g in gr.items() if isinstance(g.get("z"), (int, float))]
            gate("G2_global_grid", len(nz) >= 7 and isinstance(G.get("breadth_pct"), (int, float)),
                 f"indices_ok={len(nz)}/{len(gr)} breadth={G.get('breadth_pct')}% "
                 f"elevated={G.get('elevated')} leader={G.get('leader')} "
                 f"zs={sorted(nz, key=lambda x: -x[1])[:6]}")
            dj = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol-history.json")["Body"].read())
            R = dj.get("rows") or []
            n_as = sum(1 for r0 in R if isinstance(r0.get("as"), (int, float)))
            n_gb = sum(1 for r0 in R if isinstance(r0.get("gb"), (int, float)))
            first_as = next((r0["d"] for r0 in R if r0.get("as") is not None), None)
            def mx(col, y0, y1):
                vs = [r0[col] for r0 in R if y0 <= r0["d"][:4] <= y1
                      and isinstance(r0.get(col), (int, float))]
                return round(max(vs), 2) if vs else None
            a97, a08, a20 = mx("asp", "1997", "1998"), mx("asp", "2008", "2009"), mx("asp", "2020", "2020")
            g08, g20 = mx("gb", "2008", "2009"), mx("gb", "2020", "2020")
            ok3 = (n_as >= 1400 and (first_as or "9999")[:4] <= "1998"
                   and (a08 or 0) >= 1.0 and (g08 or 0) >= 60 and (g20 or 0) >= 60)
            gate("G3_deep_crisis", ok3,
                 f"as_rows={n_as} gb_rows={n_gb} from {first_as} · asp: 97-98={a97} "
                 f"⭐2008={a08} 2020={a20} · breadth gb: 2008={g08}% 2020={g20}% · "
                 f"today as={R[-1].get('as')} asp={R[-1].get('asp')} gb={R[-1].get('gb')}%")
            out["crisis"] = {"asp": {"97-98": a97, "2008": a08, "2020": a20},
                             "breadth": {"2008": g08, "2020": g20},
                             "today": {k: R[-1].get(k) for k in ("as", "asp", "gb")}}
    except Exception as e:
        gate("G1_daily_depth", False, str(e)[:320])

    ok4 = False; det = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            mk = {k: (k in html) for k in ("GLOBAL VOL CANARIES", "stress breadth",
                                          "22d3ee", "KOSPI")}
            det = str(mk)
            if all(mk.values()) and html.find('id="jh-fifx"') < html.find('id="jh-spx-ma"'):
                ok4 = True; break
        except Exception as e:
            det = str(e)[:120]
        time.sleep(15)
    gate("G4_page_served", ok4, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3603.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
