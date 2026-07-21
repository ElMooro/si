"""ops 3611 — Khalid: scatter was label-soup (11 sectors converge on SPY at
10y) + wants famous investor/institution portfolio styles that keep updating.
[A] scatter v2: class filter chips, sector-collapse marker, hover <title>
tooltips, sparse labels, gridlines, best-RR Sharpe ray. [B] forward-returns:
+7 formula styles (60/40, Permanent, Golden Butterfly, Swensen, Ivy-5,
Bogleheads-3, AW+growth) all auto-recomputed from live ERs, + naive risk
parity (inverse-vol re-weighting), + 13F clones (Bridgewater/Berkshire) that
refresh each filing quarter from data/13f-positions.json."""
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
FN = "justhodl-forward-returns"

with report("3611_scatter_portfolios") as rep:
    rep.heading("ops 3611 — scatter v2 + famous portfolio styles + 13F clones")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:560]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:520]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-forward-returns" / "source",
                      env_vars=env, timeout=max(300, cfg.get("Timeout", 120)),
                      memory=max(768, cfg.get("MemorySize", 512)),
                      description=(cfg.get("Description") or "forward-returns")[:200],
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_portfolios", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/forward-returns.json")["Body"].read())
            P = j.get("portfolios") or {}
            sums = {k: round(sum((v.get("weights") or {}).values()), 3) for k, v in P.items()
                    if v.get("weights")}
            bad = {k: v for k, v in sums.items() if abs(v - 1.0) > 0.02}
            aw = ((P.get("all_weather") or {}).get("weights") or {})
            rp = (P.get("risk_parity") or {}).get("weights") or {}
            ok1 = (len(P) >= 10 and not bad and aw.get("GLD") == 0.075
                   and len(rp) >= 3
                   and all(isinstance((P[k] or {}).get("forward_er_pct"), (int, float))
                           for k in ("sixty_forty", "permanent", "risk_parity") if k in P))
            clones = {k: {"cov": P[k].get("mapped_coverage_pct"),
                          "er": P[k].get("forward_er_pct"),
                          "as_of": P[k].get("as_of"),
                          "n_hold": len(P[k].get("holdings_top10") or [])}
                      for k in P if k.startswith("clone_")}
            gate("G1_portfolios", ok1,
                 f"n={len(P)} keys={sorted(P.keys())[:14]} bad_sums={bad} "
                 f"rp={rp} 60/40 ER={(P.get('sixty_forty') or {}).get('forward_er_pct')} "
                 f"swensen ER={(P.get('swensen') or {}).get('forward_er_pct')} clones={clones}")
            out["portfolios"] = {k: {"er": (P[k] or {}).get("forward_er_pct"),
                                     "tenk": (P[k] or {}).get("ten_k_10yr")} for k in P}
    except Exception as e:
        gate("G1_portfolios", False, str(e)[:320])

    ok2 = False; det = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/compass.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            mk = {"chips": "jh-sc-chip" in html, "sec_collapse": "SECTORS x" in html,
                  "tooltips": html.count("<title>") >= 1 and "hover any bubble" in html,
                  "ray": "best RR ray" in html,
                  "legacy": "by_opportunity_percentile" in html,
                  "port_note": "refreshed each 13F quarter" in html or "recomputed from live forward ERs" in html}
            det = str(mk)
            if all(mk.values()):
                ok2 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    gate("G2_page", ok2, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3611.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
