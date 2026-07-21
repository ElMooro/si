"""ops 3612 — 3611's n=0 was a GATE-KEY error: the feed key is
'benchmark_portfolios' (line 633), not 'portfolios'. Re-gate on the right key,
clones now use an inlined S3 reader (get_s3_json never existed here), page
marker checks the template source ('p.updates?')."""
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

with report("3612_bench_key") as rep:
    rep.heading("ops 3612 — benchmark_portfolios gate + clone reader fix")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:600]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]); rep.log(n + " " + str(ok))
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
            gate("G1_bench", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/forward-returns.json")["Body"].read())
            P = j.get("benchmark_portfolios") or {}
            sums = {k: round(sum((v.get("weights") or {}).values()), 3)
                    for k, v in P.items() if v.get("weights")}
            bad = {k: v for k, v in sums.items() if abs(v - 1.0) > 0.02}
            aw = ((P.get("all_weather") or {}).get("weights") or {})
            rp = (P.get("risk_parity") or {}).get("weights") or {}
            clones = {k: {"cov": P[k].get("mapped_coverage_pct"),
                          "er": P[k].get("forward_er_pct"),
                          "as_of": P[k].get("as_of"),
                          "n_hold": len(P[k].get("holdings_top10") or [])}
                      for k in P if k.startswith("clone_")}
            styles_ok = all(isinstance((P.get(k) or {}).get("forward_er_pct"), (int, float))
                            for k in ("sixty_forty", "permanent", "golden_butterfly",
                                      "swensen", "ivy5", "bogle3", "risk_parity"))
            ok1 = len(P) >= 10 and not bad and aw.get("GLD") == 0.075 and styles_ok
            gate("G1_bench", ok1,
                 f"n={len(P)} bad={bad} rp={rp} ERs: 60/40={(P.get('sixty_forty') or {}).get('forward_er_pct')} "
                 f"perm={(P.get('permanent') or {}).get('forward_er_pct')} GB={(P.get('golden_butterfly') or {}).get('forward_er_pct')} "
                 f"swensen={(P.get('swensen') or {}).get('forward_er_pct')} ivy5={(P.get('ivy5') or {}).get('forward_er_pct')} "
                 f"bogle3={(P.get('bogle3') or {}).get('forward_er_pct')} rpER={(P.get('risk_parity') or {}).get('forward_er_pct')} "
                 f"clones={clones}")
            out["bench"] = {k: {"er": (P[k] or {}).get("forward_er_pct"),
                                "tenk": (P[k] or {}).get("ten_k_10yr")} for k in P}
    except Exception as e:
        gate("G1_bench", False, str(e)[:320])

    ok2 = False; det = ""; dl = time.time() + 360
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/compass.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            mk = {"chips": "jh-sc-chip" in html, "sec": "SECTORS x" in html,
                  "ray": "best RR ray" in html, "note_tpl": "p.updates?" in html,
                  "hold_tpl": "p.holdings_top10" in html,
                  "legacy": "by_opportunity_percentile" in html}
            det = str(mk)
            if all(mk.values()):
                ok2 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(18)
    gate("G2_page", ok2, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3612.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
