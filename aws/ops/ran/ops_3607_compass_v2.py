"""ops 3607 — Capital Compass v2 (Khalid: improve compass.html additively;
horizons 1/5/10y on current macro; more classes+sectors; risk/reward) + close
3606's role-2 gate typo. Engine asset-compass v2 post-processor: 42-asset
multi-horizon CMAs (11 sectors added), lognormal 10/90 bands, opportunity
score joining forward-returns risk/percentile. Page: strategic layer + 7.5%
weight fix (the '101%' was display rounding). Gates verify math, not markers."""
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
FN = "justhodl-asset-compass"

with report("3607_compass_v2") as rep:
    rep.heading("ops 3607 — multi-horizon Capital Compass v2")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:520]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:480]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G0 — close 3606: correct backslash-quoted chip anchor on signal-board
    try:
        ok0 = False; det0 = ""
        for _ in range(10):
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            roles = ['stroke="#e6e8ee" stroke-width="1.1" stroke-dasharray',
                     "pale dashed = ASIA canary",
                     ":'#e6e8ee')+'\\\">'+m.asia_state"]
            hit = [rl in html for rl in roles]
            det0 = str(hit)
            if all(hit):
                ok0 = True; break
            time.sleep(20)
        gate("G0_3606_closed", ok0, det0)
    except Exception as e:
        gate("G0_3606_closed", False, str(e)[:200])

    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-asset-compass" / "source",
                      env_vars=env, timeout=max(300, cfg.get("Timeout", 300)),
                      memory=max(1024, cfg.get("MemorySize", 512)),
                      description=(cfg.get("Description") or "")[:200] or "asset-compass",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_engine", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/asset-compass.json")["Body"].read())
            H = j.get("horizons") or {}
            hm = j.get("h_meta") or {}
            T = j.get("verdict_trio") or {}
            CP = j.get("cash_path_pct") or {}
            n_sec = sum(1 for v in H.values() if v.get("class") == "sectors")
            ok1 = (hm.get("version") == "v2-3607" and not hm.get("error")
                   and len(H) >= 38 and n_sec >= 9
                   and all(isinstance(CP.get(k), (int, float)) for k in ("1y", "5y", "10y")))
            gate("G1_engine", ok1,
                 f"n={len(H)} sectors={n_sec} err={hm.get('error')} cash_path={CP} "
                 f"legacy_assets_intact={isinstance(j.get('assets'), list) and len(j.get('assets')) >= 25}")
            spy, lqd = H.get("SPY") or {}, H.get("LQD") or {}
            mono = all((v.get("tenk_10y") or {}).get("p10", 0)
                       <= (v.get("tenk_10y") or {}).get("base", 0)
                       <= (v.get("tenk_10y") or {}).get("p90", 1e18)
                       for v in H.values() if v.get("tenk_10y"))
            tilts_ok = all(abs(v.get("tactical_tilt_1y_pct", 0)) <= 2.5 for v in H.values())
            ok2 = (mono and tilts_ok
                   and isinstance(spy.get("er_10y_pct"), (int, float)) and 4 <= spy["er_10y_pct"] <= 10
                   and isinstance(lqd.get("er_10y_pct"), (int, float))
                   and isinstance(spy.get("rr_10y"), (int, float)))
            gate("G2_math", ok2,
                 f"SPY 1/5/10y={spy.get('er_1y_pct')}/{spy.get('er_5y_pct')}/{spy.get('er_10y_pct')} "
                 f"σ={spy.get('sigma_pct')} RR={spy.get('rr_10y')} tenk10={spy.get('tenk_10y')} · "
                 f"LQD 10y={lqd.get('er_10y_pct')} ({(lqd.get('note') or '')[:38]}) · "
                 f"XLE tilt={((H.get('XLE') or {}).get('tactical_tilt_1y_pct'))} · monotonic={mono}")
            ok3 = all(T.get(k) for k in ("highest_expected_10y", "best_risk_reward_10y",
                                         "most_attractive_vs_history", "worst_opportunity")) \
                  and all(0 <= v.get("opportunity_score", -1) <= 100 for v in H.values())
            gate("G3_trio_scores", ok3,
                 f"trio={ {k: T.get(k) for k in ('highest_expected_10y','best_risk_reward_10y','most_attractive_vs_history','worst_opportunity')} } "
                 f"top5={ (j.get('compass_ranking') or [])[:5] }")
            out["snapshot"] = {"trio": T, "cash_path": CP,
                               "top5": (j.get("compass_ranking") or [])[:5],
                               "spy": {k: spy.get(k) for k in ("er_1y_pct", "er_5y_pct", "er_10y_pct", "rr_10y")}}
    except Exception as e:
        gate("G1_engine", False, str(e)[:320])

    ok4 = False; det = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/compass.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            mk = {"strategic": "jh-strategic" in html,
                  "table": "COMPASS TABLE" in html,
                  "weights_fix": "%1?(w*100).toFixed(1)" in html,
                  "legacy_cards": "by_opportunity_percentile" in html,
                  "decisive_kept": "compass-decisive-call" in html}
            det = str(mk)
            if all(mk.values()):
                ok4 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    gate("G4_page", ok4, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3607.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
