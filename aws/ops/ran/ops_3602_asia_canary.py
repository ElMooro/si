"""ops 3602 — ASIA CANARY leg (KOSPI + Hang Seng realized vol, Yahoo max
history) into the vol-migration barometer: legs.asia + migration.asia_spill/
state, deep-shard 'as'/'asp' columns, ASIA node on the map, dashed cyan ribbon
overlay. Crisis-gated: asia-spill maxima must show 2008 (Khalid's thesis),
1997-98 (HSI era) and 2020."""
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

with report("3602_asia_canary") as rep:
    rep.heading("ops 3602 — Asia canary leg (KOSPI/HSI vol)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    try:
        env = (LAM.get_function_configuration(FunctionName=FN)
               .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-fifx-vol-migration" / "source",
                      env_vars=env, timeout=240, memory=768,
                      description="Vol migration barometer v1.3: + ASIA canary leg (KOSPI/HSI realized vol z, deep history) alongside FI/FX upstream legs.",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_asia_legs", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol.json")["Body"].read())
            az = (j.get("legs") or {}).get("asia") or {}
            ks, hs = az.get("kospi") or {}, az.get("hang_seng") or {}
            m = j.get("migration") or {}
            ok1 = (j.get("version") == "1.3.0"
                   and isinstance(ks.get("z"), (int, float)) and isinstance(hs.get("z"), (int, float))
                   and isinstance(m.get("asia_spill"), (int, float))
                   and m.get("asia_state") in ("ASIA_CALM", "ASIA_ELEVATED", "ASIA_CANARY"))
            gate("G1_asia_legs", ok1,
                 f"KOSPI rlzd={ks.get('realized_20d_pct')}% z={ks.get('z')} ({ks.get('pctile')}p, "
                 f"{ks.get('n_history')} pts) · HSI rlzd={hs.get('realized_20d_pct')}% z={hs.get('z')} "
                 f"({hs.get('n_history')} pts, VHSI={(hs.get('implied_vhsi') or {}).get('level')}) · "
                 f"asia_z={az.get('asia_z')} asia_spill={m.get('asia_spill')} state={m.get('asia_state')}")
            dj = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol-history.json")["Body"].read())
            R = dj.get("rows") or []
            n_as = sum(1 for r0 in R if isinstance(r0.get("as"), (int, float)))
            def asp_max(y0, y1):
                vs = [r0["asp"] for r0 in R if y0 <= r0["d"][:4] <= y1
                      and isinstance(r0.get("asp"), (int, float))]
                return round(max(vs), 2) if vs else None
            a97, a08, a20 = asp_max("1997", "1998"), asp_max("2008", "2009"), asp_max("2020", "2020")
            first_as = next((r0["d"] for r0 in R if r0.get("as") is not None), None)
            ok2 = (n_as >= 1000 and first_as is not None and first_as[:4] <= "1998"
                   and (a08 or 0) >= 1.2 and (a20 or 0) >= 0.8)
            gate("G2_deep_crisis", ok2,
                 f"asia rows={n_as}/{len(R)} from {first_as} · asp maxima: 97-98={a97} "
                 f"⭐2008-09={a08} 2020={a20} · today asp={R[-1].get('asp') if R else None}")
            out["crisis_asia"] = {"1997-98": a97, "2008-09": a08, "2020": a20,
                                  "today": R[-1].get("asp") if R else None}
    except Exception as e:
        gate("G1_asia_legs", False, str(e)[:320])

    ok3 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if all(k in html for k in ("'ASIA'", "22d3ee", "ASIA canary", "KOSPI")) \
               and html.find('id="jh-fifx"') < html.find('id="jh-spx-ma"'):
                ok3 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G3_page_served", ok3, "served: ASIA node + cyan canary overlay + state chip, card still top")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3602.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
