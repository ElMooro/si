"""ops 3604 — v1.4.1 date-tolerant Asia join (KOSPI KST close vs US date grid;
today's deep row must now carry the KOSPI z) + served-page forensic for the
missing 22d3ee cyan marker (print exact context or absence proof)."""
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

with report("3604_join_fix") as rep:
    rep.heading("ops 3604 — Asia date-tolerant join + page forensic")
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
                      description="Vol migration barometer v1.4.1: date-tolerant Asia joins (KST close vs US grid).",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        json.loads(r["Payload"].read() or b"{}")
        j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol.json")["Body"].read())
        dj = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol-history.json")["Body"].read())
        R = dj.get("rows") or []
        ks_z = ((j.get("legs") or {}).get("asia") or {}).get("kospi", {}).get("z")
        G = (j.get("legs") or {}).get("global") or {}
        last = R[-1] if R else {}
        ok1 = (j.get("version") == "1.4.1" and isinstance(last.get("as"), (int, float))
               and isinstance(ks_z, (int, float))
               and abs(last["as"] - max(ks_z, ((j["legs"]["asia"].get("hang_seng") or {}).get("z") or -9))) <= 0.4
               and isinstance(last.get("gb"), (int, float))
               and abs(last["gb"] - (G.get("breadth_pct") or 0)) <= 15)
        gate("G1_join_fixed", ok1,
             f"today deep as={last.get('as')} asp={last.get('asp')} gb={last.get('gb')}% "
             f"vs main kospi_z={ks_z} breadth={G.get('breadth_pct')}% state={(j.get('migration') or {}).get('asia_state')}")
    except Exception as e:
        gate("G1_join_fixed", False, str(e)[:320])

    # G2 forensic: served page 22d3ee
    ok2 = False; det = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            i = html.find("22d3ee")
            det = (f"len={len(html)} idx={i} ctx='{html[max(0,i-40):i+50]}'" if i >= 0
                   else f"ABSENT · len={len(html)} has_grid={'GLOBAL VOL CANARIES' in html} "
                        f"has_asia_node={'ASIA' in html} n_scripts={html.count('<script')}")
            if i >= 0:
                ok2 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    gate("G2_page_cyan", ok2, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3604.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
