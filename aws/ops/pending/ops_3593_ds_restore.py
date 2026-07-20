"""ops 3593 — deal-scanner v3.2.1 display restore: sized-but-not-green deals
(RTX-class) get their FULL cards back (deal $ vs revenue vs market cap), and
every card gains growth-vs-industry-peers context (Industry Boom League rank +
company revenue YoY). Config-heal doctrine applied (workflow stomps 512/300)."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=920, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-deal-scanner"

with report("3593_ds_restore") as rep:
    rep.heading("ops 3593 — deal-scanner sized-cards + industry-peer context")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 let deploy-lambdas land, then heal config (stomp doctrine) + settle
    time.sleep(150)
    try:
        LAM.update_function_configuration(FunctionName=FN, MemorySize=1024, Timeout=900)
        for _ in range(40):
            c = LAM.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") == "Successful" and c["MemorySize"] == 1024 and c["Timeout"] == 900:
                break
            time.sleep(5)
        gate("G1_config_heal", c["MemorySize"] == 1024 and c["Timeout"] == 900,
             f"mem={c['MemorySize']} timeout={c['Timeout']} status={c.get('LastUpdateStatus')}")
    except Exception as e:
        gate("G1_config_heal", False, str(e)[:250])

    # G2 invoke → feed carries restored fields
    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G2_fields_live", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-scanner.json")["Body"].read())
            deals = j.get("deals") or []
            sized_other = [d for d in deals if d.get("deal_value_str") and not d.get("highlight")
                           and d.get("event_type") != "capital_structure"]
            n_ind = sum(1 for d in deals if d.get("industry"))
            n_boom = sum(1 for d in deals if (d.get("industry_boom") or {}).get("score") is not None)
            n_rg = sum(1 for d in deals if isinstance(d.get("rev_growth_pct"), (int, float)))
            samp = (sized_other or deals or [{}])[0]
            gate("G2_fields_live",
                 j.get("version") == "3.2.1" and len(deals) >= 1 and n_ind >= 1
                 and (n_boom >= 1 or n_ind == 0) and n_rg >= 1,
                 f"v{j.get('version')} deals={len(deals)} sized_not_green={len(sized_other)} "
                 f"industry={n_ind} boom_join={n_boom} rev_growth={n_rg} · sample "
                 f"{samp.get('symbol')}: {samp.get('deal_value_str')} · {samp.get('vs_market_cap_pct')}% mcap "
                 f"· {samp.get('materiality_pct')}% rev · {samp.get('industry')} boom "
                 f"{(samp.get('industry_boom') or {}).get('score')} #"
                 f"{(samp.get('industry_boom') or {}).get('rank')} · co rev {samp.get('rev_growth_pct')}% YoY")
            out["sample"] = {k: samp.get(k) for k in ("symbol", "deal_value_str", "vs_market_cap_pct",
                             "materiality_pct", "industry", "industry_boom", "rev_growth_pct", "highlight")}
    except Exception as e:
        gate("G2_fields_live", False, str(e)[:320])

    # G3 page served with restored sections
    ok3 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/deal-scanner.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if "Sized Wins" in html and "industry_boom" in html and "rev_growth_pct" in html:
                ok3 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G3_page_served", ok3, "served: Sized Wins section + industry-peer context renderer")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3593.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
