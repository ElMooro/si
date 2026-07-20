"""ops 3576 — enforce deal-scanner v3 runtime config (1024MB / 600s) that the
deploy workflow left at defaults (512/300) on the 89ebb7e push, closing the
single 3575 gap. update_function_configuration with ONLY MemorySize+Timeout —
env untouched by construction. Also asserts universe-builder config sane."""
import json, sys, time
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", "us-east-1")

with report("3576_deal_cfg") as rep:
    rep.heading("ops 3576 — deal-scanner config enforcement (1024/600)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:300]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:280]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    def settle(fn, deadline=240):
        dl = time.time() + deadline
        while time.time() < dl:
            c = LAM.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") == "Successful":
                return c
            time.sleep(6)
        return LAM.get_function_configuration(FunctionName=fn)

    # G1 deal-scanner → 1024 / 600
    try:
        c = settle("justhodl-deal-scanner")
        if c.get("MemorySize") != 1024 or c.get("Timeout") != 600:
            LAM.update_function_configuration(FunctionName="justhodl-deal-scanner",
                                              MemorySize=1024, Timeout=600)
            c = settle("justhodl-deal-scanner")
        env_n = len((c.get("Environment") or {}).get("Variables") or {})
        gate("G1_deal_cfg", c.get("MemorySize") == 1024 and c.get("Timeout") == 600,
             f"mem={c.get('MemorySize')} timeout={c.get('Timeout')} env_vars={env_n} (env preserved)")
    except Exception as e:
        gate("G1_deal_cfg", False, str(e)[:200])

    # G2 universe-builder sane (>=512 / >=300)
    try:
        c = settle("justhodl-universe-builder")
        if (c.get("MemorySize") or 0) < 512 or (c.get("Timeout") or 0) < 300:
            LAM.update_function_configuration(FunctionName="justhodl-universe-builder",
                                              MemorySize=512, Timeout=420)
            c = settle("justhodl-universe-builder")
        gate("G2_universe_cfg", (c.get("MemorySize") or 0) >= 512 and (c.get("Timeout") or 0) >= 300,
             f"mem={c.get('MemorySize')} timeout={c.get('Timeout')}")
    except Exception as e:
        gate("G2_universe_cfg", False, str(e)[:200])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3576.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
