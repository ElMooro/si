"""ops 3525 — stress-index phantom fixed + build-fact probes.

The composer's JSI read pointed at data/stress-index.json — a key with
NO writer anywhere in the fleet (repo-grep proven). Real feed:
data/jsi.json from justhodl-stress-index, whose expanding_pctile the
composer's tolerant walker matches on contact. One-line rewire; the
gross throttle goes live for the first time.

Probes for 3526: spx-history-deep doc shape (writerless static — the
refresher must match consumers), macro-leads + cds-proxy depth-2 keys
(metrics-mode enhance configs).

  Z1 composer live: hygiene missing == [], jsi_src != default/self,
     jsi_pctile numeric, book intact
  Z2 spx-history-deep: top keys + row sample + n + last date + size
  Z3 macro-leads + cds-proxy depth-2 key maps
"""
import json, sys, time
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")

with report("3525_jsi_probes") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3525 — JSI rewire + probes")
    cfg = lam.get_function_configuration(FunctionName="justhodl-proven-portfolio")
    deploy_lambda(report=rep, function_name="justhodl-proven-portfolio",
                  source_dir=REPO/"aws"/"lambdas"/"justhodl-proven-portfolio"/"source",
                  env_vars=(cfg.get("Environment") or {}).get("Variables") or {},
                  timeout=cfg["Timeout"], memory=cfg["MemorySize"],
                  description="composer v1.2.4 jsi.json rewire (ops 3525)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName="justhodl-proven-portfolio")
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName="justhodl-proven-portfolio", Payload=b"{}")
    time.sleep(3)
    doc = json.loads(s3c.get_object(Bucket=BUCKET,
                     Key="data/proven-portfolio.json")["Body"].read())
    hyg = doc.get("input_hygiene") or {}
    missing = [k for k, v in (hyg.get("feeds") or {}).items()
               if not v.get("present")]
    reg = doc.get("regime") or {}
    gate("Z1_jsi_live", missing == [] and reg.get("jsi_pctile") is not None
         and reg.get("jsi_src") not in (None, "default")
         and len(doc.get("book") or []) >= 20,
         {"jsi_pctile": reg.get("jsi_pctile"), "jsi_src": reg.get("jsi_src"),
          "gross_scale": reg.get("gross_scale"), "missing": missing,
          "n_book": len(doc.get("book") or [])})

    try:
        o = s3c.get_object(Bucket=BUCKET, Key="data/spx-history-deep.json")
        body = o["Body"].read()
        d = json.loads(body)
        top = sorted(d.keys()) if isinstance(d, dict) else type(d).__name__
        rows = (d.get("rows") or d.get("history") or d.get("data") or
                d.get("series") or (d if isinstance(d, list) else []))
        gate("Z2_spx_shape", True,
             {"kb": len(body)//1024, "lastmod": str(o["LastModified"]),
              "top": top, "n_rows": len(rows) if isinstance(rows, list) else None,
              "row0": json.dumps(rows[0])[:160] if rows else None,
              "row_last": json.dumps(rows[-1])[:160] if rows else None})
    except Exception as e:
        gate("Z2_spx_shape", False, str(e)[:250])

    for name, key in (("Z3_macro", "data/macro-leads.json"),
                      ("Z3_cds", "data/cds-proxy.json")):
        try:
            d = json.loads(s3c.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            m2 = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    m2[k] = {k2: (type(v2).__name__ +
                                  (f"[{len(v2)}]" if isinstance(v2, (list, dict)) else
                                   f"={v2}" if isinstance(v2, (int, float)) else ""))
                             for k2, v2 in list(v.items())[:8]}
            gate(name, True, json.dumps(m2)[:620])
        except Exception as e:
            gate(name, False, str(e)[:200])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3525.json").write_text(json.dumps({"ops":3525,"fails":fails}))
sys.exit(0)
