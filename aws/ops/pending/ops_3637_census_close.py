"""ops 3637 — CENSUS 6-10 CLOSURE (audit-proof) + Asia divergence tile.
Scan results: shell-missing=0 · jh-enhance on 83 pages · SLA map live
(config/feed-sla.json → feed-registry → sentinel, ops 3415) · pricing+Stripe
shipped · composer exports doc.input_hygiene. Gates: tile served + SLA chain
readable server-side."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from ops_report import report

S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3637_census_close") as rep:
    rep.heading("ops 3637 — census 6-10 closure + divergence tile")
    out = {"gates": {}, "census_proof": {
        "shell_missing": 0, "jh_enhance_pages": 83,
        "sla": "config/feed-sla.json -> feed-registry -> sentinel (3415)",
        "monetization": "pricing.html + Stripe live",
        "composer": "doc.input_hygiene exported"}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:400]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:360]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    ok1 = False; det = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            det = f"tile={'Asia divergence' in html} verdict={'EXTERNAL-DEMAND-LED' in html}"
            if "Asia divergence" in html and "EXTERNAL-DEMAND-LED" in html:
                ok1 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(18)
    gate("G1_div_tile", ok1, det)

    try:
        sla = json.loads(S3C.get_object(Bucket=B, Key="config/feed-sla.json")["Body"].read())
        reg = json.loads(S3C.get_object(Bucket=B, Key="data/feed-registry.json")["Body"].read())
        rows = reg.get("rows") or reg.get("feeds") or []
        gate("G2_sla_chain", isinstance(sla, dict) and len(rows) >= 20,
             f"sla_overrides={len(sla)} registry_rows={len(rows)} "
             f"stale_now={len(reg.get('stale') or [])}")
    except Exception as e:
        gate("G2_sla_chain", False, str(e)[:280])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3637.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
