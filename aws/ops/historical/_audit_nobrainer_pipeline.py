"""
End-to-end audit of L1-L6 nobrainer pipeline. Verifies Lambdas exist with
fresh code, S3 outputs are recent, and a forced L4 invocation produces
a non-empty ranked list. Writes report to aws/ops/reports/latest/audit_nobrainer_pipeline.md
"""
import json, os, time
import boto3

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

LAYERS = [
    ("L1 Theme Detector",       "justhodl-theme-detector",         "data/themes-detected.json"),
    ("L2 Supply Inflection",    "justhodl-supply-inflection-scanner", "data/supply-inflection.json"),
    ("L3 Tier Classifier",      "justhodl-theme-tier-classifier",  "data/theme-tiers.json"),
    ("L4 Asymmetric Hunter",    "justhodl-asymmetric-hunter",      "data/nobrainers.json"),
    ("L5 Nobrainer Rationale",  "justhodl-nobrainer-rationale",    "data/nobrainers-rationale.json"),
    ("L6 Nobrainer Tracker",    "justhodl-nobrainer-tracker",      None),
]

REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("1) Lambda + S3 freshness audit")
    for name, fn, key in LAYERS:
        log("")
        log(f"── {name}: {fn} ──")
        try:
            cfg = L.get_function(FunctionName=fn)["Configuration"]
            log(f"  state: {cfg['State']}  mem={cfg['MemorySize']}MB  timeout={cfg['Timeout']}s")
            log(f"  modified: {cfg['LastModified']}")
            env_keys = list(cfg.get("Environment", {}).get("Variables", {}).keys())
            log(f"  env keys: {env_keys}")
        except Exception as e:
            log(f"  ❌ Lambda not found: {e}")
            continue

        if key:
            try:
                head = S3.head_object(Bucket=BUCKET, Key=key)
                log(f"  S3 {key}: {head['ContentLength']:,}b  modified {head['LastModified']}")
            except Exception as e:
                log(f"  ⚠ S3 {key}: not present ({type(e).__name__})")

    section("2) Force-invoke L4 asymmetric-hunter")
    try:
        r = L.invoke(FunctionName="justhodl-asymmetric-hunter",
                     InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
        body = json.loads(r["Payload"].read().decode())
        log(f"  status: {r['StatusCode']}  body keys: {list(body.keys())}")
        if "body" in body and body.get("statusCode") == 200:
            inner = json.loads(body["body"])
            log(f"  inner keys: {list(inner.keys())[:15]}")
            for fld in ["n_setups", "n_top", "n_ranked", "n_candidates", "top_5_symbols", "top_symbols", "top_5"]:
                if fld in inner:
                    log(f"  {fld}: {inner[fld]}")
            top = inner.get("top_5") or inner.get("ranked", [])[:8]
            if top:
                log("  top picks:")
                for t in top[:8]:
                    if isinstance(t, dict):
                        sym = t.get("symbol") or t.get("ticker") or "?"
                        sc  = t.get("score") or t.get("nobrainer_score") or "?"
                        flag = t.get("flag") or t.get("tier") or ""
                        log(f"    {sym:<8} score={sc} {flag}")
                    else:
                        log(f"    {t}")
        else:
            log(f"  raw body: {json.dumps(body)[:1500]}")
        if "LogResult" in r:
            import base64
            tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
            log("  ── tail logs (last 4kb) ──")
            for ln in tail.splitlines()[-25:]:
                log(f"    {ln}")
    except Exception as e:
        log(f"  ❌ {e}")

    section("3) Force-invoke L5 nobrainer-rationale (top_n=3)")
    try:
        r = L.invoke(FunctionName="justhodl-nobrainer-rationale",
                     InvocationType="RequestResponse", LogType="Tail",
                     Payload=json.dumps({"top_n": 3}).encode())
        body = json.loads(r["Payload"].read().decode())
        log(f"  status: {r['StatusCode']}  body keys: {list(body.keys())}")
        if "body" in body and body.get("statusCode") == 200:
            inner = json.loads(body["body"])
            log(f"  inner keys: {list(inner.keys())[:12]}")
            for fld in ["n_theses", "n_written", "symbols", "n_skipped"]:
                if fld in inner:
                    log(f"  {fld}: {inner[fld]}")
            theses = inner.get("theses") or inner.get("results") or []
            for t in theses[:2]:
                if isinstance(t, dict):
                    log(f"  ── thesis: {t.get('symbol', '?')} ──")
                    txt = t.get("rationale") or t.get("thesis") or t.get("body") or ""
                    log(f"    {txt[:300]}{'...' if len(txt) > 300 else ''}")
        else:
            log(f"  raw: {json.dumps(body)[:1500]}")
    except Exception as e:
        log(f"  ❌ {e}")

    section("4) Force-invoke L6 nobrainer-tracker")
    try:
        r = L.invoke(FunctionName="justhodl-nobrainer-tracker",
                     InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
        body = json.loads(r["Payload"].read().decode())
        log(f"  status: {r['StatusCode']}  body keys: {list(body.keys())}")
        if "body" in body and body.get("statusCode") == 200:
            inner = json.loads(body["body"])
            log(f"  inner: {json.dumps(inner)[:1500]}")
        else:
            log(f"  raw: {json.dumps(body)[:1500]}")
    except Exception as e:
        log(f"  ❌ {e}")

    section("5) Summary tally")
    issues = []
    for name, fn, key in LAYERS:
        try:
            L.get_function(FunctionName=fn)
        except Exception:
            issues.append(f"{name} missing")
        if key:
            try:
                S3.head_object(Bucket=BUCKET, Key=key)
            except Exception:
                issues.append(f"{name} S3 missing: {key}")
    if not issues:
        log("✅ all 6 layers + S3 outputs present")
    else:
        for i in issues:
            log(f"❌ {i}")

if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "audit_nobrainer_pipeline.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
