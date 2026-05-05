"""
PHASE F — Patch each hunter to use the unified universe as primary seed.

This is the multiplier. Each hunter currently has its own `get_universe()`
function pulling its own list. We replace those with reads from
`data/universe.json`, the master pool maintained by universe-builder.

After this:
  - Same 336+ tickers pass through all 5 hunter filters
  - Compound aggregator naturally finds overlaps
  - Coverage is consistent across systems

Hunters to patch:
  - justhodl-deep-value-screener (currently uses screener/data.json + SP500)
  - justhodl-eps-revision-velocity (currently uses screener/data.json + SP500)
  - justhodl-asymmetric-hunter (currently uses theme-tier output, will leave alone)
  - justhodl-insider-cluster-scanner (uses SEC daily index, doesn't need universe)
  - justhodl-smart-money-cluster (uses 13F-positions.json — already has its universe)

So practical patches needed: deep-value + eps-velocity. Both should read
data/universe.json first, fall back to existing logic.
"""
import io, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def patch_dv():
    """Insert universe-first loading in deep-value get_universe()."""
    src_path = "aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py"
    src = open(src_path).read()

    if "data/universe.json" in src:
        log("  ⚠ DV already references universe.json — skipping")
        return False

    old = '''def get_universe():
    """Return up to MAX_TICKERS de-duped from existing screener data + S&P backup + FMP active list."""
    universe = []

    # First try the existing screener output
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("rows") or d.get("stocks") or d.get("data") or []
        for r in rows:
            sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[deep-value] seeded {len(universe)} from screener/data.json")
    except Exception as e:
        print(f"[deep-value] screener seed failed: {e}")'''

    new = '''def get_universe():
    """Return up to MAX_TICKERS, prioritizing the unified universe (data/universe.json)."""
    universe = []

    # PRIMARY: pull from unified universe (master pool)
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        ud = json.loads(obj["Body"].read())
        for s in ud.get("stocks", []):
            sym = (s.get("symbol") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[deep-value] seeded {len(universe)} from data/universe.json (unified)")
    except Exception as e:
        print(f"[deep-value] unified universe failed, falling back: {e}")

    # FALLBACK: existing screener output
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("rows") or d.get("stocks") or d.get("data") or []
        for r in rows:
            sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[deep-value] universe after screener fallback: {len(universe)}")
    except Exception as e:
        print(f"[deep-value] screener seed failed: {e}")'''

    if old not in src:
        log("  ⚠ DV original get_universe block not found — skipping")
        return False

    src = src.replace(old, new)
    with open(src_path, "w") as f:
        f.write(src)
    log(f"  ✓ DV patched (size: {len(src)} chars)")
    return True


def patch_eps():
    """Insert universe-first loading in eps-velocity get_universe()."""
    src_path = "aws/lambdas/justhodl-eps-revision-velocity/source/lambda_function.py"
    src = open(src_path).read()

    if "data/universe.json" in src:
        log("  ⚠ EPS already references universe.json — skipping")
        return False

    old = '''def get_universe():
    universe = []
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("rows") or d.get("stocks") or d.get("data") or []
        for r in rows:
            sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[eps-velocity] seeded {len(universe)} from screener")
    except Exception as e:
        print(f"[eps-velocity] screener seed failed: {e}")
    for s in SP500_BACKUP:
        if s not in universe:
            universe.append(s)
    return universe[:MAX_TICKERS]'''

    new = '''def get_universe():
    universe = []
    # PRIMARY: unified universe
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        ud = json.loads(obj["Body"].read())
        for s in ud.get("stocks", []):
            sym = (s.get("symbol") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[eps-velocity] seeded {len(universe)} from data/universe.json (unified)")
    except Exception as e:
        print(f"[eps-velocity] unified universe failed, falling back: {e}")
    # FALLBACK: screener
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("rows") or d.get("stocks") or d.get("data") or []
        for r in rows:
            sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[eps-velocity] universe after screener fallback: {len(universe)}")
    except Exception as e:
        print(f"[eps-velocity] screener seed failed: {e}")
    # FALLBACK: SP500 backup
    for s in SP500_BACKUP:
        if s not in universe:
            universe.append(s)
    return universe[:MAX_TICKERS]'''

    if old not in src:
        log("  ⚠ EPS original get_universe block not found — skipping")
        return False

    src = src.replace(old, new)
    with open(src_path, "w") as f:
        f.write(src)
    log(f"  ✓ EPS patched (size: {len(src)} chars)")
    return True


def deploy(fn_name, src_path):
    src = open(src_path).read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName=fn_name, ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=fn_name)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    return c


def invoke(fn_name):
    t0 = time.time()
    r = L.invoke(FunctionName=fn_name, InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    import json
    body = json.loads(r["Payload"].read())
    log(f"  {fn_name}: status={r['StatusCode']} dur={time.time()-t0:.1f}s body={body.get('body','')[:300]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-8:]:
            log(f"    {ln.rstrip()}")


def main():
    section("1) Patch deep-value source")
    patch_dv()
    import ast
    try:
        ast.parse(open("aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py").read())
        log("  ✓ DV syntax valid")
    except SyntaxError as e:
        log(f"  ❌ DV syntax: {e}")
        return

    section("2) Patch eps-velocity source")
    patch_eps()
    try:
        ast.parse(open("aws/lambdas/justhodl-eps-revision-velocity/source/lambda_function.py").read())
        log("  ✓ EPS syntax valid")
    except SyntaxError as e:
        log(f"  ❌ EPS syntax: {e}")
        return

    section("3) Deploy both")
    c = deploy("justhodl-deep-value-screener",
                "aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py")
    log(f"  ✓ DV deployed at {c['LastModified']}")
    c = deploy("justhodl-eps-revision-velocity",
                "aws/lambdas/justhodl-eps-revision-velocity/source/lambda_function.py")
    log(f"  ✓ EPS deployed at {c['LastModified']}")

    section("4) Invoke both — verify universe.json is being used")
    invoke("justhodl-deep-value-screener")
    invoke("justhodl-eps-revision-velocity")

    section("5) Re-run compound aggregator with new outputs")
    invoke("justhodl-compound-aggregator")

    section("6) Read final compound state")
    import json
    s3 = boto3.client("s3", region_name=REGION)
    d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/compound-signals.json")["Body"].read())
    log(f"  feed_stats: {json.dumps(d.get('feed_stats', {}))}")
    log(f"  stats: {json.dumps(d.get('stats', {}))}")
    log("")
    log("  ── compound leaderboard ──")
    for r in d.get("compound", [])[:15]:
        sys_str = ", ".join(r.get("systems", []))
        log(f"    {r['symbol']:<6}  #sys={r['n_systems']}  comp={r['compound_score']:>7.1f}  ({sys_str})")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_f_patch_hunters.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
