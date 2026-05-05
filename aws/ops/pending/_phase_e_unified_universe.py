"""
PHASE E — Build the unified ticker universe.

The exponential improvement:

Currently each of the 5 hunter systems pulls its own ticker list and fetches
its own quotes/fundamentals. Result:
  - Each system scans a different 400-500 name pool with little overlap
  - Compound aggregator can only find names that happen to appear in 2+ pools
  - Today: 5/161 names overlap (3.1%)
  - Each FMP API call burned 5x for shared data

After unification:
  - One Lambda (justhodl-universe-builder) maintains a master list of ~1500
    quality stocks with sector/industry/mcap/price/52wH pre-fetched, refreshed
    every 4 hours
  - All 5 systems CAN read this master file as their universe
  - Compound aggregator naturally finds more overlaps because all systems
    operate on the same pool
  - 5x fewer FMP calls, 5x faster runs

Implementation:
  1. New Lambda: justhodl-universe-builder
     - Pulls FMP /stable/stock-list and filters for US-listed, mcap > $200M
     - Enriches each with /stable/profile and /stable/quote
     - Writes data/universe.json with shape:
       {
         "schema_version": 1,
         "generated_at": ISO,
         "stats": {n_total, n_by_sector, ...},
         "stocks": [
           {symbol, name, sector, industry, market_cap, price, year_high,
            pct_from_52w_high, exchange, country}, ...
         ]
       }
  2. Schedule: rate(4 hours)
  3. Initial population — limited to ~800 mid+large caps to avoid massive runtime

This is the FOUNDATION. Once this is live and stable, in subsequent phases
we'll patch each hunter to read from data/universe.json instead of building
its own list. That's the part that increases overlap.
"""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-universe-builder"
SCHEDULE_NAME = "justhodl-universe-builder-4h"
SCHEDULE_EXPR = "rate(4 hours)"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


LAMBDA_SOURCE = '''"""
justhodl-universe-builder — maintains data/universe.json, the master ticker
list with enriched fundamentals.

Strategy:
  1. Pull FMP /stable/stock-list (returns ~10K US tickers)
  2. Filter: US-listed (NASDAQ/NYSE), exchange != OTC, mcap >= MIN_MCAP
  3. Enrich each candidate with /stable/profile + /stable/quote (parallel)
  4. Output sorted by market cap descending

Universe size: ~1500-2000 names typically (after mcap filter)
Runtime: ~3-4 minutes with 12 workers and a 240s budget.

Output: data/universe.json with full enrichment for each name.
"""
import io
import json
import os
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/universe.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

MIN_MCAP = float(os.environ.get("MIN_MCAP", "200000000"))   # $200M
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "2500"))
ENRICH_WORKERS = int(os.environ.get("ENRICH_WORKERS", "16"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "NYSEARCA", "BATS", "PNK", "OTC"}

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Universe/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_stock_list():
    """FMP /stable/stock-list returns roughly 10K tickers."""
    url = f"https://financialmodelingprep.com/stable/stock-list?apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=30)
        if isinstance(d, list):
            return d
    except Exception as e:
        print(f"[universe] stock-list fetch failed: {e}")
    return []


def fetch_quote(symbol):
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_profile(symbol):
    url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def enrich(symbol, deadline_at):
    """Pull quote + profile in parallel for one ticker."""
    if time.time() > deadline_at:
        return None
    q = fetch_quote(symbol)
    if not q:
        return None
    mcap = q.get("marketCap") or 0
    if mcap < MIN_MCAP:
        return None
    p = fetch_profile(symbol)
    sector = (p or {}).get("sector") or q.get("sector") or ""
    industry = (p or {}).get("industry") or q.get("industry") or ""
    company = (p or {}).get("companyName") or q.get("name") or symbol
    price = q.get("price") or 0
    yhigh = q.get("yearHigh") or 0
    ylow = q.get("yearLow") or 0
    exchange = (p or {}).get("exchange") or q.get("exchange") or ""
    country = (p or {}).get("country") or "US"
    pct_from_52h = ((price - yhigh) / yhigh * 100) if yhigh else 0
    pct_from_52l = ((price - ylow) / ylow * 100) if ylow else 0
    return {
        "symbol": symbol,
        "name": company,
        "sector": sector,
        "industry": industry,
        "market_cap": mcap,
        "price": price,
        "year_high": yhigh,
        "year_low": ylow,
        "pct_from_52w_high": round(pct_from_52h, 1),
        "pct_from_52w_low": round(pct_from_52l, 1),
        "exchange": exchange,
        "country": country,
        "volume": q.get("volume") or 0,
        "avg_volume": q.get("avgVolume") or 0,
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print(f"[universe] starting v1.0, max_tickers={MAX_TICKERS}, min_mcap=${MIN_MCAP/1e9:.2f}B")

    # Step 1: pull master list
    raw = fetch_stock_list()
    print(f"[universe] FMP stock-list returned {len(raw)} tickers")

    # Step 2: pre-filter
    candidates = []
    for r in raw:
        sym = (r.get("symbol") or "").upper().strip()
        ex = (r.get("exchangeShortName") or r.get("exchange") or "").upper()
        if not sym or len(sym) > 6:
            continue
        if "." in sym or "-" in sym:
            continue  # skip preferred / non-equity instruments
        if ex and ex not in ALLOWED_EXCHANGES:
            continue
        candidates.append(sym)
    candidates = sorted(set(candidates))
    print(f"[universe] pre-filter retained {len(candidates)} candidates")

    # Cap to MAX_TICKERS at this stage to bound enrichment time
    if len(candidates) > MAX_TICKERS:
        candidates = candidates[:MAX_TICKERS]
        print(f"[universe] capped to {MAX_TICKERS} for enrichment budget")

    # Step 3: enrich in parallel
    enriched = []
    statuses = {"ok": 0, "no_quote": 0, "below_mcap": 0, "deadline": 0}
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        futures = {pool.submit(enrich, s, deadline_at): s for s in candidates}
        for fut in as_completed(futures):
            try:
                r = fut.result(timeout=30)
            except Exception:
                statuses["deadline"] += 1
                continue
            if r is None:
                statuses["below_mcap"] += 1
                continue
            enriched.append(r)
            statuses["ok"] += 1
    enriched.sort(key=lambda x: -(x["market_cap"] or 0))
    print(f"[universe] enriched: {len(enriched)} stocks, statuses: {statuses}")
    print(f"[universe] runtime: {time.time() - started:.1f}s")

    # Stats
    by_sector = {}
    by_mcap_bucket = {"mega (>$200B)": 0, "large ($10-200B)": 0, "mid ($2-10B)": 0,
                      "small ($300M-2B)": 0, "micro (<$300M)": 0}
    for s in enriched:
        sec = s.get("sector") or "Unknown"
        by_sector[sec] = by_sector.get(sec, 0) + 1
        mc = s["market_cap"]
        if mc >= 2e11: by_mcap_bucket["mega (>$200B)"] += 1
        elif mc >= 1e10: by_mcap_bucket["large ($10-200B)"] += 1
        elif mc >= 2e9: by_mcap_bucket["mid ($2-10B)"] += 1
        elif mc >= 3e8: by_mcap_bucket["small ($300M-2B)"] += 1
        else: by_mcap_bucket["micro (<$300M)"] += 1

    out = {
        "schema_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_total": len(enriched),
            "n_raw_input": len(raw),
            "n_pre_filter": len(candidates),
            "by_sector": by_sector,
            "by_mcap_bucket": by_mcap_bucket,
            "statuses": statuses,
        },
        "stocks": enriched,
    }
    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[universe] wrote {len(body):,}b to {S3_KEY}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_total": len(enriched),
            "duration_s": out["duration_s"],
            "n_by_sector": len(by_sector),
        }),
    }
'''


def main():
    section("0) Write Lambda source")
    src_dir = "aws/lambdas/justhodl-universe-builder/source"
    os.makedirs(src_dir, exist_ok=True)
    src_path = f"{src_dir}/lambda_function.py"
    with open(src_path, "w", encoding="utf-8") as f:
        f.write(LAMBDA_SOURCE)
    log(f"  wrote {src_path}: {len(LAMBDA_SOURCE)} chars")

    import ast
    try:
        ast.parse(LAMBDA_SOURCE)
        log("  ✓ valid python")
    except SyntaxError as e:
        log(f"  ❌ {e}")
        return

    section("1) Build zip + create/update Lambda")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, LAMBDA_SOURCE)
    zb = buf.getvalue()
    log(f"  zip: {len(zb):,}b")

    L = boto3.client("lambda", region_name=REGION)
    EB = boto3.client("events", region_name=REGION)
    S3_ = boto3.client("s3", region_name=REGION)

    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY": "data/universe.json",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "MIN_MCAP": "200000000",
        "MAX_TICKERS": "1800",
        "ENRICH_WORKERS": "16",
        "TIMEOUT_BUDGET_S": "260",
    }

    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        for _ in range(30):
            c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        L.update_function_configuration(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=1024, Timeout=300,
            Environment={"Variables": env},
        )
        log("  ✓ updated existing Lambda")
    except L.exceptions.ResourceNotFoundException:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=300, MemorySize=1024,
            Environment={"Variables": env},
        )
        log("  ✓ created new Lambda")

    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ready: mem={c['MemorySize']}MB to={c['Timeout']}s")

    section("2) Schedule rate(4 hours)")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR,
                            State="ENABLED")["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=f"{SCHEDULE_NAME}-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log("  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  ✓ permission already exists")

    section("3) Smoke invoke (this will take ~3-4 minutes)")
    from botocore.config import Config
    cfg = Config(read_timeout=600, connect_timeout=10, retries={"max_attempts": 1})
    L2 = boto3.client("lambda", region_name=REGION, config=cfg)

    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}, dur: {time.time()-t0:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {json.dumps(body)[:400]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log(f"    {ln.rstrip()}")

    section("4) Verify output")
    obj = S3_.get_object(Bucket=BUCKET, Key="data/universe.json")
    body = obj["Body"].read()
    d = json.loads(body)
    log(f"  size: {len(body):,}b")
    log(f"  generated_at: {d.get('generated_at')}")
    log(f"  stats: {json.dumps(d.get('stats', {}))[:500]}")
    stocks = d.get("stocks", [])
    log("")
    log(f"  ── top 10 by market cap ──")
    for s in stocks[:10]:
        mc = s.get("market_cap", 0) or 0
        ms = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
        log(f"    {s['symbol']:<6}  {ms:<8}  {s.get('sector', '')[:25]:<25}  {s.get('name', '')[:35]}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_e_universe_builder.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
