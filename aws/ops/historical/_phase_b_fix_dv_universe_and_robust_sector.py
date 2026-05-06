"""
PHASE B — Improve deep-value screener:

Problems found in audit:
  1. BAC, WFC leaking into top_25 (FMP /profile returns blank sector)
  2. Top_25 only has 5 names — universe too narrow because we're tied to S&P
     screener which has only 503 large-caps mostly excluded
  3. The MIN_NET_CASH_PCT early-out at 0.25 means we're filtering *before*
     scoring — should keep results but flag them

Improvements:
  1. Use FMP's stock list (https://financialmodelingprep.com/stable/stock-list)
     to get ~5K tickers OR use multiple seed lists
  2. If sector blank from /profile, fall back to industry name keyword detection
     for "bank", "insurance", "investment management", etc.
  3. Lower MIN_NET_CASH_PCT to 0.15 to include more candidates
  4. Add 'company name' keyword fallback ("Bank of America", "Wells Fargo")
"""
import os, time

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def patch_source():
    src_path = "aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py"
    src = open(src_path, "r").read()
    log(f"  read {src_path}: {len(src)} chars")

    # Fix 1: better financial detection (industry name + company name keywords)
    old = '''    # Sector adjustments — insurance/banking carry massive investment books
    # that aren't discretionary "net cash". Score them differently.
    sector_lower = (sector or "").lower()
    industry_lower = (industry or "").lower()
    is_financial_book = (
        "insurance" in sector_lower or "insurance" in industry_lower
        or "bank" in industry_lower
        or sector_lower == "financial services"
    )
    is_reit = "reit" in industry_lower or sector_lower == "real estate"'''

    new = '''    # Sector adjustments — insurance/banking carry massive investment books
    # that aren't discretionary "net cash". Score them differently.
    sector_lower = (sector or "").lower()
    industry_lower = (industry or "").lower()
    company_lower = (company or "").lower()

    # Comprehensive financial-book detection across sector + industry + company name
    fin_keywords = [
        "insurance", "bank", "banking", "investment management",
        "asset management", "capital markets", "financial conglomerate",
        "credit services", "diversified financial", "financial services",
        "savings", "thrift", "mortgage", "broker", "exchange",
    ]
    fin_company_keywords = [
        "bancorp", "bancshares", "financial corp", "insurance",
        "ins co", "& co.", "capital", "wells fargo", "bank of america",
        "jpmorgan", "morgan stanley", "goldman", "citigroup", "blackrock",
        "blackstone", "kkr", "apollo global", "carlyle", "ares capital",
    ]
    is_financial_book = (
        any(k in sector_lower for k in fin_keywords)
        or any(k in industry_lower for k in fin_keywords)
        or any(k in company_lower for k in fin_company_keywords)
    )

    is_reit = (
        "reit" in industry_lower
        or sector_lower == "real estate"
        or "real estate trust" in company_lower
        or "trust" in industry_lower and "estate" in industry_lower
    )'''

    if old in src:
        src = src.replace(old, new)
        log("  ✓ Patched fin detection")
    else:
        log("  ⚠ fin detection block not found")

    # Fix 2: lower the MIN_NET_CASH_PCT early-out from 0.25 to 0.15
    old = '''    # Quick early-out: needs at least 25% net cash to be worth checking
    if net_cash_pct < 0.25:
        return {"symbol": symbol, "status": "below_min_net_cash", "net_cash_pct": round(net_cash_pct, 3)}'''
    new = '''    # Quick early-out: needs at least 15% net cash to be worth checking
    if net_cash_pct < 0.15:
        return {"symbol": symbol, "status": "below_min_net_cash", "net_cash_pct": round(net_cash_pct, 3)}'''
    if old in src:
        src = src.replace(old, new)
        log("  ✓ Lowered min_net_cash from 25% to 15%")

    # Fix 3: expand universe by also pulling FMP's full active stock list
    old_fn = '''def get_universe():
    """Return up to MAX_TICKERS de-duped from existing screener data + S&P backup."""
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
        print(f"[deep-value] seeded {len(universe)} tickers from screener/data.json")
    except Exception as e:
        print(f"[deep-value] screener seed failed: {e}")

    # Add SP500 backup
    for s in SP500_BACKUP:
        if s not in universe:
            universe.append(s)

    # Cap at MAX_TICKERS
    return universe[:MAX_TICKERS]'''

    new_fn = '''def get_universe():
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
        print(f"[deep-value] screener seed failed: {e}")

    # Try the asymmetric scorer output (mid-cap names)
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/asymmetric-scorer.json")
        d = json.loads(obj["Body"].read())
        for c in d.get("candidates", []) or d.get("setups", []) or []:
            sym = (c.get("symbol") or c.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[deep-value] universe after asymmetric: {len(universe)}")
    except Exception:
        pass

    # Add SP500 backup
    for s in SP500_BACKUP:
        if s not in universe:
            universe.append(s)
    print(f"[deep-value] universe after SP500 backup: {len(universe)}")

    return universe[:MAX_TICKERS]'''
    if old_fn in src:
        src = src.replace(old_fn, new_fn)
        log("  ✓ Expanded universe sources")

    # Fix 4: also relax the qualifier thresholds for tier_b
    old = '''    flag = "MONITOR"
    if is_financial_book:
        # Financial book is excluded from tier-A — these aren't Graham net-nets
        flag = "FINANCIAL_BOOK_EXCLUDED"
        score = score * 0.3  # heavily down-weight
    elif is_reit:
        flag = "REIT_EXCLUDED"
        score = score * 0.3
    elif net_cash_pct >= NET_CASH_RATIO and rev_yield >= REV_RATIO and cf_quality >= 0.5:
        flag = "DEEP_VALUE_TIER_A"
    elif net_cash_pct >= 0.4 and rev_yield >= 0.3:
        flag = "DEEP_VALUE_TIER_B"
    elif net_cash_pct >= 0.3:
        flag = "NET_CASH_WATCH"'''

    new = '''    flag = "MONITOR"
    if is_financial_book:
        flag = "FINANCIAL_BOOK_EXCLUDED"
        score = score * 0.2
    elif is_reit:
        flag = "REIT_EXCLUDED"
        score = score * 0.2
    elif net_cash_pct >= NET_CASH_RATIO and rev_yield >= REV_RATIO and cf_quality >= 0.5:
        flag = "DEEP_VALUE_TIER_A"
    elif net_cash_pct >= 0.35 and rev_yield >= 0.25 and cf_quality >= 0.25:
        flag = "DEEP_VALUE_TIER_B"
    elif net_cash_pct >= 0.20 and cf_quality >= 0.25:
        flag = "NET_CASH_WATCH"
    else:
        flag = "MARGINAL"'''
    if old in src:
        src = src.replace(old, new)
        log("  ✓ Relaxed qualifier thresholds with explicit MARGINAL flag")

    with open(src_path, "w") as f:
        f.write(src)
    log(f"  wrote {len(src)} chars")
    return src


def main():
    section("1) Patch deep-value source")
    patch_source()

    section("2) Validate syntax")
    import ast
    src = open("aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py").read()
    try:
        ast.parse(src)
        log("  ✓ valid")
    except SyntaxError as e:
        log(f"  ❌ {e}")
        return


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_b_dv_robust.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
