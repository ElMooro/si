"""
Patch tier-classifier (Layer 3) to use FMP's new /stable/ API.

Migration:
  • Base URL: /api/v3 → /stable
  • Path style: /profile/AAPL → /profile?symbol=AAPL
  • Field renames:
      mktCap → marketCap
      enterpriseValueOverEBITDATTM → evToEBITDATTM
      peRatioTTM → priceToEarningsRatioTTM
      priceToSalesRatioTTM (unchanged)
      freeCashFlowYieldTTM (unchanged)
      grossProfitMarginTTM (unchanged, in ratios-ttm now)

Steps:
  1. Patch lambda_function.py in-place
  2. Redeploy
  3. Smoke-invoke
  4. Verify S3 output has fundamentals + asymmetric leaderboard
"""
import io
import json
import time
import zipfile
import os

import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-theme-tier-classifier"
SOURCE_FILE = "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py"

REPORT = []


def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")


def section(t):
    print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 1: change FMP_BASE
# ─────────────────────────────────────────────────────────────────────────────
PATCH_BASE_OLD = 'FMP_BASE = "https://financialmodelingprep.com/api/v3"'
PATCH_BASE_NEW = 'FMP_BASE = "https://financialmodelingprep.com/stable"'

# ─────────────────────────────────────────────────────────────────────────────
# PATCH 2: change fmp_get to use ?symbol=X format
# ─────────────────────────────────────────────────────────────────────────────
# The old fmp_get builds: /path?apikey=K&extra=v
# We need: /path?symbol=X&apikey=K   (when ticker is in the path)
# But path-style /profile/AAPL is gone.
# Easier: change fetch_fundamentals to construct URLs with ?symbol=X.

PATCH_FETCH_OLD = '''def fetch_fundamentals(ticker):
    """
    Pull profile + key-metrics-ttm from FMP. Returns dict or None.
    """
    with _CACHE_LOCK:
        if ticker in _FUNDAMENTAL_CACHE:
            return _FUNDAMENTAL_CACHE[ticker]

    profile = fmp_get(f"/profile/{ticker}")
    if not profile or not isinstance(profile, list) or not profile:
        result = None
    else:
        p = profile[0]
        # key-metrics-ttm has the trailing-twelve-month ratios
        ktm = fmp_get(f"/key-metrics-ttm/{ticker}", params={"limit": "1"})
        kt = ktm[0] if (ktm and isinstance(ktm, list) and ktm) else {}

        # ratios-ttm has additional ratios
        rtm = fmp_get(f"/ratios-ttm/{ticker}")
        rt = rtm[0] if (rtm and isinstance(rtm, list) and rtm) else {}

        market_cap = p.get("mktCap")
        revenue_ttm = kt.get("revenuePerShareTTM")
        shares = (market_cap / p.get("price")) if (market_cap and p.get("price")) else None
        if revenue_ttm is not None and shares:
            revenue_ttm_total = revenue_ttm * shares
        else:
            revenue_ttm_total = None

        result = {
            "ticker": ticker,
            "name": p.get("companyName"),
            "sector": p.get("sector"),
            "industry": p.get("industry"),
            "exchange": p.get("exchangeShortName"),
            "country": p.get("country"),
            "currency": p.get("currency"),
            "price": p.get("price"),
            "market_cap": market_cap,
            "shares_outstanding": shares,
            "revenue_ttm": revenue_ttm_total,
            "p_s": kt.get("priceToSalesRatioTTM") or rt.get("priceToSalesRatioTTM"),
            "p_e": kt.get("peRatioTTM") or rt.get("peRatioTTM"),
            "ev_ebitda": kt.get("enterpriseValueOverEBITDATTM"),
            "fcf_yield": kt.get("freeCashFlowYieldTTM"),
            "fcf_per_share": kt.get("freeCashFlowPerShareTTM"),
            "debt_to_equity": kt.get("debtToEquityTTM"),
            "roic": kt.get("roicTTM"),
            "gross_margin": rt.get("grossProfitMarginTTM"),
            "operating_margin": rt.get("operatingProfitMarginTTM"),
            "net_margin": rt.get("netProfitMarginTTM"),
            "rev_growth_ttm": rt.get("revenueGrowthTTM"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        # Derived: market_cap / revenue (asymmetry detector)
        if result["market_cap"] and result["revenue_ttm"]:
            result["mcap_to_rev"] = round(result["market_cap"] / result["revenue_ttm"], 3)
        else:
            result["mcap_to_rev"] = None

    with _CACHE_LOCK:
        _FUNDAMENTAL_CACHE[ticker] = result
    return result'''

PATCH_FETCH_NEW = '''def fetch_fundamentals(ticker):
    """
    Pull profile + ratios-ttm + key-metrics-ttm from FMP /stable. Returns dict or None.

    FMP migrated v3 → stable endpoints in Aug 2025. New structure:
      • /stable/profile?symbol=AAPL  (instead of /v3/profile/AAPL)
      • field "marketCap" (instead of "mktCap")
      • field "evToEBITDATTM" (instead of "enterpriseValueOverEBITDATTM")
      • field "priceToEarningsRatioTTM" (instead of "peRatioTTM")
    """
    with _CACHE_LOCK:
        if ticker in _FUNDAMENTAL_CACHE:
            return _FUNDAMENTAL_CACHE[ticker]

    profile = fmp_get("/profile", params={"symbol": ticker})
    if not profile or not isinstance(profile, list) or not profile:
        result = None
    else:
        p = profile[0]
        rtm = fmp_get("/ratios-ttm", params={"symbol": ticker})
        rt = rtm[0] if (rtm and isinstance(rtm, list) and rtm) else {}
        ktm = fmp_get("/key-metrics-ttm", params={"symbol": ticker})
        kt = ktm[0] if (ktm and isinstance(ktm, list) and ktm) else {}

        market_cap = p.get("marketCap") or kt.get("marketCap")
        price = p.get("price")
        rev_per_share = rt.get("revenuePerShareTTM")
        shares = (market_cap / price) if (market_cap and price and price > 0) else None
        if rev_per_share is not None and shares:
            revenue_ttm_total = rev_per_share * shares
        else:
            revenue_ttm_total = None

        result = {
            "ticker": ticker,
            "name": p.get("companyName"),
            "sector": p.get("sector"),
            "industry": p.get("industry"),
            "exchange": p.get("exchange"),
            "country": p.get("country"),
            "currency": p.get("currency"),
            "price": price,
            "market_cap": market_cap,
            "shares_outstanding": shares,
            "revenue_ttm": revenue_ttm_total,
            "p_s": rt.get("priceToSalesRatioTTM"),
            "p_e": rt.get("priceToEarningsRatioTTM"),
            "ev_ebitda": kt.get("evToEBITDATTM") or rt.get("enterpriseValueMultipleTTM"),
            "ev_sales": kt.get("evToSalesTTM"),
            "fcf_yield": kt.get("freeCashFlowYieldTTM"),
            "fcf_per_share": rt.get("freeCashFlowPerShareTTM"),
            "debt_to_equity": rt.get("debtToEquityRatioTTM"),
            "roic": kt.get("returnOnInvestedCapitalTTM"),
            "gross_margin": rt.get("grossProfitMarginTTM"),
            "operating_margin": rt.get("operatingProfitMarginTTM"),
            "net_margin": rt.get("netProfitMarginTTM"),
            "earnings_yield": kt.get("earningsYieldTTM"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        if result["market_cap"] and result["revenue_ttm"]:
            result["mcap_to_rev"] = round(result["market_cap"] / result["revenue_ttm"], 3)
        else:
            result["mcap_to_rev"] = None

    with _CACHE_LOCK:
        _FUNDAMENTAL_CACHE[ticker] = result
    return result'''


def main():
    section("1) Patch tier-classifier source — FMP /stable migration")
    src = open(SOURCE_FILE, "r", encoding="utf-8").read()

    if PATCH_BASE_OLD not in src:
        log("⚠️ FMP_BASE old constant not found — skipping (already patched?)")
    else:
        src = src.replace(PATCH_BASE_OLD, PATCH_BASE_NEW)
        log("✓ FMP_BASE: /api/v3 → /stable")

    if PATCH_FETCH_OLD not in src:
        log("⚠️ fetch_fundamentals OLD body not found — skipping")
        # try a more lenient probe
        if "def fetch_fundamentals(ticker):" not in src:
            log("✗ no fetch_fundamentals function found at all")
            return
        log("Function exists but its body has changed — exiting safely")
        return
    src = src.replace(PATCH_FETCH_OLD, PATCH_FETCH_NEW)
    log("✓ fetch_fundamentals: migrated to /stable endpoints + new field names")

    open(SOURCE_FILE, "w", encoding="utf-8").write(src)
    log(f"✓ Patched source written: {SOURCE_FILE}")

    section("2) Build deployment zip")
    src_text = open(SOURCE_FILE, "r", encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src_text)
    zip_bytes = buf.getvalue()
    log(f"zip size: {len(zip_bytes):,}b")

    section("3) Redeploy")
    lam = boto3.client("lambda", region_name=REGION)
    lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
    for _ in range(20):
        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        if cfg.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"✓ deployed, mod={cfg.get('LastModified')}")

    section("4) Smoke invoke")
    started = time.time()
    resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log(f"status={resp['StatusCode']} duration={round(time.time()-started, 1)}s")
    payload = json.loads(resp["Payload"].read())
    body = json.loads(payload.get("body", "{}")) if isinstance(payload, dict) else {}
    log("── Response body ──")
    for k, v in body.items():
        log(f"  {k}: {v}")

    if "LogResult" in resp:
        import base64
        log_text = base64.b64decode(resp["LogResult"]).decode("utf-8", errors="replace")
        log("── Log tail (last 25) ──")
        for line in log_text.splitlines()[-25:]:
            log(f"  {line}")

    section("5) Verify S3 output")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/theme-tiers.json")
        log(f"S3 size: {head['ContentLength']:,}b")
        log(f"S3 last_modified: {head['LastModified']}")
        obj = s3.get_object(Bucket=BUCKET, Key="data/theme-tiers.json")
        data = json.loads(obj["Body"].read())
        log(f"v: {data.get('schema_version')}")
        log(f"n_themes_classified: {data.get('n_themes_classified')}")
        s = data.get("summary", {})
        log(f"n_total_classifications: {s.get('n_total_classifications')}")
        log(f"n_deep_asymmetry: {s.get('n_deep_asymmetry')}")
        log(f"n_asymmetric: {s.get('n_asymmetric')}")
        log("")
        log("── Top 12 asymmetric leaderboard ──")
        for x in (s.get("top_asymmetric_leaderboard") or [])[:12]:
            mc = x.get("mcap_to_rev")
            mc_str = f"{mc:.2f}" if mc is not None else "n/a"
            ps = x.get("p_s")
            ps_str = f"{ps:.2f}" if ps is not None else "n/a"
            log(f"  {x['ticker']:<6} ({x['theme_etf']:<5} {x['theme_phase']:<13}) "
                f"tier={x['tier']} score={x['asymmetry_score']:>5.1f} "
                f"flag={x['flag']:<16} mcap_to_rev={mc_str:<6} p_s={ps_str}")
        log("")
        log("── MU-grade leaderboard (mcap_to_rev <= 3) ──")
        for x in (s.get("mu_grade_leaderboard") or [])[:12]:
            mc = x.get("mcap_to_rev")
            mc_str = f"{mc:.2f}" if mc is not None else "n/a"
            log(f"  {x['ticker']:<6} ({x['theme_etf']:<5} {x['theme_phase']:<13}) "
                f"score={x['asymmetry_score']:>5.1f} mcap_to_rev={mc_str:<6}")
        log("")
        log("── Tier-2 leaderboard ──")
        for x in (s.get("tier2_leaderboard") or [])[:10]:
            mc = x.get("mcap_to_rev")
            mc_str = f"{mc:.2f}" if mc is not None else "n/a"
            log(f"  {x['ticker']:<6} ({x['theme_etf']:<5}) score={x['asymmetry_score']:>5.1f} mcap_to_rev={mc_str}")

        # Sample one full theme — show MU's profile
        themes = data.get("themes", {})
        for theme_etf in ["SMH", "SOXX", "REMX", "LIT"]:
            if theme_etf in themes:
                t = themes[theme_etf]
                log("")
                log(f"── Sample theme: {theme_etf} ({t.get('name')}, phase={t.get('phase')}) ──")
                ts = t.get("theme_stats") or {}
                log(f"  theme medians: P/S={ts.get('p_s_mu')} P/E={ts.get('p_e_mu')} mcap/rev={ts.get('mcap_to_rev_median')}")
                log(f"  n with stats: P/S={ts.get('n_with_p_s')} P/E={ts.get('n_with_p_e')} mcr={ts.get('n_with_mcr')}")
                for tk_name, tk_data in (t.get("tickers") or {}).items():
                    f = tk_data.get("fundamentals", {})
                    log(f"    {tk_name:<6} tier={tk_data.get('tier')} "
                        f"score={tk_data.get('asymmetry_score', 0):>5.1f} "
                        f"P/S={(f.get('p_s') or 0):.2f} mcap_to_rev={(f.get('mcap_to_rev') or 0):.2f}")
                break
    except Exception as e:
        log(f"⚠️ S3 verify failed: {e}")


if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "patch_tier_classifier_fmp_stable.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("\n[report written]")
