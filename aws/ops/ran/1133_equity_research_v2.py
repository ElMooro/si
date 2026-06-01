"""1133 — equity research final verification + URL patch."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
import urllib.request

REPORT = "aws/ops/reports/1133_equity_research_v2.json"
lam = boto3.client("lambda", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:1200]})
        return None


def get_url():
    r = lam.get_function_url_config(FunctionName="justhodl-equity-research")
    return {"url": r["FunctionUrl"], "auth_type": r.get("AuthType")}


def patch_html(url):
    p = pathlib.Path("why.html")
    src = p.read_text()
    placeholder = "https://CONFIGURE_AFTER_DEPLOY.lambda-url.us-east-1.on.aws/"
    if placeholder in src:
        new = src.replace(placeholder, url)
        p.write_text(new)
        return {"patched": True, "new_url": url}
    return {"patched": False, "reason": "no placeholder (already patched)",
              "current_search": "CONFIGURE_AFTER_DEPLOY" in src}


def smoke(url, ticker="AAPL"):
    full = f"{url}?ticker={ticker}"
    req = urllib.request.Request(full, headers={"User-Agent": "ops-1133/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    return {
        "elapsed_s":          elapsed,
        "size_kb":            round(len(body)/1024, 1),
        "status_code":        r.status,
        "ticker":             d.get("ticker"),
        "company":            (d.get("company") or {}).get("name"),
        "sector":             (d.get("company") or {}).get("sector"),
        "market_cap":         (d.get("company") or {}).get("market_cap"),
        "price":              (d.get("quote") or {}).get("price"),
        "rating":             (d.get("verdict") or {}).get("rating"),
        "conviction":         (d.get("verdict") or {}).get("conviction_grade"),
        "price_target":       (d.get("verdict") or {}).get("price_target_12m"),
        "upside_pct":         (d.get("verdict") or {}).get("upside_pct"),
        "confidence_pct":     (d.get("verdict") or {}).get("confidence_pct"),
        "from_cache":         d.get("from_cache"),
        "exec_summary_chars": len(d.get("executive_summary") or ""),
        "thesis_chars":       len(((d.get("investment_thesis") or {}).get("thesis_paragraph")) or ""),
        "risk_chars":         len(((d.get("risk_factors") or {}).get("risk_paragraph")) or ""),
        "valuation_chars":    len(d.get("valuation_assessment") or ""),
        "n_catalysts":        len(d.get("catalysts_12m") or []),
        "n_triggers":         len(d.get("invalidation_triggers") or []),
        "income_years":       len((d.get("statements") or {}).get("income_annual") or []),
        "balance_years":      len((d.get("statements") or {}).get("balance_annual") or []),
        "cashflow_years":     len((d.get("statements") or {}).get("cashflow_annual") or []),
        "pe_ttm":             (d.get("valuation") or {}).get("pe_ttm"),
        "dcf_estimate":       (d.get("valuation") or {}).get("dcf_estimate"),
        "dcf_upside_pct":     (d.get("valuation") or {}).get("dcf_upside_pct"),
        "rev_5yr_cagr":       (d.get("growth") or {}).get("revenue_5yr_cagr"),
        "rev_10yr_cagr":      (d.get("growth") or {}).get("revenue_10yr_cagr"),
        "max_drawdown":       (d.get("returns") or {}).get("max_drawdown_pct"),
        "health_score":       (d.get("financial_health") or {}).get("overall_score"),
        "claude_elapsed":     (d.get("metadata") or {}).get("claude_elapsed_sec"),
        "fmp_ok":             (d.get("metadata") or {}).get("data_sources_loaded"),
        "fmp_total":          (d.get("metadata") or {}).get("data_sources_total"),
        "fmp_failed":         (d.get("metadata") or {}).get("fmp_endpoints_failed"),
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    url_info = phase(out, "function_url", get_url)
    if url_info and url_info.get("url"):
        url = url_info["url"]
        phase(out, "patch_html", lambda: patch_html(url))
        phase(out, "smoke_AAPL", lambda: smoke(url, "AAPL"))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1133] DONE")


if __name__ == "__main__":
    main()
