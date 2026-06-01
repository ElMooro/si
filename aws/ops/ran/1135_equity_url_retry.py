"""1135 — retry function URL creation with cors.AllowMethods=['*']."""
import json
import pathlib
import time
import traceback
from datetime import datetime, timezone

import boto3
import urllib.request

REPORT = "aws/ops/reports/1135_equity_url_retry.json"
FN = "justhodl-equity-research"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({
            "name": name, "status": "ERROR",
            "error": str(e)[:400],
            "traceback": traceback.format_exc()[:1500],
        })
        return None


def create_url():
    cors = {
        "AllowCredentials": False,
        "AllowHeaders":     ["*"],
        "AllowMethods":     ["*"],
        "AllowOrigins":     ["*"],
        "ExposeHeaders":    ["*"],
        "MaxAge":           86400,
    }
    r = lam.create_function_url_config(
        FunctionName=FN, AuthType="NONE", Cors=cors,
    )
    return {"url": r["FunctionUrl"], "auth_type": r.get("AuthType"),
              "creation_time": str(r.get("CreationTime"))}


def patch_html(url):
    p = pathlib.Path("why.html")
    src = p.read_text()
    placeholder = "https://CONFIGURE_AFTER_DEPLOY.lambda-url.us-east-1.on.aws/"
    if placeholder not in src:
        return {"patched": False, "reason": "no placeholder found"}
    n = src.count(placeholder)
    p.write_text(src.replace(placeholder, url))
    return {"patched": True, "occurrences": n, "new_url": url}


def smoke(url, ticker="AAPL", timeout=180):
    full = f"{url}?ticker={ticker}"
    req = urllib.request.Request(full, headers={"User-Agent": "ops-1135/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    md = d.get("metadata") or {}
    v  = d.get("verdict") or {}
    co = d.get("company") or {}
    st = d.get("statements") or {}
    q  = d.get("quote") or {}
    gr = d.get("growth") or {}
    val = d.get("valuation") or {}
    ret = d.get("returns") or {}
    return {
        "status_code":        r.status,
        "elapsed_s":          elapsed,
        "size_kb":            round(len(body)/1024, 1),
        "ticker":             d.get("ticker"),
        "company":            co.get("name"),
        "sector":             co.get("sector"),
        "industry":           co.get("industry"),
        "mkt_cap_b":          round((co.get("market_cap") or 0)/1e9, 1),
        "price":              q.get("price"),
        "from_cache":         d.get("from_cache"),

        "rating":             v.get("rating"),
        "conviction":         v.get("conviction_grade"),
        "price_target":       v.get("price_target_12m"),
        "upside_pct":         v.get("upside_pct"),
        "confidence":         v.get("confidence_pct"),
        "position_size":      v.get("position_size_pct"),
        "horizon_months":     v.get("time_horizon_months"),
        "rationale":          (v.get("verdict_rationale") or "")[:220],

        "exec_summary":       (d.get("executive_summary") or "")[:220],
        "thesis_chars":       len(((d.get("investment_thesis") or {}).get("thesis_paragraph")) or ""),
        "risk_chars":         len(((d.get("risk_factors") or {}).get("risk_paragraph")) or ""),
        "valuation_chars":    len(d.get("valuation_assessment") or ""),
        "health_chars":       len(d.get("financial_health_summary") or ""),

        "n_catalysts":        len(d.get("catalysts_12m") or []),
        "n_triggers":         len(d.get("invalidation_triggers") or []),
        "income_years":       len(st.get("income_annual") or []),
        "balance_years":      len(st.get("balance_annual") or []),
        "cf_years":           len(st.get("cashflow_annual") or []),

        "pe_ttm":             val.get("pe_ttm"),
        "pe_5y_avg":          val.get("pe_5yr_avg"),
        "dcf_estimate":       val.get("dcf_estimate"),
        "dcf_upside_pct":     val.get("dcf_upside_pct"),
        "fcf_yield_pct":      val.get("fcf_yield_pct"),
        "roe_ttm":            val.get("roe_ttm_pct"),
        "roic_ttm":           val.get("roic_ttm_pct"),

        "rev_5y_cagr":        gr.get("revenue_5yr_cagr"),
        "rev_10y_cagr":       gr.get("revenue_10yr_cagr"),
        "eps_5y_cagr":        gr.get("eps_5yr_cagr"),
        "fcf_5y_cagr":        gr.get("fcf_5yr_cagr"),
        "consecutive_qs":     gr.get("consecutive_yoy_growth"),

        "ytd_pct":            ret.get("ytd_pct"),
        "1yr_pct":            ret.get("1yr_pct"),
        "5y_cagr_pct":        ret.get("5yr_cagr_pct"),
        "max_dd_pct":         ret.get("max_drawdown_pct"),

        "health_overall":     (d.get("financial_health") or {}).get("overall_score"),

        "claude_elapsed":     md.get("claude_elapsed_sec"),
        "fmp_loaded":         f"{md.get('data_sources_loaded')}/{md.get('data_sources_total')}",
        "fmp_failed":         md.get("fmp_endpoints_failed"),
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    url_info = phase(out, "create_url", create_url)

    if url_info and url_info.get("url"):
        url = url_info["url"]
        try:
            pathlib.Path(f"aws/lambdas/{FN}/.function-url").write_text(url + "\n")
        except Exception:
            pass

        phase(out, "patch_html", lambda: patch_html(url))
        time.sleep(3)
        phase(out, "smoke_AAPL", lambda: smoke(url, "AAPL"))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1135] DONE")


if __name__ == "__main__":
    main()
